package asb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/Azure/go-amqp"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

func toReceived(sb *azservicebus.Message) *azservicebus.ReceivedMessage {
	return &azservicebus.ReceivedMessage{
		Body:                  sb.Body,
		ApplicationProperties: sb.ApplicationProperties,
	}
}

func TestMessageRoundTrip_ValueKeyAndHeaders(t *testing.T) {
	original := &connectors.Message{
		Key:     []byte("topic-1:https://example.com/callback"),
		Value:   []byte(`{"active":true}`),
		Headers: map[string][]string{"X-Trace-Id": {"abc123"}, "X-Multi": {"a", "b"}},
	}

	sb, err := toServiceBusMessage(original)
	if err != nil {
		t.Fatalf("toServiceBusMessage returned error: %v", err)
	}

	got := receivedMessageToMessage(toReceived(sb), "topic-1")

	if string(got.Value) != string(original.Value) {
		t.Fatalf("Value round-trip mismatch: got %q, want %q", got.Value, original.Value)
	}
	if string(got.Key) != string(original.Key) {
		t.Fatalf("Key round-trip mismatch: got %q, want %q", got.Key, original.Key)
	}
	if !reflect.DeepEqual(got.Headers, original.Headers) {
		t.Fatalf("Headers round-trip mismatch: got %v, want %v", got.Headers, original.Headers)
	}
	if got.Topic != "topic-1" {
		t.Fatalf("expected topic to be set to %q, got %q", "topic-1", got.Topic)
	}
}

func TestMessageRoundTrip_TombstoneIsPreserved(t *testing.T) {
	original := &connectors.Message{
		Key:   []byte("topic-1:https://example.com/callback"),
		Value: nil,
	}

	sb, err := toServiceBusMessage(original)
	if err != nil {
		t.Fatalf("toServiceBusMessage returned error: %v", err)
	}

	got := receivedMessageToMessage(toReceived(sb), "topic-1")
	if got.Value != nil {
		t.Fatalf("expected tombstone to round-trip as a nil Value, got %q", got.Value)
	}
	if string(got.Key) != string(original.Key) {
		t.Fatalf("expected tombstone's key to be preserved, got %q, want %q", got.Key, original.Key)
	}
}

func TestMessageRoundTrip_TombstoneDistinctFromEmptyValue(t *testing.T) {
	empty := &connectors.Message{Key: []byte("k"), Value: []byte{}}
	tombstone := &connectors.Message{Key: []byte("k"), Value: nil}

	emptySB, err := toServiceBusMessage(empty)
	if err != nil {
		t.Fatalf("toServiceBusMessage(empty) returned error: %v", err)
	}
	tombstoneSB, err := toServiceBusMessage(tombstone)
	if err != nil {
		t.Fatalf("toServiceBusMessage(tombstone) returned error: %v", err)
	}

	gotEmpty := receivedMessageToMessage(toReceived(emptySB), "t")
	gotTombstone := receivedMessageToMessage(toReceived(tombstoneSB), "t")

	if gotEmpty.Value == nil {
		t.Fatal("expected a present-but-empty value to round-trip as non-nil")
	}
	if gotTombstone.Value != nil {
		t.Fatal("expected a tombstone to round-trip as nil, not empty")
	}
}

func TestMessageRoundTrip_NoKeyOrHeaders(t *testing.T) {
	original := &connectors.Message{Value: []byte("plain")}

	sb, err := toServiceBusMessage(original)
	if err != nil {
		t.Fatalf("toServiceBusMessage returned error: %v", err)
	}
	got := receivedMessageToMessage(toReceived(sb), "topic")

	if string(got.Value) != "plain" {
		t.Fatalf("expected value %q, got %q", "plain", got.Value)
	}
	if len(got.Key) != 0 {
		t.Fatalf("expected no key, got %q", got.Key)
	}
	if len(got.Headers) != 0 {
		t.Fatalf("expected no headers, got %v", got.Headers)
	}
}

func TestBackoffDelay_GrowsAndStaysWithinJitterBounds(t *testing.T) {
	for attempt := 0; attempt < 5; attempt++ {
		base := receiveRetryBase << attempt
		for i := 0; i < 50; i++ {
			got := backoffDelay(attempt)
			if got < base || got >= base+base/2 {
				t.Fatalf("backoffDelay(%d) = %s, want [%s, %s)", attempt, got, base, base+base/2)
			}
		}
	}
}

func TestBackoffDelay_CapsAtMaximum(t *testing.T) {
	for _, attempt := range []int{5, 10, 64} {
		got := backoffDelay(attempt)
		if got < receiveRetryMax || got >= receiveRetryMax+receiveRetryMax/2 {
			t.Fatalf("backoffDelay(%d) = %s, want it capped within [%s, %s)",
				attempt, got, receiveRetryMax, receiveRetryMax+receiveRetryMax/2)
		}
	}
}

func TestBackoffDelay_JittersSoReplicasDoNotRetryInLockstep(t *testing.T) {
	seen := make(map[time.Duration]struct{})
	for i := 0; i < 100; i++ {
		seen[backoffDelay(3)] = struct{}{}
	}
	if len(seen) == 1 {
		t.Fatal("backoffDelay returned a constant delay; replicas would retry in lockstep")
	}
}

func TestSleepBackoff_ReturnsFalseOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if sleepBackoff(ctx, 10) {
		t.Fatal("expected sleepBackoff to report a cancelled context rather than waiting out the delay")
	}
}

func TestSubscriptionNameFor_PassesThroughRealisticWebSubGroupID(t *testing.T) {
	groupID := "event-gateway-websub-a1b2c3d4e5f60718"
	got, err := subscriptionNameFor(groupID)
	if err != nil {
		t.Fatalf("subscriptionNameFor returned error: %v", err)
	}
	if got != groupID {
		t.Fatalf("expected a short group ID to pass through unchanged, got %q", got)
	}
}

func TestSubscriptionNameFor_HashesRealisticWebSocketGroupID(t *testing.T) {
	groupID := "event-gateway-ws-3f8a1c72-9b4e-4d21-a5f6-7e0c1d2b3a49"
	if len(groupID) <= maxSubscriptionNameLength {
		t.Fatalf("test premise broken: group ID is only %d chars, expected it to exceed %d",
			len(groupID), maxSubscriptionNameLength)
	}

	got, err := subscriptionNameFor(groupID)
	if err != nil {
		t.Fatalf("subscriptionNameFor returned error: %v", err)
	}
	if got == "" || len(got) > maxSubscriptionNameLength {
		t.Fatalf("subscriptionNameFor() = %q is %d characters, outside the 1-%d ASB limit",
			got, len(got), maxSubscriptionNameLength)
	}
}

func TestSubscriptionNameFor_DeterministicAndDistinct(t *testing.T) {
	long := "event-gateway-ws-3f8a1c72-9b4e-4d21-a5f6-7e0c1d2b3a49"
	other := "event-gateway-ws-00000000-9b4e-4d21-a5f6-7e0c1d2b3a49"

	first, _ := subscriptionNameFor(long)
	second, _ := subscriptionNameFor(long)
	if first != second {
		t.Fatalf("subscriptionNameFor must be deterministic; got %q then %q", first, second)
	}

	distinct, _ := subscriptionNameFor(other)
	if first == distinct {
		t.Fatalf("distinct group IDs must not collide, both mapped to %q", first)
	}
}

func TestSubscriptionNameFor_RejectsEmpty(t *testing.T) {
	if _, err := subscriptionNameFor(""); err == nil {
		t.Fatal("expected error for empty group ID")
	}
}

func TestIsNotFoundErr_MatchesAllThreeNotFoundForms(t *testing.T) {
	linkDetached := &amqp.Error{
		Condition:   amqp.ErrCondNotFound,
		Description: "The messaging entity 'ns:Topic:abc|cache-def' could not be found.",
	}

	cases := map[string]struct {
		err  error
		want bool
	}{
		"management 404":       {&azcore.ResponseError{StatusCode: http.StatusNotFound}, true},
		"rpc path notfound":    {&azservicebus.Error{Code: azservicebus.CodeNotFound}, true},
		"detached link":        {linkDetached, true},
		"wrapped detached":     {fmt.Errorf("receive: %w", linkDetached), true},
		"management 409":       {&azcore.ResponseError{StatusCode: http.StatusConflict}, false},
		"connection lost":      {&azservicebus.Error{Code: azservicebus.CodeConnectionLost}, false},
		"other amqp condition": {&amqp.Error{Condition: amqp.ErrCondUnauthorizedAccess}, false},
		"unrelated error":      {errors.New("boom"), false},
	}

	for name, tc := range cases {
		if got := isNotFoundErr(tc.err); got != tc.want {
			t.Fatalf("%s: isNotFoundErr = %v, want %v", name, got, tc.want)
		}
	}
}

func TestSubscriptionProps_NilWhenNothingConfigured(t *testing.T) {
	driver := &ASBBrokerDriver{}

	if got := driver.subscriptionProps(false); got != nil {
		t.Errorf("durable subscription asserted properties with none configured: %+v", got)
	}
	if got := driver.subscriptionProps(true); got != nil {
		t.Errorf("disposable subscription asserted properties with none configured: %+v", got)
	}
}

func TestSubscriptionProps_IdleTimeoutOnlyForDisposable(t *testing.T) {
	driver := &ASBBrokerDriver{cfg: ConnectionConfig{SubscriptionIdleTimeout: 10 * time.Minute}}

	if got := driver.subscriptionProps(false); got != nil {
		t.Errorf("a durable subscription must never carry AutoDeleteOnIdle: %+v", got)
	}
	got := driver.subscriptionProps(true)
	if got == nil || got.AutoDeleteOnIdle == nil || *got.AutoDeleteOnIdle != "PT600S" {
		t.Fatalf("disposable subscription did not carry the configured idle timeout: %+v", got)
	}
}

func TestSubscriptionProps_MaxDeliveryCountAppliesToBoth(t *testing.T) {
	driver := &ASBBrokerDriver{cfg: ConnectionConfig{MaxDeliveryCount: 20}}

	for _, disposable := range []bool{false, true} {
		got := driver.subscriptionProps(disposable)
		if got == nil || got.MaxDeliveryCount == nil || *got.MaxDeliveryCount != 20 {
			t.Errorf("subscriptionProps(disposable=%v) dropped the max delivery count: %+v", disposable, got)
		}
	}
}

func TestRenewLock_LockLostCancelsHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	c := &Consumer{topic: "t", groupID: "g"}
	lost := &azservicebus.Error{Code: azservicebus.CodeLockLost}

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.renewLock(ctx, cancel, time.Millisecond, func(context.Context) error { return lost })
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("renewLock did not return after the lock was lost")
	}
	if ctx.Err() == nil {
		t.Fatal("expected the handler context to be cancelled after a lost lock")
	}
}

func TestRenewLock_TransientFailureKeepsGoing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &Consumer{topic: "t", groupID: "g"}
	attempts := make(chan struct{}, 8)

	go c.renewLock(ctx, cancel, time.Millisecond, func(context.Context) error {
		select {
		case attempts <- struct{}{}:
		default:
		}
		return errors.New("namespace briefly unreachable")
	})

	for i := 0; i < 3; i++ {
		select {
		case <-attempts:
		case <-time.After(time.Second):
			t.Fatalf("renewal stopped after %d attempts; a transient error must not cancel", i)
		}
	}
	if ctx.Err() != nil {
		t.Fatal("transient renewal failures must not cancel the handler context")
	}
}

func TestRenewLock_StopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{topic: "t", groupID: "g"}

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.renewLock(ctx, cancel, time.Hour, func(context.Context) error { return nil })
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("renewLock outlived its context")
	}
}

func TestRunHandler_FastHandlerKeepsLock(t *testing.T) {
	c := &Consumer{topic: "t", groupID: "g", handler: func(context.Context, *connectors.Message) error { return nil }}

	err, lockHeld := c.runHandler(context.Background(), &connectors.Message{},
		time.Minute, time.Hour, func(context.Context) error { return nil })

	if err != nil {
		t.Fatalf("handler returned %v, want nil", err)
	}
	if !lockHeld {
		t.Fatal("a handler that returned immediately must still hold its lock, so the message can be settled")
	}
}
func TestRunHandler_BudgetExceededDropsLock(t *testing.T) {
	c := &Consumer{topic: "t", groupID: "g", handler: func(ctx context.Context, _ *connectors.Message) error {
		<-ctx.Done()
		return ctx.Err()
	}}

	_, lockHeld := c.runHandler(context.Background(), &connectors.Message{},
		20*time.Millisecond, time.Hour, func(context.Context) error { return nil })

	if lockHeld {
		t.Fatal("a handler that outran its budget must not report the lock as held")
	}
}

func TestRunHandler_LockLostCancelsHandlerAndDropsLock(t *testing.T) {
	observed := make(chan error, 1)
	c := &Consumer{topic: "t", groupID: "g", handler: func(ctx context.Context, _ *connectors.Message) error {
		<-ctx.Done()
		observed <- ctx.Err()
		return ctx.Err()
	}}

	_, lockHeld := c.runHandler(context.Background(), &connectors.Message{},
		time.Minute, time.Millisecond,
		func(context.Context) error { return &azservicebus.Error{Code: azservicebus.CodeLockLost} })

	if lockHeld {
		t.Fatal("lock was lost mid-handling; lockHeld must be false")
	}
	select {
	case err := <-observed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler saw %v, want context.Canceled", err)
		}
	default:
		t.Fatal("handler was never cancelled after the lock was lost")
	}
}

type fakeReceiver struct {
	mu sync.Mutex

	completed    int
	deadLettered int
	renewals     int
	closed       int

	completeErr   error
	deadLetterErr error
	renewErr      error

	deadLetterReason string
}

func (f *fakeReceiver) ReceiveMessages(context.Context, int, *azservicebus.ReceiveMessagesOptions) (
	[]*azservicebus.ReceivedMessage, error) {
	return nil, errors.New("not used by these tests")
}

func (f *fakeReceiver) CompleteMessage(context.Context, *azservicebus.ReceivedMessage,
	*azservicebus.CompleteMessageOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.completeErr != nil {
		return f.completeErr
	}
	f.completed++
	return nil
}

func (f *fakeReceiver) DeadLetterMessage(_ context.Context, _ *azservicebus.ReceivedMessage,
	options *azservicebus.DeadLetterOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deadLetterErr != nil {
		return f.deadLetterErr
	}
	f.deadLettered++
	if options != nil && options.Reason != nil {
		f.deadLetterReason = *options.Reason
	}
	return nil
}

func (f *fakeReceiver) RenewMessageLock(context.Context, *azservicebus.ReceivedMessage,
	*azservicebus.RenewMessageLockOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewals++
	return f.renewErr
}

func (f *fakeReceiver) Close(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func (f *fakeReceiver) counts() (completed, deadLettered int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completed, f.deadLettered
}

func receivedMessage(deliveryCount uint32) *azservicebus.ReceivedMessage {
	sequence := int64(1)
	return &azservicebus.ReceivedMessage{
		Body:           []byte(`{"id":1}`),
		DeliveryCount:  deliveryCount,
		SequenceNumber: &sequence,
	}
}

func TestHandleMessage_CompletesAHealthyMessage(t *testing.T) {
	receiver := &fakeReceiver{}
	c := &Consumer{
		topic:    "cache-settle01",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleAutoComplete,
		handler:  func(context.Context, *connectors.Message) error { return nil },
	}

	c.handleMessage(context.Background(), receivedMessage(1))

	completed, deadLettered := receiver.counts()
	if completed != 1 {
		t.Fatalf("message was completed %d times, want 1 — an unsettled message is redelivered", completed)
	}
	if deadLettered != 0 {
		t.Errorf("a healthy message was dead-lettered %d times, want 0", deadLettered)
	}
}

func TestHandleMessage_DeadLettersAFailedHandler(t *testing.T) {
	receiver := &fakeReceiver{}
	c := &Consumer{
		topic:    "cache-settle02",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleOnHandlerResult,
		handler:  func(context.Context, *connectors.Message) error { return errors.New("subscriber refused") },
	}

	c.handleMessage(context.Background(), receivedMessage(1))

	completed, deadLettered := receiver.counts()
	if deadLettered != 1 {
		t.Fatalf("message was dead-lettered %d times, want 1", deadLettered)
	}
	if completed != 0 {
		t.Errorf("a failed message was also completed %d times, want 0", completed)
	}
	if receiver.deadLetterReason == "" {
		t.Error("dead-letter carried no reason; an operator cannot tell why it is in the queue")
	}
}

func TestHandleMessage_AutoCompleteSettlesDespiteAHandlerError(t *testing.T) {
	receiver := &fakeReceiver{}
	c := &Consumer{
		topic:    "cache-settle03",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleAutoComplete,
		handler:  func(context.Context, *connectors.Message) error { return errors.New("delivery failed") },
	}

	c.handleMessage(context.Background(), receivedMessage(1))

	completed, deadLettered := receiver.counts()
	if completed != 1 {
		t.Errorf("auto-complete mode completed %d messages, want 1", completed)
	}
	if deadLettered != 0 {
		t.Errorf("auto-complete mode dead-lettered %d messages, want 0", deadLettered)
	}
}

// A redelivery is an ordinary message: Azure raises DeliveryCount, and nothing
// about settlement changes. Only the WARN it emits is different.
func TestHandleMessage_RedeliveryIsSettledLikeAnyOtherMessage(t *testing.T) {
	receiver := &fakeReceiver{}
	c := &Consumer{
		topic:    "cache-settle04",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleAutoComplete,
		handler:  func(context.Context, *connectors.Message) error { return nil },
	}

	c.handleMessage(context.Background(), receivedMessage(1))
	c.handleMessage(context.Background(), receivedMessage(3))

	completed, deadLettered := receiver.counts()
	if completed != 2 {
		t.Errorf("completed %d messages, want 2 — a redelivery must settle like any other", completed)
	}
	if deadLettered != 0 {
		t.Errorf("a redelivery was dead-lettered %d times, want 0", deadLettered)
	}
}

// A settle that fails must not fall through to a second settle attempt. Azure
// redelivers the message on its own once the lock lapses.
func TestHandleMessage_FailedCompleteIsNotFollowedByADeadLetter(t *testing.T) {
	receiver := &fakeReceiver{completeErr: errors.New("link detached")}
	c := &Consumer{
		topic:    "cache-settle05",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleAutoComplete,
		handler:  func(context.Context, *connectors.Message) error { return nil },
	}

	c.handleMessage(context.Background(), receivedMessage(1))

	completed, deadLettered := receiver.counts()
	if completed != 0 {
		t.Errorf("a failed complete was recorded as completed %d times, want 0", completed)
	}
	if deadLettered != 0 {
		t.Errorf("a failed complete was escalated to a dead-letter %d times, want 0", deadLettered)
	}
}

func TestHandleMessage_LapsedLockLeavesTheMessageUnsettled(t *testing.T) {
	receiver := &fakeReceiver{}
	c := &Consumer{
		topic:    "cache-settle06",
		groupID:  "grp-settle",
		receiver: receiver,
		mode:     settleAutoComplete,
		handler: func(ctx context.Context, _ *connectors.Message) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	c.handleMessageWithBudget(context.Background(), receivedMessage(1), 20*time.Millisecond, time.Hour)

	completed, deadLettered := receiver.counts()
	if completed != 0 || deadLettered != 0 {
		t.Fatalf("a message whose lock lapsed was settled anyway (completed=%d dead-lettered=%d)",
			completed, deadLettered)
	}
}

func TestReconcileSubscription_PreservesTheOtherProperties(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t, "subscription_get_200", "subscription_update_200"))

	desired := &admin.SubscriptionProperties{AutoDeleteOnIdle: to.Ptr(iso8601Seconds(30 * time.Minute))}
	if err := driver.reconcileSubscription(context.Background(), "a-topic", "a-sub", desired); err != nil {
		t.Fatalf("reconcileIdleTimeout returned an error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected a read then a write, got %d calls", len(*calls))
	}
	sent := (*calls)[1].Body
	if !strings.Contains(sent, "<AutoDeleteOnIdle>PT1800S</AutoDeleteOnIdle>") {
		t.Fatalf("the update did not carry the new idle timeout; body was:\n%s", sent)
	}
	for _, preserved := range []string{"<LockDuration>PT1M</LockDuration>", "<MaxDeliveryCount>10</MaxDeliveryCount>"} {
		if !strings.Contains(sent, preserved) {
			t.Fatalf("the update dropped %s, resetting it to a default; body was:\n%s", preserved, sent)
		}
	}
}

func TestReconcileSubscription_MatchingTimeoutSkipsTheWrite(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t, "subscription_get_200"))

	desired := &admin.SubscriptionProperties{AutoDeleteOnIdle: to.Ptr(iso8601Seconds(10 * time.Minute))}
	if err := driver.reconcileSubscription(context.Background(), "a-topic", "a-sub", desired); err != nil {
		t.Fatalf("reconcileIdleTimeout returned an error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected the read alone, got %d calls — an unchanged timeout was rewritten", len(*calls))
	}
}

func TestReconcileSubscription_MissingSubscriptionIsNotAFailure(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "subscription_get_missing"))

	desired := &admin.SubscriptionProperties{AutoDeleteOnIdle: to.Ptr(iso8601Seconds(30 * time.Minute))}
	if err := driver.reconcileSubscription(context.Background(), "a-topic", "gone-sub", desired); err != nil {
		t.Fatalf("a reclaimed subscription is not a failure to reconcile: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected the read alone, got %d calls — it tried to update a subscription that is gone", len(*calls))
	}
}

func TestReconcileSubscription_UpdatesAChangedMaxDeliveryCount(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t, "subscription_get_200", "subscription_update_200"))

	desired := &admin.SubscriptionProperties{MaxDeliveryCount: to.Ptr(int32(20))}
	if err := driver.reconcileSubscription(context.Background(), "a-topic", "a-sub", desired); err != nil {
		t.Fatalf("reconcileSubscription returned an error: %v", err)
	}

	if len(*calls) != 2 {
		t.Fatalf("expected a read then a write, got %d calls", len(*calls))
	}
	sent := (*calls)[1].Body
	if !strings.Contains(sent, "<MaxDeliveryCount>20</MaxDeliveryCount>") {
		t.Fatalf("the update did not carry the new max delivery count; body was:\n%s", sent)
	}
	if !strings.Contains(sent, "<AutoDeleteOnIdle>PT10M</AutoDeleteOnIdle>") {
		t.Fatalf("reconciling one property reset another; body was:\n%s", sent)
	}
}

func TestReconcileSubscription_MatchingPropertiesSkipTheWrite(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t, "subscription_get_200"))

	desired := &admin.SubscriptionProperties{
		AutoDeleteOnIdle: to.Ptr(iso8601Seconds(10 * time.Minute)),
		MaxDeliveryCount: to.Ptr(int32(10)),
	}
	if err := driver.reconcileSubscription(context.Background(), "a-topic", "a-sub", desired); err != nil {
		t.Fatalf("reconcileSubscription returned an error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected the read alone, got %d calls — unchanged properties were rewritten", len(*calls))
	}
}

func TestCreateSubscription_ConflictReconcilesTheIdleTimeout(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t,
		"subscription_create_409", "subscription_get_200", "subscription_update_200"))

	props := &admin.SubscriptionProperties{AutoDeleteOnIdle: to.Ptr(iso8601Seconds(30 * time.Minute))}
	if err := driver.createSubscription(context.Background(), "a-topic", "a-sub", props); err != nil {
		t.Fatalf("a conflicting create must still succeed: %v", err)
	}

	if len(*calls) != 3 {
		t.Fatalf("expected create, read, update; got %d calls", len(*calls))
	}
	if !strings.Contains((*calls)[2].Body, "<AutoDeleteOnIdle>PT1800S</AutoDeleteOnIdle>") {
		t.Fatal("the conflict did not carry the configured idle timeout through to an update")
	}
}

func TestCreateSubscription_DurableSubscriptionSkipsTheReadBack(t *testing.T) {
	driver, calls := mockAdmin(t, respondInOrder(t, "subscription_create_409"))

	if err := driver.createSubscription(context.Background(), "a-topic", "a-sub", nil); err != nil {
		t.Fatalf("a conflicting durable create must succeed: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected the create alone, got %d calls", len(*calls))
	}
}

func TestConsumerStop_EphemeralDeletesItsSubscription(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "topic_delete_200"))

	receiver, err := driver.client.NewReceiverForSubscription("ws-topic", "grp-abc", nil)
	if err != nil {
		t.Fatalf("failed to build a receiver: %v", err)
	}
	consumer := &Consumer{
		receiver:  receiver,
		topic:     "ws-topic",
		ephemeral: true,
		driver:    driver,
		groupID:   "grp-abc",
	}

	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned an error: %v", err)
	}

	if len(*calls) != 1 {
		t.Fatalf("expected one delete, got %d calls", len(*calls))
	}
	if got := (*calls)[0]; got.Method != http.MethodDelete ||
		!strings.HasSuffix(got.Path, "/ws-topic/Subscriptions/grp-abc") {
		t.Fatalf("expected a DELETE of the connection's subscription, got %s %s", got.Method, got.Path)
	}
}

func TestConsumerStop_DurableKeepsItsSubscription(t *testing.T) {
	driver, calls := mockAdmin(t, alwaysRespond(t, "topic_delete_200"))

	receiver, err := driver.client.NewReceiverForSubscription("websub-topic", "grp-xyz", nil)
	if err != nil {
		t.Fatalf("failed to build a receiver: %v", err)
	}
	consumer := &Consumer{
		receiver: receiver,
		topic:    "websub-topic",
		driver:   driver,
		groupID:  "grp-xyz",
	}

	if err := consumer.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned an error: %v", err)
	}

	if len(*calls) != 0 {
		t.Fatalf("a durable consumer deleted its subscription on stop: %v", *calls)
	}
}

// recordingHandler collects what an observer delivered, in order.
type recordingHandler struct {
	mu     sync.Mutex
	keys   []string
	values map[string][]string // key -> values in delivery order
	seen   chan struct{}
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{
		values: make(map[string][]string),
		seen:   make(chan struct{}, observerQueueDepth*4),
	}
}

func (r *recordingHandler) handle(context.Context, *connectors.Message) error { return nil }

func (r *recordingHandler) record(_ context.Context, msg *connectors.Message) error {
	r.mu.Lock()
	key := string(msg.Key)
	r.keys = append(r.keys, key)
	r.values[key] = append(r.values[key], string(msg.Value))
	r.mu.Unlock()
	select {
	case r.seen <- struct{}{}:
	default:
	}
	return nil
}

func (r *recordingHandler) sorted() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := append([]string(nil), r.keys...)
	sort.Strings(out)
	return out
}

func (r *recordingHandler) waitFor(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		r.mu.Lock()
		got := len(r.keys)
		r.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-r.seen:
		case <-deadline:
			t.Fatalf("observer delivered %d messages, want at least %d", got, n)
		}
	}
}

func observerState(cache map[string][]byte) *compactedTopicState {
	if cache == nil {
		cache = map[string][]byte{}
	}
	// cancel is set because a driver Close stops every registered state, the same
	// as one built by EnsureCompactedTopic.
	_, cancel := context.WithCancel(context.Background())
	return &compactedTopicState{
		topic:  "cache-obs",
		cache:  cache,
		dirty:  make(chan struct{}, 1),
		cancel: cancel,
	}
}

func TestCacheObserver_IsPrimedWithTheCacheOnStart(t *testing.T) {
	state := observerState(map[string][]byte{
		"orders:https://a.example/hook": []byte(`{"state":"active"}`),
		"orders:https://b.example/hook": []byte(`{"state":"active"}`),
	})
	rec := newRecordingHandler()
	o := newCacheObserver(state, state.topic, rec.record)

	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	t.Cleanup(func() { _ = o.Stop(context.Background()) })

	rec.waitFor(t, 2)
	got := rec.sorted()
	if len(got) != 2 || got[0] != "orders:https://a.example/hook" || got[1] != "orders:https://b.example/hook" {
		t.Fatalf("primed with %v; a restarting store would not see existing subscribers", got)
	}
}

func TestCacheObserver_ReceivesLiveMessagesFromTheTail(t *testing.T) {
	state := observerState(nil)
	rec := newRecordingHandler()
	o := newCacheObserver(state, state.topic, rec.record)

	if err := o.Start(context.Background()); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	t.Cleanup(func() { _ = o.Stop(context.Background()) })

	state.fanOutToObservers(&connectors.Message{Topic: state.topic, Key: []byte("k1"), Value: []byte(`{}`)})
	state.fanOutToObservers(&connectors.Message{Topic: state.topic, Key: []byte("k2"), Value: []byte(`{}`)})

	rec.waitFor(t, 2)
	if got := rec.sorted(); len(got) != 2 || got[0] != "k1" || got[1] != "k2" {
		t.Fatalf("delivered %v, want k1 and k2", got)
	}
}

func TestCacheObserver_StoppedObserverStopsReceiving(t *testing.T) {
	state := observerState(nil)
	rec := newRecordingHandler()
	o := newCacheObserver(state, state.topic, rec.record)

	_ = o.Start(context.Background())
	state.fanOutToObservers(&connectors.Message{Topic: state.topic, Key: []byte("before"), Value: []byte(`{}`)})
	rec.waitFor(t, 1)

	if err := o.Stop(context.Background()); err != nil {
		t.Fatalf("Stop returned %v", err)
	}
	state.fanOutToObservers(&connectors.Message{Topic: state.topic, Key: []byte("after"), Value: []byte(`{}`)})

	for _, k := range rec.sorted() {
		if k == "after" {
			t.Fatal("a stopped observer still received a message")
		}
	}
}

// A handler slow enough to fill the queue must not block the tail: the snapshot's
// freshness cannot depend on how fast a delivery consumer starts.
func TestCacheObserver_FullQueueDoesNotBlockTheTail(t *testing.T) {
	state := observerState(nil)
	release := make(chan struct{})
	blocked := func(context.Context, *connectors.Message) error {
		<-release
		return nil
	}
	o := newCacheObserver(state, state.topic, blocked)
	_ = o.Start(context.Background())
	t.Cleanup(func() { close(release); _ = o.Stop(context.Background()) })

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < observerQueueDepth*3; i++ {
			state.fanOutToObservers(&connectors.Message{Topic: state.topic, Key: []byte("k"), Value: []byte(`{}`)})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fanOutToObservers blocked on a stalled observer; the tail would stall with it")
	}

	if len(o.resync) != 1 {
		t.Error("a dropped message did not schedule a re-prime, so the observer would stay incomplete")
	}
}

func TestCacheObserver_StopIsIdempotent(t *testing.T) {
	state := observerState(nil)
	o := newCacheObserver(state, state.topic, newRecordingHandler().handle)
	_ = o.Start(context.Background())

	if err := o.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop returned %v", err)
	}
	if err := o.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop returned %v", err)
	}
}

func TestResyncObservers_RePrimesFromTheReplacedCache(t *testing.T) {
	state := observerState(nil)
	rec := newRecordingHandler()
	o := newCacheObserver(state, state.topic, rec.record)
	_ = o.Start(context.Background())
	t.Cleanup(func() { _ = o.Stop(context.Background()) })

	// Stand in for rebuildCacheTail replacing the map wholesale.
	state.mu.Lock()
	state.cache = map[string][]byte{"rebuilt:key": []byte(`{}`)}
	state.mu.Unlock()
	state.resyncObservers()

	rec.waitFor(t, 1)
	for _, k := range rec.sorted() {
		if k == "rebuilt:key" {
			return
		}
	}
	t.Fatal("a wholesale cache replacement never reached the observer")
}

func TestWatch_CreatesNoSubscriptionAndAttachesToTheTail(t *testing.T) {
	driver, calls := mockAdmin(t, respondStatus(http.StatusInternalServerError))
	state := observerState(map[string][]byte{"orders:https://a.example/hook": []byte(`{}`)})
	driver.compacted["websub-api-sync"] = state

	rec := newRecordingHandler()
	receiver, err := driver.Watch(context.Background(), "runtime-1-api-a", "websub-api-sync", rec.record)
	if err != nil {
		t.Fatalf("Watch returned %v, want nil", err)
	}

	// The mock transport fails every request, so any admin call at all would have
	// surfaced as an error above. Zero calls is the proof no entity was created.
	if len(*calls) != 0 {
		t.Fatalf("Watch made %d admin calls, want 0 — it must not create a subscription: %v",
			len(*calls), describeCalls(*calls))
	}

	if err := receiver.Start(context.Background()); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	t.Cleanup(func() { _ = receiver.Stop(context.Background()) })

	rec.waitFor(t, 1)
	if got := rec.sorted(); len(got) != 1 || got[0] != "orders:https://a.example/hook" {
		t.Fatalf("observer primed with %v, want the cache's one key", got)
	}
}

func TestWatch_WithoutACompactedTailIsAnError(t *testing.T) {
	driver, calls := mockAdmin(t, respondStatus(http.StatusInternalServerError))

	if _, err := driver.Watch(context.Background(), "runtime-1-api-a", "never-ensured", newRecordingHandler().handle); err == nil {
		t.Fatal("Watch succeeded for a topic with no tail; live updates would have nothing making them durable")
	}
	if len(*calls) != 0 {
		t.Errorf("Watch made %d admin calls while failing, want 0", len(*calls))
	}
}

func TestIsSessionHeldErr_RecognisesContentionButNotOtherFailures(t *testing.T) {
	held := &amqp.Error{
		Condition:   amqp.ErrCond(sessionCannotBeLocked),
		Description: "The requested session 'leader' cannot be accepted. It may be locked by another receiver.",
	}

	cases := map[string]struct {
		err  error
		want bool
	}{
		"session held":     {held, true},
		"wrapped":          {fmt.Errorf("accept session: %w", held), true},
		"timeout":          {&azservicebus.Error{Code: azservicebus.CodeTimeout}, false},
		"entity not found": {&amqp.Error{Condition: amqp.ErrCondNotFound}, false},
		"unauthorized":     {&azservicebus.Error{Code: azservicebus.CodeUnauthorizedAccess}, false},
		"nil":              {nil, false},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := isSessionHeldErr(tc.err); got != tc.want {
				t.Fatalf("isSessionHeldErr = %v, want %v — a misread logs routine contention as a "+
					"warning, or hides a real failure", got, tc.want)
			}
		})
	}
}

// valuesFor returns the values delivered for key, in delivery order, which is
// what a stale-after-fresh regression shows up in.
func (r *recordingHandler) valuesFor(key string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.values[key]...)
}

func TestCacheObserver_RePrimeSupersedesQueuedMessages(t *testing.T) {
	const key = "loginSuccess:https://acme.example/hook"

	state := observerState(map[string][]byte{key: []byte("current")})
	rec := newRecordingHandler()
	o := newCacheObserver(state, "cache-obs", rec.record)

	// The tail outran the observer, so the queue holds values the cache has
	// already moved past, and fanOutToObservers marked it stale on a full queue.
	for i := 0; i < observerQueueDepth; i++ {
		o.queue <- &connectors.Message{Topic: "cache-obs", Key: []byte(key), Value: []byte("superseded")}
	}
	o.markStale()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); o.pump(ctx, nil) }()

	// The value the observer is left holding is the invariant. A queued message
	// may still be picked before the re-prime, since pump selects at random, and
	// that is a redundant delivery rather than a regression — what must not
	// happen is the queue outliving the re-prime and overwriting it.
	deadline := time.After(2 * time.Second)
	var got []string
	for {
		got = rec.valuesFor(key)
		if len(got) > 0 && got[len(got)-1] == "current" {
			break
		}
		select {
		case <-rec.seen:
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("observer never settled on the cache's value; delivered %v", got)
		}
	}

	cancel()
	<-done

	if n := len(got); n > observerQueueDepth {
		t.Errorf("re-prime did not supersede the queue: %d deliveries for one key", n)
	}
}

func TestMessageIDForKey_PassesShortKeysThroughUnchanged(t *testing.T) {
	key := []byte("issues:http://subscriber:8080/webhook")
	if got := messageIDForKey(key); got != string(key) {
		t.Errorf("messageIDForKey() = %q, want the key itself", got)
	}
}

func TestMessageIDForKey_BoundsAKeyServiceBusWouldReject(t *testing.T) {
	// The shape that breaks it: a URI-named channel plus a real callback URL.
	key := []byte("https://schemas.identity.wso2.org/events/login/event-type/loginSuccess" +
		":https://webhooks.acme.example.com/asgardeo/identity-events")
	if len(key) <= maxMessageIDLength {
		t.Fatalf("test key is %d bytes, which no longer exceeds the limit", len(key))
	}

	got := messageIDForKey(key)
	if len(got) > maxMessageIDLength {
		t.Errorf("messageIDForKey() returned %d bytes, over the %d limit", len(got), maxMessageIDLength)
	}
	// Hashed, not truncated: two callbacks under one channel share a long prefix.
	if strings.HasPrefix(string(key), got) {
		t.Errorf("messageIDForKey() truncated instead of hashing: %q", got)
	}
}

func TestMessageIDForKey_DistinctKeysDoNotCollide(t *testing.T) {
	channel := "https://schemas.identity.wso2.org/events/login/event-type/loginSuccess:"
	a := messageIDForKey([]byte(channel + "https://webhooks.acme.example.com/asgardeo/identity-events"))
	b := messageIDForKey([]byte(channel + "https://webhooks.globex.example.com/asgardeo/identity-events"))
	if a == b {
		t.Errorf("two callbacks under one channel produced the same message ID: %q", a)
	}
}
