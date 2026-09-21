package asb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
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
