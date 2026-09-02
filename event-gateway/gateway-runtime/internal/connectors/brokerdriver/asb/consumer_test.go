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

func TestIsNotFoundErr_MatchesManagementAndAMQPForms(t *testing.T) {
	cases := map[string]struct {
		err  error
		want bool
	}{
		"management 404":  {&azcore.ResponseError{StatusCode: http.StatusNotFound}, true},
		"amqp notfound":   {&azservicebus.Error{Code: azservicebus.CodeNotFound}, true},
		"wrapped amqp":    {fmt.Errorf("receive: %w", &azservicebus.Error{Code: azservicebus.CodeNotFound}), true},
		"management 409":  {&azcore.ResponseError{StatusCode: http.StatusConflict}, false},
		"amqp conn lost":  {&azservicebus.Error{Code: azservicebus.CodeConnectionLost}, false},
		"unrelated error": {errors.New("boom"), false},
	}

	for name, tc := range cases {
		if got := isNotFoundErr(tc.err); got != tc.want {
			t.Fatalf("%s: isNotFoundErr = %v, want %v", name, got, tc.want)
		}
	}
}
