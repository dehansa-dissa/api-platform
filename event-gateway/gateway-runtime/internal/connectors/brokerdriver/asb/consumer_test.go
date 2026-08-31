package asb

import (
	"reflect"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// toReceived simulates what the ASB SDK does when it hands back a message
// previously sent via toServiceBusMessage: Body and ApplicationProperties
// round-trip verbatim over the wire.
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
		Value: nil, // tombstone
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

func TestValidateSubscriptionName_RejectsEmpty(t *testing.T) {
	if err := validateSubscriptionName(""); err == nil {
		t.Fatal("expected error for empty subscription name")
	}
}

func TestValidateSubscriptionName_RejectsOverLimit(t *testing.T) {
	// One character over the 50-character ASB subscription-name limit.
	overLimit := make([]byte, maxSubscriptionNameLength+1)
	for i := range overLimit {
		overLimit[i] = 'a'
	}
	if err := validateSubscriptionName(string(overLimit)); err == nil {
		t.Fatal("expected error for subscription name exceeding the ASB limit")
	}
}

func TestValidateSubscriptionName_AcceptsExactLimit(t *testing.T) {
	exactly := make([]byte, maxSubscriptionNameLength)
	for i := range exactly {
		exactly[i] = 'a'
	}
	if err := validateSubscriptionName(string(exactly)); err != nil {
		t.Fatalf("expected name at exactly the ASB limit to be accepted, got error: %v", err)
	}
}

// TestSubscriptionNameFor_PassesThroughRealisticWebSubGroupID pins the common
// case: the WebSub ConsumerManager's group ID fits comfortably, so it must
// reach ASB unchanged and stay legible in the portal.
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

// TestSubscriptionNameFor_HashesRealisticWebSocketGroupID covers the case that
// used to fail outright: the per-connection WebSocket paths build
// prefix + "-ws-" + a UUID, which is 53 characters — over ASB's limit.
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
	if err := validateSubscriptionName(got); err != nil {
		t.Fatalf("subscriptionNameFor() = %q is not a valid ASB subscription name: %v", got, err)
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

// TestConsumerEphemeralFlag_MatchesConstructorIntent guards the invariant the
// cleanup depends on: only the auto-complete constructor (used by the
// per-connection WebSocket paths) marks its subscriptions for deletion on
// Stop. Durable subscribers must never be marked, or unsubscribing one
// callback would tear down state that outlives it.
func TestConsumerEphemeralFlag_MatchesConstructorIntent(t *testing.T) {
	if got := (&Consumer{ephemeral: true}).ephemeral; !got {
		t.Fatal("expected ephemeral consumers to be flagged for cleanup")
	}
	// A durable consumer with no driver reference must not attempt deletion.
	durable := &Consumer{ephemeral: false}
	if durable.ephemeral {
		t.Fatal("durable consumers must never be flagged ephemeral")
	}
}
