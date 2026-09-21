package asb

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

type fakeSender struct {
	mu   sync.Mutex
	sent []*azservicebus.Message
	err  error
}

func (f *fakeSender) SendMessage(_ context.Context, m *azservicebus.Message, _ *azservicebus.SendMessageOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeSender) Close(context.Context) error { return nil }

func testPublisher(sender messageSender) *Publisher {
	return &Publisher{
		newSender: func(string) (messageSender, error) { return sender, nil },
		senders:   make(map[string]messageSender),
	}
}

func TestPublish_SendsTheEncodedMessage(t *testing.T) {
	sender := &fakeSender{}
	body := []byte(`{"id":1}`)

	if err := testPublisher(sender).Publish(context.Background(), "cache-pub01",
		&connectors.Message{Value: body}); err != nil {
		t.Fatalf("Publish returned %v, want nil", err)
	}

	if len(sender.sent) != 1 {
		t.Fatalf("sender received %d messages, want 1", len(sender.sent))
	}
	if !bytes.Equal(sender.sent[0].Body, body) {
		t.Errorf("sent body is %q, want %q", sender.sent[0].Body, body)
	}
}

func TestPublish_ReturnsAnErrorOnAFailedSend(t *testing.T) {
	sender := &fakeSender{err: errors.New("entity not found")}

	if err := testPublisher(sender).Publish(context.Background(), "cache-pub02",
		&connectors.Message{Value: []byte(`{"id":1}`)}); err == nil {
		t.Fatal("Publish returned nil on a failed send, want an error")
	}

	if len(sender.sent) != 0 {
		t.Errorf("a failed send recorded %d messages, want 0", len(sender.sent))
	}
}
