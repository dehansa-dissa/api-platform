package websub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// passThroughProcessor runs no policies, so a handler under test reaches the
// branch being exercised rather than the policy error path.
type passThroughProcessor struct{}

func (passThroughProcessor) ProcessSubscribe(_ context.Context, _ string, msg *connectors.Message) (*connectors.Message, bool, error) {
	return msg, false, nil
}

func (passThroughProcessor) ProcessUnsubscribe(_ context.Context, _ string, msg *connectors.Message) (*connectors.Message, bool, error) {
	return msg, false, nil
}

func (passThroughProcessor) ProcessInbound(_ context.Context, _ string, msg *connectors.Message) (*connectors.Message, bool, error) {
	return msg, false, nil
}

func (passThroughProcessor) ProcessOutbound(_ context.Context, _ string, msg *connectors.Message) (*connectors.Message, bool, error) {
	return msg, false, nil
}

func (passThroughProcessor) ProcessConnectionInit(_ context.Context, _ string, msg *connectors.Message) (*connectors.Message, bool, error) {
	return msg, false, nil
}

func (passThroughProcessor) ProcessProduce(_ context.Context, _ string, msg *connectors.Message) (*connectors.Message, bool, error) {
	return msg, false, nil
}

func (passThroughProcessor) ProcessConsume(_ context.Context, _ string, msg *connectors.Message) (*connectors.Message, bool, error) {
	return msg, false, nil
}

func (passThroughProcessor) ProcessByChainKey(_ context.Context, _ string, _ string, msg *connectors.Message) (*connectors.Message, bool, error) {
	return msg, false, nil
}

// registryWith returns a registry holding exactly the given channel names.
func registryWith(channels ...string) *TopicRegistry {
	r := NewTopicRegistry()
	for _, c := range channels {
		r.Register(c)
	}
	return r
}

// A subscriber can't know or act on which broker the deployment runs, so these
// assert that no response names any driver.
var driverNames = []string{"kafka", "azureservicebus", "servicebus"}

func assertNamesNoBroker(t *testing.T, body string) {
	t.Helper()
	lower := strings.ToLower(body)
	for _, name := range driverNames {
		if strings.Contains(lower, name) {
			t.Errorf("response body names the broker %q: %s", name, body)
		}
	}
}

func TestHubSubscribe_UnmappedChannelDoesNotNameTheBroker(t *testing.T) {
	h := &HubHandler{
		topics:    registryWith("loginSuccess"),
		processor: passThroughProcessor{},
		channels:  map[string]string{}, // registered, but no broker topic mapped
	}

	form := strings.NewReader("hub.mode=subscribe&hub.callback=https://acme.test/hooks&hub.topic=loginSuccess")
	req := httptest.NewRequest(http.MethodPost, "/hub", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	assertNamesNoBroker(t, rec.Body.String())
}

func TestWebhookReceiver_UnmappedChannelDoesNotNameTheBroker(t *testing.T) {
	h := &WebhookReceiverHandler{
		topics:    registryWith("loginSuccess"),
		processor: passThroughProcessor{},
		channels:  map[string]string{},
	}

	req := httptest.NewRequest(http.MethodPost, "/webhook-receiver?topic=loginSuccess", strings.NewReader("{}"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	assertNamesNoBroker(t, rec.Body.String())
}

// The channel name comes from the caller, so it is still echoed; only the
// broker's name is left out.
func TestHubSubscribe_UnmappedChannelStillEchoesTheChannel(t *testing.T) {
	h := &HubHandler{
		topics:    registryWith("loginSuccess"),
		processor: passThroughProcessor{},
		channels:  map[string]string{},
	}

	form := strings.NewReader("hub.mode=subscribe&hub.callback=https://acme.test/hooks&hub.topic=loginSuccess")
	req := httptest.NewRequest(http.MethodPost, "/hub", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), "loginSuccess") {
		t.Errorf("response does not identify the channel: %s", rec.Body.String())
	}
}

// stubDriver reports a fixed TopicExists answer. The package had no driver stub;
// the other methods return zero values because no test here reaches them.
type stubDriver struct{ exists bool }

func (stubDriver) Type() string                                        { return "kafka" }
func (d stubDriver) TopicExists(context.Context, string) (bool, error) { return d.exists, nil }
func (stubDriver) Publish(context.Context, string, *connectors.Message) error {
	return nil
}
func (stubDriver) Subscribe(string, []string, connectors.MessageHandler) (connectors.Receiver, error) {
	return nil, nil
}
func (stubDriver) SubscribeManual(string, []string, connectors.MessageHandler) (connectors.Receiver, error) {
	return nil, nil
}
func (stubDriver) Replay(context.Context, string, connectors.MessageHandler) error { return nil }
func (stubDriver) Watch(context.Context, string, string, connectors.MessageHandler) (connectors.Receiver, error) {
	return nil, nil
}
func (stubDriver) EnsureTopics(context.Context, []string, map[string]map[string]string) error {
	return nil
}
func (stubDriver) EnsureCompactedTopic(context.Context, string) error { return nil }
func (stubDriver) DeleteTopics(context.Context, []string) error       { return nil }
func (stubDriver) Close() error                                       { return nil }

func TestHubSubscribe_MissingBrokerTopicDoesNotEchoTheTopicHash(t *testing.T) {
	// The broker topic is an internal SHA-256 that tells a subscriber nothing, so
	// the response names the channel they asked for instead.
	const topicHash = "9b20e5d2847060d60712b7e8858cb7a8c3fa638fa4fc443cccb8d6cc1daaef99"
	h := &HubHandler{
		topics:       registryWith("loginSuccess"),
		processor:    passThroughProcessor{},
		channels:     map[string]string{"loginSuccess": topicHash},
		brokerDriver: stubDriver{exists: false},
	}

	form := strings.NewReader("hub.mode=subscribe&hub.callback=https://acme.test/hooks&hub.topic=loginSuccess")
	req := httptest.NewRequest(http.MethodPost, "/hub", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	body := rec.Body.String()
	if strings.Contains(body, topicHash) {
		t.Errorf("response echoes the internal topic hash: %s", body)
	}
	if !strings.Contains(body, "loginSuccess") {
		t.Errorf("response does not name the channel the caller asked for: %s", body)
	}
	assertNamesNoBroker(t, body)
}
