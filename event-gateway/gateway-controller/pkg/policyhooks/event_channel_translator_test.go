package policyhooks

import (
	"encoding/json"
	"log/slog"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	eventgateway "github.com/wso2/api-platform/event-gateway/gateway-controller/pkg/api/eventgateway"
)

// decodeResource unwraps the *anypb.Any{*structpb.Struct} produced by
// policyxds.ToAnyResource back into a plain map, mirroring how
// gateway-runtime/internal/xdsclient/handler.go decodes it on the receiving
// end (minus the outer LinearCache Any wrapping, which only applies to
// resources that have gone through the actual xDS cache/transport).
func decodeResource(t *testing.T, res interface{}) map[string]interface{} {
	t.Helper()
	msg, ok := res.(proto.Message)
	if !ok {
		t.Fatalf("resource is not a proto.Message: %T", res)
	}
	anyMsg, ok := msg.(*anypb.Any)
	if !ok {
		t.Fatalf("resource is not *anypb.Any: %T", res)
	}
	// anyMsg.TypeUrl is deliberately overridden to a custom xDS type URL (see
	// policyxds.ToAnyResource), so UnmarshalTo's type-URL check would reject
	// it — unmarshal the raw bytes directly instead, exactly as
	// gateway-runtime/internal/xdsclient/handler.go does on the receiving end.
	s := &structpb.Struct{}
	if err := proto.Unmarshal(anyMsg.Value, s); err != nil {
		t.Fatalf("failed to unmarshal inner struct: %v", err)
	}
	raw, err := json.Marshal(s.AsMap())
	if err != nil {
		t.Fatalf("failed to marshal struct to JSON: %v", err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("failed to unmarshal JSON to map: %v", err)
	}
	return out
}

func TestBuildEventChannelResourceForWebSub_OmittedBrokerDefaultsToEmpty(t *testing.T) {
	translator := New(slog.Default())
	cfg := &eventgateway.WebSubAPI{
		Metadata: eventgateway.Metadata{Name: "orders"},
		Spec: eventgateway.WebhookAPIData{
			Context: "/orders",
			Version: "v1.0",
			// Broker intentionally left nil — must fall through to the
			// runtime's Kafka default (see resolveBrokerDriver in
			// event-gateway/gateway-runtime/internal/xdsclient/handler.go),
			// preserving behavior for every WebSubApi created before this
			// field existed.
		},
	}

	res, err := translator.buildEventChannelResourceForWebSub("uuid-1", cfg)
	if err != nil {
		t.Fatalf("buildEventChannelResourceForWebSub returned error: %v", err)
	}

	data := decodeResource(t, res)
	brokerDriver, ok := data["broker-driver"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected broker-driver to be an object, got %T: %v", data["broker-driver"], data["broker-driver"])
	}
	if brokerType, present := brokerDriver["type"]; present && brokerType != "" {
		t.Fatalf("expected no broker type when Broker is omitted, got %v", brokerType)
	}
}

func TestBuildEventChannelResourceForWebSub_ExplicitBrokerIsEmitted(t *testing.T) {
	translator := New(slog.Default())
	name := "asb-driver"
	props := map[string]interface{}{"connection_string": "Endpoint=sb://example/"}
	cfg := &eventgateway.WebSubAPI{
		Metadata: eventgateway.Metadata{Name: "orders"},
		Spec: eventgateway.WebhookAPIData{
			Context: "/orders",
			Version: "v1.0",
			Broker: &eventgateway.WebSubApiBroker{
				Type:       "azureservicebus",
				Name:       &name,
				Properties: &props,
			},
		},
	}

	res, err := translator.buildEventChannelResourceForWebSub("uuid-2", cfg)
	if err != nil {
		t.Fatalf("buildEventChannelResourceForWebSub returned error: %v", err)
	}

	data := decodeResource(t, res)
	brokerDriver, ok := data["broker-driver"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected broker-driver to be an object, got %T: %v", data["broker-driver"], data["broker-driver"])
	}
	if got := brokerDriver["type"]; got != "azureservicebus" {
		t.Fatalf("expected broker type %q, got %v", "azureservicebus", got)
	}
	if got := brokerDriver["name"]; got != "asb-driver" {
		t.Fatalf("expected broker name %q, got %v", "asb-driver", got)
	}
	propsOut, ok := brokerDriver["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected properties to be an object, got %T", brokerDriver["properties"])
	}
	if propsOut["connection_string"] != "Endpoint=sb://example/" {
		t.Fatalf("expected connection_string property to round-trip, got %v", propsOut["connection_string"])
	}
}
