package xdsclient

import "testing"

func TestResolveBrokerDriver_EmptyTypeDefaultsToKafkaWithLocalConfig(t *testing.T) {
	h := &Handler{kafkaConfig: KafkaConfig{Brokers: []string{"broker-1:9092"}}}

	spec := h.resolveBrokerDriver(BrokerDriverEntry{})

	if spec.Type != "kafka" {
		t.Fatalf("expected default type %q, got %q", "kafka", spec.Type)
	}
	brokers, _ := spec.Config["brokers"].([]string)
	if len(brokers) != 1 || brokers[0] != "broker-1:9092" {
		t.Fatalf("expected local kafka brokers to be used as fallback config, got %v", spec.Config["brokers"])
	}
}

func TestResolveBrokerDriver_NonKafkaTypeWithEmptyPropertiesStaysEmpty(t *testing.T) {
	h := &Handler{kafkaConfig: KafkaConfig{Brokers: []string{"broker-1:9092"}}}

	spec := h.resolveBrokerDriver(BrokerDriverEntry{Type: "azureservicebus"})

	if spec.Type != "azureservicebus" {
		t.Fatalf("expected type %q to be preserved, got %q", "azureservicebus", spec.Type)
	}
	if len(spec.Config) != 0 {
		t.Fatalf("expected no kafka-shaped fallback config for a non-kafka driver, got %v", spec.Config)
	}
}

func TestResolveBrokerDriver_NonKafkaTypeWithExplicitPropertiesPassesThrough(t *testing.T) {
	h := &Handler{kafkaConfig: KafkaConfig{Brokers: []string{"broker-1:9092"}}}

	spec := h.resolveBrokerDriver(BrokerDriverEntry{
		Type:       "azureservicebus",
		Properties: map[string]interface{}{"connection_string": "Endpoint=sb://example/"},
	})

	if got := spec.Config["connection_string"]; got != "Endpoint=sb://example/" {
		t.Fatalf("expected explicit connection_string to pass through, got %v", got)
	}
	if _, hasKafkaKey := spec.Config["brokers"]; hasKafkaKey {
		t.Fatalf("did not expect kafka-shaped keys to be injected alongside explicit asb properties")
	}
}
