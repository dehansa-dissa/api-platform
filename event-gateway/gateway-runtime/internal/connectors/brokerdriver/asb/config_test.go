package asb

import (
	"testing"

	runtimeconfig "github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/config"
)

const validConnStr = "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA=="

func TestResolveConnectionConfig_UsesGlobalWhenNoOverride(t *testing.T) {
	resolved, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{ConnectionString: validConnStr}, nil)
	if err != nil {
		t.Fatalf("ResolveConnectionConfig returned error: %v", err)
	}
	if resolved.ConnectionString != validConnStr {
		t.Fatalf("expected global connection string to be used, got %q", resolved.ConnectionString)
	}
}

func TestResolveConnectionConfig_OverrideWins(t *testing.T) {
	override := "Endpoint=sb://override.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=b3ZlcnJpZGU="
	resolved, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{ConnectionString: validConnStr}, map[string]interface{}{
		"connection_string": override,
	})
	if err != nil {
		t.Fatalf("ResolveConnectionConfig returned error: %v", err)
	}
	if resolved.ConnectionString != override {
		t.Fatalf("expected binding override to win, got %q", resolved.ConnectionString)
	}
}

func TestResolveConnectionConfig_TrimsWhitespace(t *testing.T) {
	resolved, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{}, map[string]interface{}{
		"connection_string": "  " + validConnStr + "  ",
	})
	if err != nil {
		t.Fatalf("ResolveConnectionConfig returned error: %v", err)
	}
	if resolved.ConnectionString != validConnStr {
		t.Fatalf("expected trimmed connection string, got %q", resolved.ConnectionString)
	}
}

func TestResolveConnectionConfig_RejectsEmpty(t *testing.T) {
	if _, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{}, nil); err == nil {
		t.Fatal("expected error for empty asb connection string")
	}
}

func TestResolveConnectionConfig_RejectsMalformedValue(t *testing.T) {
	_, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{}, map[string]interface{}{
		"connection_string": "not-a-connection-string",
	})
	if err == nil {
		t.Fatal("expected error for malformed asb connection string")
	}
}

func TestResolveConnectionConfig_RejectsNonStringOverride(t *testing.T) {
	_, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{ConnectionString: validConnStr}, map[string]interface{}{
		"connection_string": 12345,
	})
	if err == nil {
		t.Fatal("expected error for non-string connection_string override")
	}
}
