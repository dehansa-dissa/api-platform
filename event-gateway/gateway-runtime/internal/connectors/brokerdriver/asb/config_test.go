package asb

import (
	"testing"
	"time"

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

func TestResolveConnectionConfig_CarriesSubscriptionIdleTimeout(t *testing.T) {
	resolved, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{
		ConnectionString:               validConnStr,
		SubscriptionIdleTimeoutSeconds: 3600,
	}, nil)
	if err != nil {
		t.Fatalf("ResolveConnectionConfig returned error: %v", err)
	}
	if resolved.SubscriptionIdleTimeout != time.Hour {
		t.Fatalf("expected 1h idle timeout, got %s", resolved.SubscriptionIdleTimeout)
	}
}

func TestResolveConnectionConfig_AllowsDisabledSubscriptionIdleTimeout(t *testing.T) {
	resolved, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{ConnectionString: validConnStr}, nil)
	if err != nil {
		t.Fatalf("ResolveConnectionConfig returned error: %v", err)
	}
	if resolved.SubscriptionIdleTimeout != 0 {
		t.Fatalf("expected reclamation to be disabled, got %s", resolved.SubscriptionIdleTimeout)
	}
}

func TestResolveConnectionConfig_RejectsIdleTimeoutBelowAzureMinimum(t *testing.T) {
	_, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{
		ConnectionString:               validConnStr,
		SubscriptionIdleTimeoutSeconds: 60,
	}, nil)
	if err == nil {
		t.Fatal("expected error for an idle timeout below the five-minute ASB minimum")
	}
}

func TestISO8601Seconds_RendersManagementAPIDuration(t *testing.T) {
	for input, want := range map[time.Duration]string{
		5 * time.Minute: "PT300S",
		time.Hour:       "PT3600S",
		24 * time.Hour:  "PT86400S",
	} {
		if got := iso8601Seconds(input); got != want {
			t.Fatalf("iso8601Seconds(%s) = %q, want %q", input, got, want)
		}
	}
}
