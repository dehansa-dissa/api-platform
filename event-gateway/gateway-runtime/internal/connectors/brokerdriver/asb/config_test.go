package asb

import (
	"math"
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

func TestSameIdleTimeout_MatchesAcrossAzuresNormalisation(t *testing.T) {
	for _, tc := range []struct {
		sent, storedBack string
	}{
		{"PT600S", "PT10M"},
		{"PT3600S", "PT1H"},
		{"PT5400S", "PT1H30M"},
		{"PT86400S", "P1D"},
	} {
		if !sameIdleTimeout(tc.sent, tc.storedBack) {
			t.Errorf("sent %s read back as %s was treated as a change", tc.sent, tc.storedBack)
		}
	}
}

func TestSameIdleTimeout_DistinguishesRealChanges(t *testing.T) {
	if sameIdleTimeout("PT10M", "PT30M") {
		t.Fatal("a genuinely changed timeout was skipped, so config would never reach Azure")
	}
}

func TestSameIdleTimeout_UnparsableFallsBackToStringEquality(t *testing.T) {
	const nonsense = "not-a-duration"
	if !sameIdleTimeout(nonsense, nonsense) {
		t.Fatal("identical unparsable values must still compare equal")
	}
	if sameIdleTimeout(nonsense, "PT10M") {
		t.Fatal("an unparsable value must never be treated as equal to a parsed one")
	}
}

func TestISO8601Duration_RejectsAzuresNeverValue(t *testing.T) {
	const never = "P10675199DT2H48M5.4775807S"

	if _, ok := iso8601Duration(never); ok {
		t.Fatal("a value beyond time.Duration must be reported as not comparable")
	}

	if !sameIdleTimeout(never, never) {
		t.Fatal("two identical never values must compare equal")
	}
}

func TestISO8601Duration_RejectsVariableLengthAndMalformedUnits(t *testing.T) {
	for _, invalid := range []string{
		"P1Y", "P2M", "", "P", "PT", "10M", "PTS", "PT10X",
		"PT1.5M", "PT10M20", "PPT10M", "PT10MT20S",
		"P99999999999999999999D",
	} {
		if _, ok := iso8601Duration(invalid); ok {
			t.Errorf("iso8601Duration(%q) was accepted", invalid)
		}
	}
}

func TestISO8601Duration_RoundTripsWhatThisPackageRenders(t *testing.T) {
	for _, d := range []time.Duration{5 * time.Minute, 10 * time.Minute, time.Hour, 24 * time.Hour} {
		got, ok := iso8601Duration(iso8601Seconds(d))
		if !ok {
			t.Fatalf("iso8601Seconds(%s) produced a value its own parser rejects", d)
		}
		if got != d {
			t.Fatalf("round trip of %s produced %s", d, got)
		}
	}
}

func TestISO8601Duration_ParsesFractionalSeconds(t *testing.T) {
	got, ok := iso8601Duration("PT1.5S")
	if !ok || got != 1500*time.Millisecond {
		t.Fatalf("iso8601Duration(\"PT1.5S\") = %s, %v; want 1.5s, true", got, ok)
	}
}

func TestResolveConnectionConfig_RejectsMaxDeliveryCountBeyondInt32(t *testing.T) {
	_, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{
		ConnectionString: validConnStr,
		MaxDeliveryCount: 1<<32 + 10,
	}, nil)
	if err == nil {
		t.Fatal("expected error for max delivery count beyond int32")
	}
}

func TestResolveConnectionConfig_AcceptsMaxDeliveryCountAtInt32(t *testing.T) {
	resolved, err := ResolveConnectionConfig(runtimeconfig.ASBConfig{
		ConnectionString: validConnStr,
		MaxDeliveryCount: math.MaxInt32,
	}, nil)
	if err != nil {
		t.Fatalf("ResolveConnectionConfig returned error: %v", err)
	}
	if resolved.MaxDeliveryCount != math.MaxInt32 {
		t.Fatalf("MaxDeliveryCount = %d, want %d", resolved.MaxDeliveryCount, int32(math.MaxInt32))
	}
}
