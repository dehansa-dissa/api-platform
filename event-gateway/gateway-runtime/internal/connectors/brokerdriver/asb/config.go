/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package asb

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/config"
)

const minSubscriptionIdleTimeout = 5 * time.Minute

// ConnectionConfig holds the Azure Service Bus connection settings used by the driver.
type ConnectionConfig struct {
	ConnectionString        string
	Namespace               string
	SubscriptionIdleTimeout time.Duration
	MaxDeliveryCount        int32

	// Set only when the driver may place a WebSubApi in, and create, namespaces
	// beside the configured one. See namespace_pool.go.
	SubscriptionID string
	ResourceGroup  string
}

// ResolveConnectionConfig merges global runtime config with per-binding overrides.
func ResolveConnectionConfig(global config.ASBConfig, overrides map[string]interface{}) (ConnectionConfig, error) {
	if global.MaxDeliveryCount > math.MaxInt32 {
		return ConnectionConfig{}, fmt.Errorf("asb max delivery count must not exceed %d, got %d", math.MaxInt32, global.MaxDeliveryCount)
	}

	cfg := ConnectionConfig{
		ConnectionString:        global.ConnectionString,
		Namespace:               global.Namespace,
		SubscriptionIdleTimeout: time.Duration(global.SubscriptionIdleTimeoutSeconds) * time.Second,
		MaxDeliveryCount:        int32(global.MaxDeliveryCount),
		SubscriptionID:          global.SubscriptionID,
		ResourceGroup:           global.ResourceGroup,
	}

	if overrides != nil {
		connectionString, hasConnectionString, err := stringOverride(overrides["connection_string"])
		if err != nil {
			return ConnectionConfig{}, err
		}
		namespace, hasNamespace, err := stringOverride(overrides["namespace"])
		if err != nil {
			return ConnectionConfig{}, err
		}
		if hasConnectionString && hasNamespace {
			return ConnectionConfig{}, fmt.Errorf("asb binding config must set connection_string or namespace, not both")
		}
		// A binding that names its own target replaces both global fields, so it
		// never pairs its namespace with a credential the runtime held for another.
		switch {
		case hasConnectionString:
			cfg.ConnectionString, cfg.Namespace = connectionString, ""
		case hasNamespace:
			cfg.ConnectionString, cfg.Namespace = "", namespace
		}
		if hasConnectionString || hasNamespace {
			cfg.SubscriptionID, cfg.ResourceGroup = "", ""
		}
	}

	normalizeConnectionConfig(&cfg)
	if err := validateConnectionConfig(cfg); err != nil {
		return ConnectionConfig{}, err
	}
	return cfg, nil
}

func normalizeConnectionConfig(cfg *ConnectionConfig) {
	cfg.ConnectionString = strings.TrimSpace(cfg.ConnectionString)
	cfg.Namespace = strings.ToLower(strings.TrimSpace(cfg.Namespace))
	cfg.SubscriptionID = strings.TrimSpace(cfg.SubscriptionID)
	cfg.ResourceGroup = strings.TrimSpace(cfg.ResourceGroup)
}

// validateConnectionConfig fails closed so a misconfigured binding is rejected
// at startup rather than on first use.
func validateConnectionConfig(cfg ConnectionConfig) error {
	switch {
	case cfg.ConnectionString != "" && cfg.Namespace != "":
		return fmt.Errorf("asb connection string and namespace are mutually exclusive")
	case cfg.Namespace != "":
		if err := config.ValidateASBNamespace(cfg.Namespace); err != nil {
			return fmt.Errorf("asb %w", err)
		}
	case cfg.ConnectionString == "":
		return fmt.Errorf("asb connection string or namespace must be set")
	case !strings.HasPrefix(cfg.ConnectionString, "Endpoint=sb://"):
		return fmt.Errorf("asb connection string is malformed: expected it to start with \"Endpoint=sb://\"")
	}
	if cfg.SubscriptionIdleTimeout < 0 {
		return fmt.Errorf("asb subscription idle timeout must not be negative")
	}
	if cfg.MaxDeliveryCount < 0 {
		return fmt.Errorf("asb max delivery count must not be negative")
	}
	if cfg.SubscriptionIdleTimeout > 0 && cfg.SubscriptionIdleTimeout < minSubscriptionIdleTimeout {
		return fmt.Errorf("asb subscription idle timeout must be 0 (disabled) or at least %s, got %s", minSubscriptionIdleTimeout, cfg.SubscriptionIdleTimeout)
	}
	return nil
}

// iso8601Seconds renders a duration in the ATOM-XML form the management API
// expects for AutoDeleteOnIdle.
func iso8601Seconds(d time.Duration) string {
	return fmt.Sprintf("PT%dS", int64(d.Seconds()))
}

// Largest whole-day count whose nanoseconds still fit an int64.
const maxISO8601Days = 106751

// sameIdleTimeout compares two ISO-8601 durations by value: Azure normalises
// what it stores, so PT600S reads back as PT10M. Unparsable falls back to ==.
func sameIdleTimeout(a, b string) bool {
	if a == b {
		return true
	}
	left, okLeft := iso8601Duration(a)
	right, okRight := iso8601Duration(b)
	return okLeft && okRight && left == right
}

// iso8601Duration parses the P[nD][T...] form Azure emits for AutoDeleteOnIdle.
// Months, years and values beyond time.Duration read as unparsable.
func iso8601Duration(value string) (time.Duration, bool) {
	rest, ok := strings.CutPrefix(value, "P")
	if !ok || rest == "" {
		return 0, false
	}
	var days time.Duration
	if d, after, found := strings.Cut(rest, "D"); found {
		n, err := strconv.Atoi(d)
		if err != nil || n < 0 || n > maxISO8601Days {
			return 0, false
		}
		days, rest = time.Duration(n)*24*time.Hour, after
	}
	if rest == "" {
		return days, true
	}
	clock, ok := strings.CutPrefix(rest, "T")
	if !ok {
		return 0, false
	}
	elapsed, err := time.ParseDuration(strings.ToLower(clock))
	if err != nil || elapsed < 0 || elapsed > math.MaxInt64-days {
		return 0, false
	}
	return days + elapsed, true
}

func stringOverride(value interface{}) (string, bool, error) {
	if value == nil {
		return "", false, nil
	}
	v, ok := value.(string)
	if !ok {
		return "", false, fmt.Errorf("expected string ASB config override, got %T", value)
	}
	return v, true, nil
}
