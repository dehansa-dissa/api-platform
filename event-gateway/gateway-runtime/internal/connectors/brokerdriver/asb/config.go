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
	"strings"
	"time"

	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/config"
)

// minSubscriptionIdleTimeout is the shortest AutoDeleteOnIdle Azure accepts.
const minSubscriptionIdleTimeout = 5 * time.Minute

// ConnectionConfig holds the Azure Service Bus connection settings used by the driver.
type ConnectionConfig struct {
	ConnectionString string
	// SubscriptionIdleTimeout is applied as AutoDeleteOnIdle to the driver's
	// disposable subscriptions. Global-only: it protects the namespace against
	// orphans, so a per-binding override could not safely widen it.
	SubscriptionIdleTimeout time.Duration
}

// ResolveConnectionConfig merges global runtime config with per-binding overrides.
func ResolveConnectionConfig(global config.ASBConfig, overrides map[string]interface{}) (ConnectionConfig, error) {
	cfg := ConnectionConfig{
		ConnectionString:        global.ConnectionString,
		SubscriptionIdleTimeout: time.Duration(global.SubscriptionIdleTimeoutSeconds) * time.Second,
	}

	if overrides != nil {
		if v, ok, err := stringOverride(overrides["connection_string"]); err != nil {
			return ConnectionConfig{}, err
		} else if ok {
			cfg.ConnectionString = v
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
}

// validateConnectionConfig fails closed so a misconfigured binding is rejected
// at startup rather than on first use. The connection string is never echoed
// into an error: even a malformed value can carry a live SharedAccessKey.
func validateConnectionConfig(cfg ConnectionConfig) error {
	if cfg.ConnectionString == "" {
		return fmt.Errorf("asb connection string must not be empty")
	}
	if !strings.HasPrefix(cfg.ConnectionString, "Endpoint=sb://") {
		return fmt.Errorf("asb connection string is malformed: expected it to start with \"Endpoint=sb://\"")
	}
	if cfg.SubscriptionIdleTimeout < 0 {
		return fmt.Errorf("asb subscription idle timeout must not be negative")
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
