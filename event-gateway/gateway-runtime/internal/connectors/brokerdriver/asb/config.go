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

	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/config"
)

// ConnectionConfig holds the Azure Service Bus connection settings used by the driver.
type ConnectionConfig struct {
	ConnectionString string
}

// ResolveConnectionConfig merges global runtime config with per-binding overrides.
func ResolveConnectionConfig(global config.ASBConfig, overrides map[string]interface{}) (ConnectionConfig, error) {
	cfg := ConnectionConfig{
		ConnectionString: global.ConnectionString,
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

// validateConnectionConfig fails closed: an azureservicebus broker-driver binding
// with no usable connection string must never be allowed to start (it would
// otherwise fail lazily, and confusingly, on the first Publish/Subscribe call).
// The connection string itself is never included in an error message — even a
// malformed value can contain a live SharedAccessKey (see GO-AUTH-003).
func validateConnectionConfig(cfg ConnectionConfig) error {
	if cfg.ConnectionString == "" {
		return fmt.Errorf("asb connection string must not be empty")
	}
	if !strings.HasPrefix(cfg.ConnectionString, "Endpoint=sb://") {
		return fmt.Errorf("asb connection string is malformed: expected it to start with \"Endpoint=sb://\"")
	}
	return nil
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
