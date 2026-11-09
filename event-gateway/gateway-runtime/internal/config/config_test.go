package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestLoadAppliesEnvironmentOverrides(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "tls.crt")
	keyPath := filepath.Join(tempDir, "tls.key")
	if err := os.WriteFile(certPath, []byte("cert"), 0o644); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte("key"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	t.Setenv("APIP_EGW_SERVER_WEBSUB_ENABLED", "false")
	t.Setenv("APIP_EGW_SERVER_WEBSUB_HTTP_PORT", "9090")
	t.Setenv("APIP_EGW_SERVER_WEBSUB_HTTPS_PORT", "9443")
	t.Setenv("APIP_EGW_SERVER_WEBSUB_TLS_ENABLED", "true")
	t.Setenv("APIP_EGW_SERVER_WEBSUB_TLS_CERT_FILE", certPath)
	t.Setenv("APIP_EGW_SERVER_WEBSUB_TLS_KEY_FILE", keyPath)
	t.Setenv("APIP_EGW_KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092")
	t.Setenv("APIP_EGW_CONTROLPLANE_ENABLED", "true")
	t.Setenv("APIP_EGW_CONTROLPLANE_XDS_ADDRESS", "xds:18001")
	t.Setenv("APIP_EGW_POLICY_ENGINE_CONFIG_FILE", "/tmp/policies.toml")
	t.Setenv("APIP_EGW_LOGGING_LEVEL", "debug")
	t.Setenv("APIP_EGW_LOGGING_FORMAT", "json")
	t.Setenv("APIP_EGW_WEBSUB_SUBSCRIPTIONS_TOPIC_NAME", "websub.subscriptions")

	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[server]
websub_http_port = 8080
websub_https_port = 8443

[kafka]
brokers = ["localhost:9092"]

[controlplane]
enabled = false
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, _, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Server.WebSubEnabled {
		t.Fatalf("expected websub to be disabled")
	}
	if cfg.Server.WebSubHTTPPort != 9090 {
		t.Fatalf("expected websub http port 9090, got %d", cfg.Server.WebSubHTTPPort)
	}
	if cfg.Server.WebSubHTTPSPort != 9443 {
		t.Fatalf("expected websub https port 9443, got %d", cfg.Server.WebSubHTTPSPort)
	}
	if !cfg.Server.WebSubTLSEnabled {
		t.Fatalf("expected websub TLS to be enabled")
	}
	if cfg.Server.WebSubTLSCertFile != certPath {
		t.Fatalf("expected websub TLS cert file override, got %q", cfg.Server.WebSubTLSCertFile)
	}
	if cfg.Server.WebSubTLSKeyFile != keyPath {
		t.Fatalf("expected websub TLS key file override, got %q", cfg.Server.WebSubTLSKeyFile)
	}

	wantBrokers := []string{"kafka-1:9092", "kafka-2:9092"}
	if !reflect.DeepEqual(cfg.Kafka.Brokers, wantBrokers) {
		t.Fatalf("expected brokers %v, got %v", wantBrokers, cfg.Kafka.Brokers)
	}

	if !cfg.ControlPlane.Enabled {
		t.Fatalf("expected control plane to be enabled")
	}

	if cfg.ControlPlane.XDSAddress != "xds:18001" {
		t.Fatalf("expected xds address xds:18001, got %q", cfg.ControlPlane.XDSAddress)
	}

	if cfg.PolicyEngine.ConfigFile != "/tmp/policies.toml" {
		t.Fatalf("expected policy engine config file override, got %q", cfg.PolicyEngine.ConfigFile)
	}
	if cfg.WebSub.SubscriptionsTopicName != "websub.subscriptions" {
		t.Fatalf("expected websub subscription sync topic override, got %q", cfg.WebSub.SubscriptionsTopicName)
	}
	if cfg.Logging.Level != "debug" {
		t.Fatalf("expected logging level debug, got %q", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Fatalf("expected logging format json, got %q", cfg.Logging.Format)
	}
}

// TestLoadAppliesASBEnvironmentOverride guards against mapEnvKey's generic
// fallback (which turns every remaining underscore into a dot) mis-mapping
// APIP_EGW_ASB_CONNECTION_STRING to "asb.connection.string" instead of the
// koanf tag's actual "asb.connection_string" — a real regression that shipped
// once already and silently left cfg.ASB.ConnectionString empty regardless of
// what the operator exported.
func TestLoadAppliesASBEnvironmentOverride(t *testing.T) {
	t.Setenv("APIP_EGW_ASB_CONNECTION_STRING", "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==")

	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[kafka]
brokers = ["localhost:9092"]
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, _, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	want := "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA=="
	if cfg.ASB.ConnectionString != want {
		t.Fatalf("expected asb connection string %q, got %q", want, cfg.ASB.ConnectionString)
	}
}

func TestLoadAppliesASBNamespaceEnvironmentOverride(t *testing.T) {
	t.Setenv("APIP_EGW_ASB_NAMESPACE", "example.servicebus.windows.net")

	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[kafka]
brokers = ["localhost:9092"]
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, _, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ASB.Namespace != "example.servicebus.windows.net" {
		t.Fatalf("expected asb namespace from the environment, got %q", cfg.ASB.Namespace)
	}
}

func TestValidateASBConfig_RejectsConnectionStringAndNamespaceTogether(t *testing.T) {
	err := validateASBConfig(ASBConfig{
		ConnectionString: "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=dGVzdA==",
		Namespace:        "example.servicebus.windows.net",
	})
	if err == nil {
		t.Fatal("expected setting both credentials to be rejected")
	}
}

func TestValidateASBNamespace(t *testing.T) {
	for _, valid := range []string{
		"example.servicebus.windows.net",
		"asgardeo-events-01.servicebus.windows.net",
		"example.servicebus.chinacloudapi.cn",
	} {
		if err := ValidateASBNamespace(valid); err != nil {
			t.Errorf("ValidateASBNamespace(%q) = %v, want nil", valid, err)
		}
	}
	for _, invalid := range []string{
		"",
		"example",
		"sb://example.servicebus.windows.net",
		"https://example.servicebus.windows.net",
		"example.servicebus.windows.net/",
		"example.servicebus.windows.net:5671",
		"example.servicebus.windows.net/path",
		"user@example.servicebus.windows.net",
		"-example.servicebus.windows.net",
		"example..servicebus.windows.net",
		"exa mple.servicebus.windows.net",
	} {
		if err := ValidateASBNamespace(invalid); err == nil {
			t.Errorf("ValidateASBNamespace(%q) = nil, want an error", invalid)
		}
	}
}

// TestLoadAppliesASBIdleTimeoutEnvironmentOverride covers the other half of the
// env path: mapEnvValue only runs strconv.Atoi for paths it lists explicitly, so
// an int field left off that list arrives as a string.
func TestLoadAppliesASBIdleTimeoutEnvironmentOverride(t *testing.T) {
	t.Setenv("APIP_EGW_ASB_SUBSCRIPTION_IDLE_TIMEOUT_SECONDS", "300")

	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[kafka]
brokers = ["localhost:9092"]
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, _, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ASB.SubscriptionIdleTimeoutSeconds != 300 {
		t.Fatalf("expected idle timeout 300, got %d", cfg.ASB.SubscriptionIdleTimeoutSeconds)
	}
}

func TestLoadRequiresWebSubTLSFilesWhenEnabled(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[server]
websub_enabled = true
websub_tls_enabled = true
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, _, err := Load(configPath)
	if err == nil {
		t.Fatalf("expected load to fail when TLS files are missing")
	}

	want := "server.websub_tls_cert_file is required when server.websub_tls_enabled is true"
	if err.Error() != want {
		t.Fatalf("expected error %q, got %q", want, err.Error())
	}
}

func TestLoadRequiresTLSFilesEvenWhenWebSubDisabled(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[server]
websub_enabled = false
websub_tls_enabled = true
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, _, err := Load(configPath)
	if err == nil {
		t.Fatalf("expected load to fail when TLS is enabled without readable files")
	}

	want := "server.websub_tls_cert_file is required when server.websub_tls_enabled is true"
	if err.Error() != want {
		t.Fatalf("expected error %q, got %q", want, err.Error())
	}
}

func TestLoadRejectsInvalidLoggingLevel(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[logging]
level = "trace"
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, _, err := Load(configPath)
	if err == nil {
		t.Fatalf("expected load to fail for invalid logging level")
	}

	want := "logging.level must be one of debug, info, warn, error"
	if err.Error() != want {
		t.Fatalf("expected error %q, got %q", want, err.Error())
	}
}

func TestLoadRejectsMissingTLSFilesWhenEnabled(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	missingCert := filepath.Join(t.TempDir(), "missing.crt")
	missingKey := filepath.Join(t.TempDir(), "missing.key")
	if err := os.WriteFile(configPath, []byte(`
[server]
websub_enabled = true
websub_tls_enabled = true
websub_tls_cert_file = "`+missingCert+`"
websub_tls_key_file = "`+missingKey+`"
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, _, err := Load(configPath)
	if err == nil {
		t.Fatalf("expected load to fail when TLS cert file is missing")
	}

	if !strings.Contains(err.Error(), `server.websub_tls_cert_file file "`) || !strings.Contains(err.Error(), `does not exist`) {
		t.Fatalf("expected missing cert file error, got %q", err.Error())
	}
}

func TestLoadRejectsUnreadableTLSKeyFileWhenEnabled(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based readability checks are unreliable when running as root")
	}

	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "tls.crt")
	keyPath := filepath.Join(tempDir, "tls.key")
	configPath := filepath.Join(tempDir, "config.toml")

	if err := os.WriteFile(certPath, []byte("cert"), 0o644); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte("key"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if err := os.Chmod(keyPath, 0o000); err != nil {
		t.Fatalf("chmod key unreadable: %v", err)
	}
	defer func() {
		_ = os.Chmod(keyPath, 0o600)
	}()

	if err := os.WriteFile(configPath, []byte(`
[server]
websub_enabled = true
websub_tls_enabled = true
websub_tls_cert_file = "`+certPath+`"
websub_tls_key_file = "`+keyPath+`"
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, _, err := Load(configPath)
	if err == nil {
		t.Fatalf("expected load to fail when TLS key file is unreadable")
	}

	if !strings.Contains(err.Error(), `server.websub_tls_key_file file "`) || !strings.Contains(err.Error(), `is not readable`) {
		t.Fatalf("expected unreadable key file error, got %q", err.Error())
	}
}

func TestLoadAppliesWebSocketTLSEnvironmentOverrides(t *testing.T) {
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "tls.crt")
	keyPath := filepath.Join(tempDir, "tls.key")
	if err := os.WriteFile(certPath, []byte("cert"), 0o644); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte("key"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	t.Setenv("APIP_EGW_SERVER_WEBSOCKET_HTTPS_PORT", "9444")
	t.Setenv("APIP_EGW_SERVER_WEBSOCKET_TLS_ENABLED", "true")
	t.Setenv("APIP_EGW_SERVER_WEBSOCKET_TLS_CERT_FILE", certPath)
	t.Setenv("APIP_EGW_SERVER_WEBSOCKET_TLS_KEY_FILE", keyPath)

	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[kafka]
brokers = ["localhost:9092"]
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, _, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Server.WebSocketHTTPSPort != 9444 {
		t.Fatalf("expected websocket https port 9444, got %d", cfg.Server.WebSocketHTTPSPort)
	}
	if !cfg.Server.WebSocketTLSEnabled {
		t.Fatalf("expected websocket TLS to be enabled")
	}
	if cfg.Server.WebSocketTLSCertFile != certPath {
		t.Fatalf("expected websocket TLS cert file override, got %q", cfg.Server.WebSocketTLSCertFile)
	}
	if cfg.Server.WebSocketTLSKeyFile != keyPath {
		t.Fatalf("expected websocket TLS key file override, got %q", cfg.Server.WebSocketTLSKeyFile)
	}
}

func TestLoadRequiresWebSocketTLSFilesWhenEnabled(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[server]
websocket_tls_enabled = true
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, _, err := Load(configPath)
	if err == nil {
		t.Fatalf("expected load to fail when TLS files are missing")
	}

	want := "server.websocket_tls_cert_file is required when server.websocket_tls_enabled is true"
	if err.Error() != want {
		t.Fatalf("expected error %q, got %q", want, err.Error())
	}
}

func TestLoadRejectsMissingWebSocketTLSFilesWhenEnabled(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	missingCert := filepath.Join(t.TempDir(), "missing.crt")
	missingKey := filepath.Join(t.TempDir(), "missing.key")
	if err := os.WriteFile(configPath, []byte(`
[server]
websocket_tls_enabled = true
websocket_tls_cert_file = "`+missingCert+`"
websocket_tls_key_file = "`+missingKey+`"
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, _, err := Load(configPath)
	if err == nil {
		t.Fatalf("expected load to fail when TLS cert file is missing")
	}

	if !strings.Contains(err.Error(), `server.websocket_tls_cert_file file "`) || !strings.Contains(err.Error(), `does not exist`) {
		t.Fatalf("expected missing cert file error, got %q", err.Error())
	}
}

func TestLoadRejectsUnreadableWebSocketTLSKeyFileWhenEnabled(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based readability checks are unreliable when running as root")
	}

	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "tls.crt")
	keyPath := filepath.Join(tempDir, "tls.key")
	configPath := filepath.Join(tempDir, "config.toml")

	if err := os.WriteFile(certPath, []byte("cert"), 0o644); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, []byte("key"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if err := os.Chmod(keyPath, 0o000); err != nil {
		t.Fatalf("chmod key unreadable: %v", err)
	}
	defer func() {
		_ = os.Chmod(keyPath, 0o600)
	}()

	if err := os.WriteFile(configPath, []byte(`
[server]
websocket_tls_enabled = true
websocket_tls_cert_file = "`+certPath+`"
websocket_tls_key_file = "`+keyPath+`"
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, _, err := Load(configPath)
	if err == nil {
		t.Fatalf("expected load to fail when TLS key file is unreadable")
	}

	if !strings.Contains(err.Error(), `server.websocket_tls_key_file file "`) || !strings.Contains(err.Error(), `is not readable`) {
		t.Fatalf("expected unreadable key file error, got %q", err.Error())
	}
}

func TestLoadRejectsNonPositiveServerPorts(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[server]
websub_http_port = 0
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, _, err := Load(configPath)
	if err == nil {
		t.Fatalf("expected load to fail for non-positive port")
	}

	want := "server.websub_http_port must be a positive integer, got 0"
	if err.Error() != want {
		t.Fatalf("expected error %q, got %q", want, err.Error())
	}
}

func TestLoadRejectsDuplicateServerPorts(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[server]
websub_http_port = 8080
websub_https_port = 8443
websocket_port = 8080
admin_port = 9002
metrics_port = 9003
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, _, err := Load(configPath)
	if err == nil {
		t.Fatalf("expected load to fail for duplicate ports")
	}

	want := "server.websocket_port port 8080 conflicts with server.websub_http_port"
	if err.Error() != want {
		t.Fatalf("expected error %q, got %q", want, err.Error())
	}
}

func TestLoadAppliesASBNamespacePoolEnvironmentOverrides(t *testing.T) {
	t.Setenv("APIP_EGW_ASB_NAMESPACE", "example.servicebus.windows.net")
	t.Setenv("APIP_EGW_ASB_SUBSCRIPTION_ID", "00000000-1111-2222-3333-444444444444")
	t.Setenv("APIP_EGW_ASB_RESOURCE_GROUP", "rg-events")

	configPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(configPath, []byte(`
[kafka]
brokers = ["localhost:9092"]
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, _, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.ASB.SubscriptionID != "00000000-1111-2222-3333-444444444444" || cfg.ASB.ResourceGroup != "rg-events" {
		t.Fatalf("pool settings not read from the environment: %+v", cfg.ASB)
	}
}

func TestValidateASBConfig_NamespacePool(t *testing.T) {
	const subscriptionID = "00000000-1111-2222-3333-444444444444"
	pool := ASBConfig{Namespace: "example.servicebus.windows.net", SubscriptionID: subscriptionID, ResourceGroup: "rg-events"}
	if err := validateASBConfig(pool); err != nil {
		t.Fatalf("a complete pool was rejected: %v", err)
	}

	for name, mutate := range map[string]func(*ASBConfig){
		"no resource group":      func(c *ASBConfig) { c.ResourceGroup = "" },
		"no subscription":        func(c *ASBConfig) { c.SubscriptionID = "" },
		"no first namespace":     func(c *ASBConfig) { c.Namespace = "" },
		"subscription not GUID":  func(c *ASBConfig) { c.SubscriptionID = "APIM-rnd-001" },
		"resource group path":    func(c *ASBConfig) { c.ResourceGroup = "rg/../other" },
		"resource group dot end": func(c *ASBConfig) { c.ResourceGroup = "rg." },
	} {
		cfg := pool
		mutate(&cfg)
		if err := validateASBConfig(cfg); err == nil {
			t.Errorf("%s: accepted %+v", name, cfg)
		}
	}
}
