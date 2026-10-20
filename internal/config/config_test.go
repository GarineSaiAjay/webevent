package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

const testKey = "test-key-0123456789abcdef0123456789"

// lookupFrom returns a lookup over m. Required settings get valid defaults
// unless the test sets them explicitly (use "" to simulate "unset").
func lookupFrom(m map[string]string) func(string) (string, bool) {
	if _, ok := m["WEBEVENT_API_KEYS"]; !ok {
		m["WEBEVENT_API_KEYS"] = testKey
	}
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestDefaults(t *testing.T) {
	cfg, err := FromLookup(lookupFrom(map[string]string{
		"WEBEVENT_DATABASE_URL": "postgres://localhost/webevent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.DBMaxConns != 20 {
		t.Errorf("DBMaxConns = %d, want 20", cfg.DBMaxConns)
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want info", cfg.LogLevel)
	}
	if !cfg.AutoMigrate {
		t.Error("AutoMigrate should default to true")
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 30s", cfg.ShutdownTimeout)
	}
	if cfg.WorkerPollInterval != time.Second || cfg.DeliveryTimeout != 15*time.Second {
		t.Errorf("delivery defaults wrong: poll=%v timeout=%v", cfg.WorkerPollInterval, cfg.DeliveryTimeout)
	}
}

func TestDeliveryTimeoutBounds(t *testing.T) {
	for _, v := range []string{"500ms", "10m"} {
		_, err := FromLookup(lookupFrom(map[string]string{
			"WEBEVENT_DATABASE_URL":     "postgres://db/x",
			"WEBEVENT_DELIVERY_TIMEOUT": v,
		}))
		if err == nil || !strings.Contains(err.Error(), "WEBEVENT_DELIVERY_TIMEOUT") {
			t.Errorf("timeout %s: expected bounds error, got %v", v, err)
		}
	}
}

func TestOverrides(t *testing.T) {
	cfg, err := FromLookup(lookupFrom(map[string]string{
		"WEBEVENT_DATABASE_URL":     "postgres://db/x",
		"WEBEVENT_HTTP_ADDR":        "127.0.0.1:9000",
		"WEBEVENT_DB_MAX_CONNS":     "50",
		"WEBEVENT_LOG_LEVEL":        "debug",
		"WEBEVENT_LOG_FORMAT":       "text",
		"WEBEVENT_AUTO_MIGRATE":     "false",
		"WEBEVENT_SHUTDOWN_TIMEOUT": "5s",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9000" || cfg.DBMaxConns != 50 || cfg.LogLevel != slog.LevelDebug ||
		cfg.LogFormat != "text" || cfg.AutoMigrate || cfg.ShutdownTimeout != 5*time.Second {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
}

func TestInvalidValuesAreAllReported(t *testing.T) {
	_, err := FromLookup(lookupFrom(map[string]string{
		"WEBEVENT_DATABASE_URL":     "postgres://db/x",
		"WEBEVENT_DB_MAX_CONNS":     "lots",
		"WEBEVENT_AUTO_MIGRATE":     "maybe",
		"WEBEVENT_SHUTDOWN_TIMEOUT": "forever",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"WEBEVENT_DB_MAX_CONNS", "WEBEVENT_AUTO_MIGRATE", "WEBEVENT_SHUTDOWN_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestDatabaseURLRequired(t *testing.T) {
	_, err := FromLookup(lookupFrom(map[string]string{}))
	if err == nil || !strings.Contains(err.Error(), "WEBEVENT_DATABASE_URL") {
		t.Fatalf("expected missing database url error, got %v", err)
	}
}

func TestWhitespaceTreatedAsUnset(t *testing.T) {
	cfg, err := FromLookup(lookupFrom(map[string]string{
		"WEBEVENT_DATABASE_URL": "postgres://db/x",
		"WEBEVENT_HTTP_ADDR":    "   ",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("blank value should fall back to default, got %q", cfg.HTTPAddr)
	}
}

func TestAPIKeys(t *testing.T) {
	cfg, err := FromLookup(lookupFrom(map[string]string{
		"WEBEVENT_DATABASE_URL": "postgres://db/x",
		"WEBEVENT_API_KEYS":     " " + testKey + " , second-key-0123456789abcdef012345 ,",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.APIKeys) != 2 || cfg.APIKeys[0] != testKey {
		t.Fatalf("APIKeys = %q", cfg.APIKeys)
	}

	_, err = FromLookup(lookupFrom(map[string]string{
		"WEBEVENT_DATABASE_URL": "postgres://db/x",
		"WEBEVENT_API_KEYS":     "short",
	}))
	if err == nil || !strings.Contains(err.Error(), "shorter than 32") {
		t.Fatalf("expected short key error, got %v", err)
	}

	_, err = FromLookup(lookupFrom(map[string]string{
		"WEBEVENT_DATABASE_URL": "postgres://db/x",
		"WEBEVENT_API_KEYS":     "",
	}))
	if err == nil || !strings.Contains(err.Error(), "WEBEVENT_API_KEYS is required") {
		t.Fatalf("expected missing key error, got %v", err)
	}
}

func TestRetrySettings(t *testing.T) {
	cfg, err := FromLookup(lookupFrom(map[string]string{"WEBEVENT_DATABASE_URL": "postgres://db/x"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxAttempts != 15 || cfg.RetryBaseDelay != 10*time.Second || cfg.RetryMaxDelay != 6*time.Hour {
		t.Fatalf("retry defaults: %+v", cfg)
	}
	_, err = FromLookup(lookupFrom(map[string]string{
		"WEBEVENT_DATABASE_URL":     "postgres://db/x",
		"WEBEVENT_RETRY_BASE_DELAY": "1m",
		"WEBEVENT_RETRY_MAX_DELAY":  "30s",
		"WEBEVENT_MAX_ATTEMPTS":     "0",
	}))
	if err == nil || !strings.Contains(err.Error(), "WEBEVENT_RETRY_MAX_DELAY") || !strings.Contains(err.Error(), "WEBEVENT_MAX_ATTEMPTS") {
		t.Fatalf("expected retry validation errors, got %v", err)
	}
}
