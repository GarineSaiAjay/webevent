package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func lookupFrom(m map[string]string) func(string) (string, bool) {
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
