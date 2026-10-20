// Package config loads and validates the server configuration from
// environment variables. Every setting has a safe default so the server can
// boot with only WEBEVENT_DATABASE_URL set.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the complete, validated runtime configuration.
type Config struct {
	HTTPAddr        string
	DatabaseURL     string
	DBMaxConns      int32
	LogLevel        slog.Level
	LogFormat       string // "json" or "text"
	AutoMigrate     bool
	ShutdownTimeout time.Duration
	APIKeys         []string
}

// Load reads configuration from the process environment.
func Load() (Config, error) {
	return FromLookup(os.LookupEnv)
}

// FromLookup builds a Config from any lookup function. Tests pass a map-backed
// lookup so they never depend on the real environment.
func FromLookup(lookup func(string) (string, bool)) (Config, error) {
	r := reader{lookup: lookup}
	cfg := Config{
		HTTPAddr:        r.str("WEBEVENT_HTTP_ADDR", ":8080"),
		DatabaseURL:     r.str("WEBEVENT_DATABASE_URL", ""),
		DBMaxConns:      int32(r.int("WEBEVENT_DB_MAX_CONNS", 20)),
		LogLevel:        r.level("WEBEVENT_LOG_LEVEL", slog.LevelInfo),
		LogFormat:       r.str("WEBEVENT_LOG_FORMAT", "json"),
		AutoMigrate:     r.bool("WEBEVENT_AUTO_MIGRATE", true),
		ShutdownTimeout: r.duration("WEBEVENT_SHUTDOWN_TIMEOUT", 30*time.Second),
		APIKeys:         r.list("WEBEVENT_API_KEYS"),
	}
	if err := errors.Join(r.errs...); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks cross-field invariants that a single parser cannot.
func (c Config) Validate() error {
	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("WEBEVENT_DATABASE_URL is required"))
	}
	if c.DBMaxConns < 2 {
		errs = append(errs, errors.New("WEBEVENT_DB_MAX_CONNS must be at least 2"))
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		errs = append(errs, fmt.Errorf("WEBEVENT_LOG_FORMAT must be json or text, got %q", c.LogFormat))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("WEBEVENT_SHUTDOWN_TIMEOUT must be positive"))
	}
	if len(c.APIKeys) == 0 {
		errs = append(errs, errors.New("WEBEVENT_API_KEYS is required (comma-separated list of keys)"))
	}
	for i, k := range c.APIKeys {
		if len(k) < 32 {
			errs = append(errs, fmt.Errorf("WEBEVENT_API_KEYS entry %d is shorter than 32 characters", i+1))
		}
	}
	return errors.Join(errs...)
}

type reader struct {
	lookup func(string) (string, bool)
	errs   []error
}

func (r *reader) raw(key string) (string, bool) {
	v, ok := r.lookup(key)
	if !ok {
		return "", false
	}
	v = strings.TrimSpace(v)
	return v, v != ""
}

func (r *reader) str(key, def string) string {
	if v, ok := r.raw(key); ok {
		return v
	}
	return def
}

func (r *reader) int(key string, def int) int {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func (r *reader) bool(key string, def bool) bool {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not a boolean", key, v))
		return def
	}
	return b
}

func (r *reader) duration(key string, def time.Duration) time.Duration {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not a duration (e.g. 30s, 5m)", key, v))
		return def
	}
	return d
}

func (r *reader) list(key string) []string {
	v, ok := r.raw(key)
	if !ok {
		return nil
	}
	var out []string
	for _, part := range strings.Split(v, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (r *reader) level(key string, def slog.Level) slog.Level {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(v)); err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s: %q is not a log level (debug, info, warn, error)", key, v))
		return def
	}
	return l
}
