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
	AdminAddr       string // internal-only: /metrics and /debug/pprof ("off" disables)
	DatabaseURL     string
	DBMaxConns      int32
	LogLevel        slog.Level
	LogFormat       string // "json" or "text"
	AutoMigrate     bool
	ShutdownTimeout time.Duration
	// ShutdownDelay keeps serving (while reporting not-ready) for this long
	// after SIGTERM, so load balancers stop routing to us first.
	ShutdownDelay time.Duration
	// Retention is how long events are kept (0 = forever).
	Retention time.Duration
	APIKeys   []string

	// Delivery
	WorkerEnabled      bool
	WorkerConcurrency  int
	WorkerPollInterval time.Duration
	DeliveryTimeout    time.Duration
	LeaseDuration      time.Duration
	ReapInterval       time.Duration

	// Retries
	MaxAttempts    int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	// Dead letters
	EndpointDisableAfter int

	// Security
	AllowPrivateNetworks bool
	SecretKey            string // base64 32-byte key for encrypting signing secrets at rest
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
		AdminAddr:       r.str("WEBEVENT_ADMIN_ADDR", ":9090"),
		DatabaseURL:     r.str("WEBEVENT_DATABASE_URL", ""),
		DBMaxConns:      int32(r.int("WEBEVENT_DB_MAX_CONNS", 20)),
		LogLevel:        r.level("WEBEVENT_LOG_LEVEL", slog.LevelInfo),
		LogFormat:       r.str("WEBEVENT_LOG_FORMAT", "json"),
		AutoMigrate:     r.bool("WEBEVENT_AUTO_MIGRATE", true),
		ShutdownTimeout: r.duration("WEBEVENT_SHUTDOWN_TIMEOUT", 30*time.Second),
		ShutdownDelay:   r.duration("WEBEVENT_SHUTDOWN_DELAY", 0),
		Retention:       r.duration("WEBEVENT_RETENTION", 30*24*time.Hour),
		APIKeys:         r.list("WEBEVENT_API_KEYS"),

		WorkerEnabled:      r.bool("WEBEVENT_WORKER_ENABLED", true),
		WorkerConcurrency:  r.int("WEBEVENT_WORKER_CONCURRENCY", 32),
		WorkerPollInterval: r.duration("WEBEVENT_WORKER_POLL_INTERVAL", time.Second),
		DeliveryTimeout:    r.duration("WEBEVENT_DELIVERY_TIMEOUT", 15*time.Second),
		LeaseDuration:      r.duration("WEBEVENT_LEASE_DURATION", 60*time.Second),
		ReapInterval:       r.duration("WEBEVENT_REAP_INTERVAL", 10*time.Second),

		MaxAttempts:    r.int("WEBEVENT_MAX_ATTEMPTS", 15),
		RetryBaseDelay: r.duration("WEBEVENT_RETRY_BASE_DELAY", 10*time.Second),
		RetryMaxDelay:  r.duration("WEBEVENT_RETRY_MAX_DELAY", 6*time.Hour),

		EndpointDisableAfter: r.int("WEBEVENT_ENDPOINT_DISABLE_AFTER", 10),

		AllowPrivateNetworks: r.bool("WEBEVENT_ALLOW_PRIVATE_NETWORKS", false),
		SecretKey:            r.str("WEBEVENT_SECRET_KEY", ""),
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
	if c.WorkerPollInterval < 10*time.Millisecond {
		errs = append(errs, errors.New("WEBEVENT_WORKER_POLL_INTERVAL must be at least 10ms"))
	}
	if c.DeliveryTimeout < time.Second || c.DeliveryTimeout > 5*time.Minute {
		errs = append(errs, errors.New("WEBEVENT_DELIVERY_TIMEOUT must be between 1s and 5m"))
	}
	if c.ShutdownDelay < 0 || c.ShutdownDelay > time.Minute {
		errs = append(errs, errors.New("WEBEVENT_SHUTDOWN_DELAY must be between 0 and 1m"))
	}
	if c.Retention != 0 && c.Retention < time.Hour {
		errs = append(errs, errors.New("WEBEVENT_RETENTION must be 0 (keep forever) or at least 1h"))
	}
	if c.WorkerConcurrency < 1 || c.WorkerConcurrency > 1024 {
		errs = append(errs, errors.New("WEBEVENT_WORKER_CONCURRENCY must be between 1 and 1024"))
	}
	// A lease must outlive the slowest possible attempt, or a healthy worker
	// would lose its delivery to the reaper mid-request (causing duplicates).
	if c.LeaseDuration < c.DeliveryTimeout+15*time.Second {
		errs = append(errs, fmt.Errorf("WEBEVENT_LEASE_DURATION (%v) must be at least WEBEVENT_DELIVERY_TIMEOUT + 15s", c.LeaseDuration))
	}
	if c.ReapInterval <= 0 {
		errs = append(errs, errors.New("WEBEVENT_REAP_INTERVAL must be positive"))
	}
	// Shutdown waits for in-flight deliveries, which take up to DeliveryTimeout.
	if c.ShutdownTimeout < c.DeliveryTimeout+10*time.Second {
		errs = append(errs, fmt.Errorf("WEBEVENT_SHUTDOWN_TIMEOUT (%v) must be at least WEBEVENT_DELIVERY_TIMEOUT + 10s", c.ShutdownTimeout))
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > 100 {
		errs = append(errs, errors.New("WEBEVENT_MAX_ATTEMPTS must be between 1 and 100"))
	}
	if c.RetryBaseDelay <= 0 {
		errs = append(errs, errors.New("WEBEVENT_RETRY_BASE_DELAY must be positive"))
	}
	if c.RetryMaxDelay < c.RetryBaseDelay {
		errs = append(errs, errors.New("WEBEVENT_RETRY_MAX_DELAY must be >= WEBEVENT_RETRY_BASE_DELAY"))
	}
	if c.EndpointDisableAfter < 0 {
		errs = append(errs, errors.New("WEBEVENT_ENDPOINT_DISABLE_AFTER must be >= 0 (0 disables auto-disabling)"))
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
