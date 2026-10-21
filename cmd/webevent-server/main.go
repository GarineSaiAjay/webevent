// Command webevent-server runs the webhook delivery service.
//
// Usage:
//
//	webevent-server [serve]   run the API and delivery workers (default)
//	webevent-server migrate      apply database migrations and exit
//	webevent-server healthcheck  exit 0 if the local server is ready (for Docker HEALTHCHECK)
//	webevent-server version      print the version and exit
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/api"
	"github.com/GarineSaiAjay/webevent/internal/config"
	"github.com/GarineSaiAjay/webevent/internal/db"
	"github.com/GarineSaiAjay/webevent/internal/delivery"
	"github.com/GarineSaiAjay/webevent/internal/logging"
	"github.com/GarineSaiAjay/webevent/internal/observability"
	"github.com/GarineSaiAjay/webevent/internal/retention"
	"github.com/GarineSaiAjay/webevent/internal/secrets"
	"github.com/GarineSaiAjay/webevent/internal/store"
)

// version is overridden at build time: -ldflags "-X main.version=v1.2.3".
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	switch cmd {
	case "version":
		fmt.Println(version)
		return nil
	case "healthcheck":
		return healthcheck()
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("invalid configuration:\n%w", err)
	}
	log := logging.New(os.Stdout, cfg.LogFormat, cfg.LogLevel).With("service", "webevent", "version", version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "serve":
		return serve(ctx, cfg, log)
	case "migrate":
		return migrate(ctx, cfg, log)
	default:
		return fmt.Errorf("unknown command %q (want serve, migrate or version)", cmd)
	}
}

// healthcheck probes /readyz on the local listener. Distroless images have no
// curl or wget, so the binary checks itself. It deliberately reads only
// WEBEVENT_HTTP_ADDR, not the full (secret-bearing) configuration.
func healthcheck() error {
	addr := os.Getenv("WEBEVENT_HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, port)+"/readyz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("not ready: HTTP %d", resp.StatusCode)
	}
	return nil
}

func migrate(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	pool, err := db.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns, log)
	if err != nil {
		return err
	}
	defer pool.Close()
	migs, err := db.Migrations()
	if err != nil {
		return err
	}
	return db.Migrate(ctx, pool, migs, log)
}

func serve(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	pool, err := db.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns, log)
	if err != nil {
		return err
	}
	defer pool.Close()

	if cfg.AutoMigrate {
		migs, err := db.Migrations()
		if err != nil {
			return err
		}
		if err := db.Migrate(ctx, pool, migs, log); err != nil {
			return err
		}
	}

	box, err := secrets.NewBox(cfg.SecretKey)
	if err != nil {
		return fmt.Errorf("WEBEVENT_SECRET_KEY: %w", err)
	}
	if !box.Encrypting() {
		log.Warn("WEBEVENT_SECRET_KEY is not set: endpoint signing secrets are stored unencrypted")
	}
	if cfg.AllowPrivateNetworks {
		log.Warn("WEBEVENT_ALLOW_PRIVATE_NETWORKS is on: SSRF protection is disabled (development only)")
	}
	st := store.New(pool, store.WithSecretBox(box))

	// Observability: metrics are always collected; tracing exports only when
	// OTEL_EXPORTER_OTLP_ENDPOINT is set.
	metrics := observability.NewMetrics()
	metrics.RegisterQueueCollector(pool, 10*time.Second, log)
	shutdownTracing, err := observability.SetupTracing(ctx, "webevent", version)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(flushCtx)
	}()
	if cfg.AdminAddr != "off" {
		admin := &http.Server{Addr: cfg.AdminAddr, Handler: observability.AdminHandler(metrics), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			log.Info("admin server listening (metrics, pprof)", "addr", cfg.AdminAddr)
			if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("admin server", "err", err)
			}
		}()
		defer func() { _ = admin.Close() }()
	}

	policy := delivery.RetryPolicy{MaxAttempts: cfg.MaxAttempts, BaseDelay: cfg.RetryBaseDelay, MaxDelay: cfg.RetryMaxDelay}

	// Delivery workers run until ctx is cancelled (SIGINT/SIGTERM), then
	// drain in-flight deliveries. API-only replicas set WEBEVENT_WORKER_ENABLED=false.
	workerDone := make(chan struct{})
	if cfg.WorkerEnabled {
		workers, err := delivery.NewPool(delivery.PoolOptions{
			Store: st,
			Sender: delivery.NewSender(delivery.SenderOptions{
				Timeout:              cfg.DeliveryTimeout,
				UserAgent:            "webevent/" + version,
				AllowPrivateNetworks: cfg.AllowPrivateNetworks,
			}),
			Policy:               policy,
			Logger:               log.With("component", "delivery"),
			Concurrency:          cfg.WorkerConcurrency,
			PollInterval:         cfg.WorkerPollInterval,
			LeaseDuration:        cfg.LeaseDuration,
			ReapInterval:         cfg.ReapInterval,
			DisableEndpointAfter: cfg.EndpointDisableAfter,
			Wake:                 delivery.Listen(ctx, pool, log.With("component", "listener")),
			Metrics:              metrics,
		})
		if err != nil {
			return err
		}
		go func() {
			defer close(workerDone)
			workers.Run(ctx)
		}()
	} else {
		close(workerDone)
	}

	if cfg.Retention > 0 {
		go retention.Run(ctx, retention.Options{
			Pool: pool, Logger: log.With("component", "retention"),
			Keep: cfg.Retention, Interval: 10 * time.Minute, BatchSize: 1000,
		})
	}

	draining := &atomic.Bool{}
	handler := api.New(api.Options{
		Logger:               log,
		Store:                st,
		APIKeys:              cfg.APIKeys,
		Version:              version,
		AllowPrivateNetworks: cfg.AllowPrivateNetworks,
		Metrics:              metrics,
		Draining:             draining,
		RetryPolicy:          policy,
	})
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	// Report not-ready and keep serving for a moment so the load balancer
	// (e.g. a Kubernetes Service) stops routing new requests to us first.
	draining.Store(true)
	if cfg.ShutdownDelay > 0 {
		log.Info("draining before shutdown", "delay", cfg.ShutdownDelay)
		time.Sleep(cfg.ShutdownDelay)
	}
	log.Info("shutting down", "timeout", cfg.ShutdownTimeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http shutdown: %w", err)
	}
	select {
	case <-workerDone:
	case <-shutdownCtx.Done():
		return errors.New("timed out waiting for in-flight deliveries to finish")
	}
	log.Info("shutdown complete")
	return nil
}
