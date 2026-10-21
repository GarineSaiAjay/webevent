// Command webevent-server runs the webhook delivery service.
//
// Usage:
//
//	webevent-server [serve]   run the API and delivery workers (default)
//	webevent-server migrate   apply database migrations and exit
//	webevent-server version   print the version and exit
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
	"syscall"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/api"
	"github.com/GarineSaiAjay/webevent/internal/config"
	"github.com/GarineSaiAjay/webevent/internal/db"
	"github.com/GarineSaiAjay/webevent/internal/delivery"
	"github.com/GarineSaiAjay/webevent/internal/logging"
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
	if cmd == "version" {
		fmt.Println(version)
		return nil
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
			Policy:               delivery.RetryPolicy{MaxAttempts: cfg.MaxAttempts, BaseDelay: cfg.RetryBaseDelay, MaxDelay: cfg.RetryMaxDelay},
			Logger:               log.With("component", "delivery"),
			Concurrency:          cfg.WorkerConcurrency,
			PollInterval:         cfg.WorkerPollInterval,
			LeaseDuration:        cfg.LeaseDuration,
			ReapInterval:         cfg.ReapInterval,
			DisableEndpointAfter: cfg.EndpointDisableAfter,
			Wake:                 delivery.Listen(ctx, pool, log.With("component", "listener")),
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

	handler := api.New(api.Options{
		Logger:               log,
		Store:                st,
		APIKeys:              cfg.APIKeys,
		Version:              version,
		AllowPrivateNetworks: cfg.AllowPrivateNetworks,
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
