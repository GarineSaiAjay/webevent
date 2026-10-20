// Package db owns the PostgreSQL connection pool and schema migrations.
package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a pool and waits (with backoff) until the database answers.
// Containers often start before Postgres is ready, so we retry for up to
// the lifetime of ctx instead of crashing on the first refused connection.
func Connect(ctx context.Context, url string, maxConns int32, log *slog.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 1
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.MaxConnLifetime = time.Hour
	cfg.HealthCheckPeriod = 30 * time.Second
	// Return every timestamp in UTC regardless of the server's default zone.
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	backoff := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = pool.Ping(pingCtx)
		cancel()
		if err == nil {
			return pool, nil
		}
		log.Warn("database not ready", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, fmt.Errorf("database unreachable: %w", err)
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}
