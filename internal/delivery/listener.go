package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

// Listen turns Postgres NOTIFY messages on store.NotifyChannel into wake-up
// signals on the returned channel, until ctx is cancelled.
//
// It holds one dedicated connection (LISTEN is per-session). If that
// connection breaks it reconnects with backoff; meanwhile the pool's polling
// keeps work flowing, just with slightly higher latency. Signals coalesce:
// the channel has capacity 1, so a burst of 10,000 notifications becomes a
// single wake-up, and the pool then claims in batches.
func Listen(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) <-chan struct{} {
	wake := make(chan struct{}, 1)
	signal := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	go func() {
		backoff := 500 * time.Millisecond
		for ctx.Err() == nil {
			err := listenOnce(ctx, pool, signal)
			if ctx.Err() != nil {
				return
			}
			log.Warn("notification listener disconnected; retrying", "err", err, "backoff", backoff)
			// We may have missed notifications while disconnected.
			signal()
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 30*time.Second)
		}
	}()
	return wake
}

func listenOnce(ctx context.Context, pool *pgxpool.Pool, signal func()) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// The connection is in LISTEN mode; never hand it back to the pool.
	raw := conn.Hijack()
	defer func() { _ = raw.Close(context.Background()) }()

	if _, err := raw.Exec(ctx, "LISTEN "+pgx.Identifier{store.NotifyChannel}.Sanitize()); err != nil {
		return err
	}
	for {
		if _, err := raw.WaitForNotification(ctx); err != nil {
			return err
		}
		signal()
	}
}
