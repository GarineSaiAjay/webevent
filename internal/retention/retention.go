// Package retention deletes old, fully-processed events so the database does
// not grow forever. Deleting an event cascades to its deliveries and their
// attempt logs.
package retention

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// lockID makes sure only one server instance runs the cleanup at a time.
const lockID int64 = 7_270_461_003

// Options configures the cleaner.
type Options struct {
	Pool   *pgxpool.Pool
	Logger *slog.Logger
	// Keep is how long events are kept. Events older than this are deleted
	// once none of their deliveries is still pending or in flight.
	Keep time.Duration
	// Interval between cleanup runs.
	Interval time.Duration
	// BatchSize bounds each DELETE so no single transaction holds locks
	// on (or bloats WAL with) millions of rows at once.
	BatchSize int
}

// Run deletes expired events every Interval until ctx is cancelled.
func Run(ctx context.Context, o Options) {
	t := time.NewTicker(o.Interval)
	defer t.Stop()
	for {
		if n, err := RunOnce(ctx, o); err != nil && ctx.Err() == nil {
			o.Logger.Error("retention cleanup failed", "err", err)
		} else if n > 0 {
			o.Logger.Info("retention cleanup deleted events", "count", n, "older_than", o.Keep)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// RunOnce performs one cleanup pass and returns how many events it deleted.
// If another instance holds the cleanup lock it returns (0, nil).
func RunOnce(ctx context.Context, o Options) (int, error) {
	conn, err := o.Pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&locked); err != nil {
		return 0, err
	}
	if !locked {
		return 0, nil
	}
	defer func() { _, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lockID) }()

	total := 0
	for {
		tag, err := conn.Exec(ctx, `
			DELETE FROM events WHERE id IN (
				SELECT e.id FROM events e
				WHERE e.created_at < now() - make_interval(secs => $1)
				  AND NOT EXISTS (
					SELECT 1 FROM deliveries d
					WHERE d.event_id = e.id AND d.status IN ('pending', 'delivering'))
				ORDER BY e.created_at
				LIMIT $2
			)`, o.Keep.Seconds(), o.BatchSize)
		if err != nil {
			return total, err
		}
		n := int(tag.RowsAffected())
		total += n
		if n < o.BatchSize || ctx.Err() != nil {
			return total, nil
		}
	}
}
