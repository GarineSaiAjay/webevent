package retention_test

import (
	"context"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/logging"
	"github.com/GarineSaiAjay/webevent/internal/retention"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

func TestRunOnceDeletesOnlyOldFinishedEvents(t *testing.T) {
	pool := testutil.NewDB(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		INSERT INTO endpoints (id, url, event_types, secret) VALUES ('ep_1', 'https://x', '{*}', 'whsec_x');
		INSERT INTO events (id, event_type, payload, request_hash, created_at) VALUES
			('evt_old_done',    'a', '{}', '\x00', now() - interval '40 days'),
			('evt_old_pending', 'a', '{}', '\x00', now() - interval '40 days'),
			('evt_new',         'a', '{}', '\x00', now() - interval '1 day');
		INSERT INTO deliveries (id, event_id, endpoint_id, status) VALUES
			('dlv_1', 'evt_old_done',    'ep_1', 'succeeded'),
			('dlv_2', 'evt_old_pending', 'ep_1', 'pending'),
			('dlv_3', 'evt_new',         'ep_1', 'succeeded');
		INSERT INTO delivery_attempts (id, delivery_id, attempt_number, duration_ms) VALUES ('att_1', 'dlv_1', 1, 5);
	`)
	if err != nil {
		t.Fatal(err)
	}
	n, err := retention.RunOnce(ctx, retention.Options{Pool: pool, Logger: logging.Discard(), Keep: 30 * 24 * time.Hour, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("deleted %d events, want 1", n)
	}
	var events, deliveries, attempts int
	_ = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM events), (SELECT count(*) FROM deliveries), (SELECT count(*) FROM delivery_attempts)`).
		Scan(&events, &deliveries, &attempts)
	if events != 2 || deliveries != 2 || attempts != 0 {
		t.Fatalf("after cleanup: events=%d deliveries=%d attempts=%d", events, deliveries, attempts)
	}
}

func TestOnlyOneInstanceCleansAtATime(t *testing.T) {
	pool := testutil.NewDB(t)
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	// Another "instance" holds the lock.
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(7270461003)"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock(7270461003)") }()

	n, err := retention.RunOnce(ctx, retention.Options{Pool: pool, Logger: logging.Discard(), Keep: time.Hour, BatchSize: 10})
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v; must skip when another instance holds the lock", n, err)
	}
}
