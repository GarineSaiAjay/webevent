package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/GarineSaiAjay/webevent/internal/id"
)

// NotifyChannel is the Postgres LISTEN/NOTIFY channel used to wake workers
// when new work is enqueued.
const NotifyChannel = "webevent_work"

// ClaimJobs atomically claims up to limit due deliveries for workerID and
// leases them for leaseFor.
//
// This is the heart of safe concurrency. FOR UPDATE SKIP LOCKED makes
// concurrent claimers (goroutines, processes, machines) skip rows another
// claimer has locked instead of waiting for them, so:
//
//   - no two workers ever receive the same delivery, and
//   - workers never block each other; they just take different rows.
//
// The claimed rows move to 'delivering' with a random lease token. The token
// is the fencing token checked by RecordAttempt.
//
// attempt_count is incremented HERE, at claim time, not when the attempt
// finishes. If a worker crashes mid-attempt, the attempt still counts, so a
// payload that crashes workers ("poison message") eventually exhausts its
// retry budget instead of looping forever.
func (s *Store) ClaimJobs(ctx context.Context, workerID string, limit int, leaseFor time.Duration) ([]Job, error) {
	token := newLeaseToken()
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT d.id
			FROM deliveries d
			JOIN endpoints ep ON ep.id = d.endpoint_id
			WHERE d.status = 'pending'
			  AND d.next_attempt_at <= now()
			  AND ep.status = 'active'
			  AND ep.deleted_at IS NULL
			ORDER BY d.next_attempt_at
			LIMIT $1
			FOR UPDATE OF d SKIP LOCKED
		)
		UPDATE deliveries d SET
			status        = 'delivering',
			locked_by     = $2,
			lease_token   = $3,
			locked_until  = now() + make_interval(secs => $4),
			attempt_count = d.attempt_count + 1
		FROM due, events e, endpoints ep
		WHERE d.id = due.id AND e.id = d.event_id AND ep.id = d.endpoint_id
		RETURNING d.id, d.attempt_count, d.attempt_count - d.attempt_offset,
		          e.id, e.event_type, e.payload, e.created_at, ep.id, ep.url, d.lease_token,
		          ep.secret,
		          CASE WHEN ep.previous_secret_expires_at > now() THEN ep.previous_secret END`,
		limit, workerID, token, leaseFor.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Job, error) {
		var j Job
		var current string
		var previous *string
		err := row.Scan(&j.DeliveryID, &j.AttemptNumber, &j.RetryAttempt, &j.EventID, &j.EventType, &j.Payload,
			&j.EventCreatedAt, &j.EndpointID, &j.URL, &j.LeaseToken, &current, &previous)
		if err != nil {
			return j, err
		}
		j.Secrets, j.SecretErr = s.openSecrets(current, previous)
		return j, nil
	})
}

func (s *Store) openSecrets(current string, previous *string) ([]string, error) {
	cur, err := s.box.Open(current)
	if err != nil {
		return nil, err
	}
	out := []string{cur}
	if previous != nil {
		prev, err := s.box.Open(*previous)
		if err != nil {
			return nil, err
		}
		out = append(out, prev)
	}
	return out, nil
}

// ReapExpiredLeases returns deliveries whose lease ran out (their worker
// crashed, was killed, or stalled) to the queue, retrying after retryIn. It
// logs a synthetic attempt for each so the history shows what happened.
// It reaps at most limit rows per call and returns how many it reaped.
func (s *Store) ReapExpiredLeases(ctx context.Context, retryIn time.Duration, limit int) (int, error) {
	var reaped int
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			UPDATE deliveries SET
				status          = 'pending',
				locked_by       = NULL,
				lease_token     = NULL,
				locked_until    = NULL,
				last_attempt_at = now(),
				last_status_code = NULL,
				last_error      = 'lease expired: worker ' || locked_by || ' stopped responding',
				next_attempt_at = now() + make_interval(secs => $1)
			WHERE id IN (
				SELECT id FROM deliveries
				WHERE status = 'delivering' AND locked_until < now()
				ORDER BY locked_until
				LIMIT $2
				FOR UPDATE SKIP LOCKED
			)
			RETURNING id, attempt_count, last_error`, retryIn.Seconds(), limit)
		if err != nil {
			return err
		}
		type expired struct {
			id      string
			attempt int
			reason  string
		}
		list, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (expired, error) {
			var e expired
			err := row.Scan(&e.id, &e.attempt, &e.reason)
			return e, err
		})
		if err != nil {
			return err
		}
		for _, e := range list {
			if _, err := tx.Exec(ctx, `
				INSERT INTO delivery_attempts (id, delivery_id, attempt_number, error, duration_ms)
				VALUES ($1, $2, $3, $4, 0)`, id.New(id.PrefixAttempt), e.id, e.attempt, e.reason); err != nil {
				return err
			}
		}
		reaped = len(list)
		return nil
	})
	if err == nil && reaped > 0 {
		s.notifyWork(ctx)
	}
	return reaped, err
}

// notifyWork wakes idle workers. It is best effort: workers also poll, so a
// lost notification only delays work by one poll interval.
func (s *Store) notifyWork(ctx context.Context) {
	_, _ = s.pool.Exec(ctx, `SELECT pg_notify($1, '')`, NotifyChannel)
}

func newLeaseToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
