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
// leases them for leaseFor, while honouring every endpoint's concurrency cap
// and rate limit and sharing capacity fairly between endpoints.
//
// It runs in a single transaction, in three steps:
//
//  1. Lock up to maxCandidates active endpoints that have due work, in
//     round-robin order (least recently visited first), with FOR NO KEY
//     UPDATE SKIP LOCKED, then read their in-flight counts and token
//     buckets. Holding the endpoint row lock means no other claimer can look
//     at the same endpoint at the same time, so the per-endpoint limits hold
//     across every worker and process. NO KEY UPDATE (rather than FOR
//     UPDATE) matters: it does not conflict with the FOR KEY SHARE locks that
//     event fan-out takes, so publishers and claimers never block each other.
//  2. Decide in Go how many deliveries each endpoint may start now:
//     min(max_in_flight - in_flight, available rate-limit tokens), then share
//     the free worker slots fairly between endpoints (see allocate).
//  3. Claim that many deliveries per endpoint, with FOR UPDATE SKIP LOCKED on
//     the delivery rows, and save the new token-bucket state and the
//     round-robin cursor on the endpoint rows.
//
// Why this gives isolation: an endpoint that is slow or down can occupy at
// most max_in_flight workers; every other endpoint keeps being served.
//
// attempt_count is incremented HERE, at claim time, not when the attempt
// finishes. If a worker crashes mid-attempt, the attempt still counts, so a
// payload that crashes workers ("poison message") eventually exhausts its
// retry budget instead of looping forever.
func (s *Store) ClaimJobs(ctx context.Context, workerID string, limit int, leaseFor time.Duration) ([]Job, error) {
	if limit <= 0 {
		return nil, nil
	}
	token := newLeaseToken()
	var jobs []Job
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		jobs = jobs[:0]
		// Step 1a: lock candidate endpoints.
		rows, err := tx.Query(ctx, `
			SELECT ep.id
			FROM endpoints ep
			WHERE ep.status = 'active' AND ep.deleted_at IS NULL
			  AND EXISTS (SELECT 1 FROM deliveries d
			              WHERE d.endpoint_id = ep.id AND d.status = 'pending' AND d.next_attempt_at <= now())
			ORDER BY ep.last_claimed_at NULLS FIRST
			LIMIT $1
			FOR NO KEY UPDATE OF ep SKIP LOCKED`, maxCandidates(limit))
		if err != nil {
			return err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil || len(ids) == 0 {
			return err
		}

		// Step 1b: read their state in a NEW statement. This separation is
		// essential. Under READ COMMITTED each statement sees a snapshot
		// taken when it starts. Step 1a's snapshot may predate the commit of
		// the claimer that held one of these locks just before us, so an
		// in-flight count computed there could miss that claimer's
		// deliveries and let us exceed max_in_flight. This statement starts
		// after we hold the locks, so it sees every earlier claim.
		rows, err = tx.Query(ctx, `
			SELECT ep.id, ep.max_in_flight, ep.rate_limit_per_sec, ep.rl_tokens, ep.rl_updated_at, now(),
			       (SELECT count(*) FROM deliveries x WHERE x.endpoint_id = ep.id AND x.status = 'delivering')
			FROM endpoints ep
			WHERE ep.id = ANY($1)
			ORDER BY ep.last_claimed_at NULLS FIRST`, ids)
		if err != nil {
			return err
		}
		cands, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (candidate, error) {
			var c candidate
			err := row.Scan(&c.endpointID, &c.maxInFlight, &c.ratePerSec, &c.tokens, &c.tokensAt, &c.now, &c.inFlight)
			return c, err
		})
		if err != nil {
			return err
		}

		// Step 2: how many may each endpoint start right now?
		for i := range cands {
			cands[i].allowance = cands[i].allowanceNow()
		}
		grants := allocate(limit, cands)

		// Step 3: claim, then persist bucket state and the round-robin cursor.
		for i, c := range cands {
			claimed := 0
			if grants[i] > 0 {
				got, err := s.claimForEndpoint(ctx, tx, c.endpointID, grants[i], workerID, token, leaseFor)
				if err != nil {
					return err
				}
				jobs = append(jobs, got...)
				claimed = len(got)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE endpoints SET
					last_claimed_at = now(),
					rl_tokens       = $2,
					rl_updated_at   = now()
				WHERE id = $1`, c.endpointID, c.tokensAfter(claimed)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *Store) claimForEndpoint(ctx context.Context, tx pgx.Tx, endpointID string, n int, workerID, token string, leaseFor time.Duration) ([]Job, error) {
	rows, err := tx.Query(ctx, `
		WITH due AS (
			SELECT id FROM deliveries
			WHERE endpoint_id = $1 AND status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE deliveries d SET
			status        = 'delivering',
			locked_by     = $3,
			lease_token   = $4,
			locked_until  = now() + make_interval(secs => $5),
			attempt_count = d.attempt_count + 1
		FROM due, events e, endpoints ep
		WHERE d.id = due.id AND e.id = d.event_id AND ep.id = d.endpoint_id
		RETURNING d.id, d.attempt_count, d.attempt_count - d.attempt_offset,
		          e.id, e.event_type, e.payload, e.created_at, ep.id, ep.url, d.lease_token,
		          ep.secret,
		          CASE WHEN ep.previous_secret_expires_at > now() THEN ep.previous_secret END,
		          COALESCE(e.trace_parent, '')`,
		endpointID, n, workerID, token, leaseFor.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Job, error) {
		var j Job
		var current string
		var previous *string
		err := row.Scan(&j.DeliveryID, &j.AttemptNumber, &j.RetryAttempt, &j.EventID, &j.EventType, &j.Payload,
			&j.EventCreatedAt, &j.EndpointID, &j.URL, &j.LeaseToken, &current, &previous, &j.TraceParent)
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
