package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/GarineSaiAjay/webevent/internal/id"
)

// Delivery statuses.
const (
	DeliveryPending    = "pending"    // waiting for its next attempt
	DeliveryDelivering = "delivering" // claimed by a worker (leased)
	DeliverySucceeded  = "succeeded"  // receiver accepted it
	DeliveryDead       = "dead"       // retries exhausted or permanent failure; replayable
	DeliveryCancelled  = "cancelled"  // endpoint deleted before delivery finished
)

// ErrNotReplayable is returned when a delivery is not in a state that can be
// replayed (it is still pending or was cancelled, or its endpoint is gone).
var ErrNotReplayable = errors.New("delivery cannot be replayed in its current state")

// Delivery is the state of one event being sent to one endpoint.
type Delivery struct {
	ID             string     `json:"id"`
	EventID        string     `json:"event_id"`
	EndpointID     string     `json:"endpoint_id"`
	Status         string     `json:"status"`
	AttemptCount   int        `json:"attempt_count"`
	ReplayCount    int        `json:"replay_count"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	LastAttemptAt  *time.Time `json:"last_attempt_at,omitempty"`
	LastStatusCode *int       `json:"last_status_code,omitempty"`
	LastError      *string    `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
}

// Attempt is one HTTP request made for a delivery.
type Attempt struct {
	ID            string    `json:"id"`
	DeliveryID    string    `json:"delivery_id"`
	AttemptNumber int       `json:"attempt_number"`
	StatusCode    *int      `json:"status_code,omitempty"`
	Error         *string   `json:"error,omitempty"`
	ResponseBody  *string   `json:"response_body,omitempty"`
	DurationMS    int       `json:"duration_ms"`
	CreatedAt     time.Time `json:"created_at"`
}

const deliveryColumns = `d.id, d.event_id, d.endpoint_id, d.status, d.attempt_count, d.replay_count, d.next_attempt_at,
	d.last_attempt_at, d.last_status_code, d.last_error, d.created_at, d.updated_at, d.completed_at`

func scanDelivery(row pgx.CollectableRow) (Delivery, error) {
	var d Delivery
	err := row.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptCount, &d.ReplayCount, &d.NextAttemptAt,
		&d.LastAttemptAt, &d.LastStatusCode, &d.LastError, &d.CreatedAt, &d.UpdatedAt, &d.CompletedAt)
	return d, err
}

const attemptColumns = `id, delivery_id, attempt_number, status_code, error, response_body, duration_ms, created_at`

func scanAttempt(row pgx.CollectableRow) (Attempt, error) {
	var a Attempt
	err := row.Scan(&a.ID, &a.DeliveryID, &a.AttemptNumber, &a.StatusCode, &a.Error, &a.ResponseBody, &a.DurationMS, &a.CreatedAt)
	return a, err
}

// DeliveryFilter narrows ListDeliveries. Empty fields are ignored.
type DeliveryFilter struct {
	EventID    string
	EndpointID string
	Status     string
}

// GetDelivery returns one delivery.
func (s *Store) GetDelivery(ctx context.Context, deliveryID string) (Delivery, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+deliveryColumns+` FROM deliveries d WHERE d.id = $1`, deliveryID)
	if err != nil {
		return Delivery{}, err
	}
	d, err := pgx.CollectExactlyOneRow(rows, scanDelivery)
	return d, notFoundIfNoRows(err)
}

// ListDeliveries returns deliveries newest first.
func (s *Store) ListDeliveries(ctx context.Context, f DeliveryFilter, p Page) ([]Delivery, string, error) {
	p = p.normalize()
	rows, err := s.pool.Query(ctx, `
		SELECT `+deliveryColumns+` FROM deliveries d
		WHERE ($1 = '' OR d.event_id = $1)
		  AND ($2 = '' OR d.endpoint_id = $2)
		  AND ($3 = '' OR d.status = $3)
		  AND ($4 = '' OR d.id < $4)
		ORDER BY d.id DESC
		LIMIT $5`, f.EventID, f.EndpointID, f.Status, p.Cursor, p.Limit+1)
	if err != nil {
		return nil, "", err
	}
	list, err := pgx.CollectRows(rows, scanDelivery)
	if err != nil {
		return nil, "", err
	}
	list, next := trimPage(list, p.Limit, func(d Delivery) string { return d.ID })
	return list, next, nil
}

// ListAttempts returns every attempt of one delivery, oldest first.
func (s *Store) ListAttempts(ctx context.Context, deliveryID string) ([]Attempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+attemptColumns+` FROM delivery_attempts
		WHERE delivery_id = $1 ORDER BY attempt_number`, deliveryID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanAttempt)
}

// Job is everything a worker needs to perform one delivery attempt.
type Job struct {
	DeliveryID string
	// AttemptNumber is the 1-based number of the attempt about to be made,
	// counted over the whole life of the delivery (used for the attempt log).
	AttemptNumber int
	// RetryAttempt is the 1-based number within the current retry budget.
	// It restarts at 1 after a replay; the retry policy uses this one.
	RetryAttempt   int
	EventID        string
	EventType      string
	Payload        json.RawMessage
	EventCreatedAt time.Time
	EndpointID     string
	URL            string
	// LeaseToken proves this worker still owns the delivery when it records
	// the result (see RecordAttempt).
	LeaseToken string
}

// AttemptResult is what happened when a worker sent a job.
type AttemptResult struct {
	StatusCode   int    // 0 if no HTTP response was received
	Error        string // transport error or description of a non-2xx response
	ResponseBody string // truncated
	Duration     time.Duration
	// RetryAfter is the receiver's Retry-After hint, if any.
	RetryAfter time.Duration
	// Permanent marks failures that retrying cannot fix (e.g. a URL that
	// cannot be turned into a request).
	Permanent bool
}

// Outcome is what an attempt means for the delivery.
type Outcome int

// Possible outcomes of an attempt.
const (
	OutcomeSucceeded Outcome = iota + 1 // done, receiver accepted it
	OutcomeRetry                        // failed, try again later
	OutcomeDead                         // failed for good: dead-letter it
)

// AttemptRecord is the input to RecordAttempt.
type AttemptRecord struct {
	Job     Job
	Result  AttemptResult
	Outcome Outcome
	RetryIn time.Duration // for OutcomeRetry
	// DisableEndpointAfter disables the endpoint once this many deliveries
	// in a row have gone dead. 0 turns auto-disabling off.
	DisableEndpointAfter int
}

// RecordResult reports side effects of RecordAttempt.
type RecordResult struct {
	// Applied is false when we no longer own the delivery: our lease
	// expired and the reaper (or another worker) took over, or it was
	// cancelled because its endpoint was deleted while we were sending.
	// The attempt is still logged.
	Applied bool
	// EndpointDisabled is true when this attempt tipped the endpoint over
	// the auto-disable threshold.
	EndpointDisabled bool
}

// RecordAttempt appends to the attempt log and moves the delivery to the
// state the outcome dictates, in one transaction. It then updates the
// endpoint's health counter in a second, separate transaction.
//
// For OutcomeRetry the next attempt is scheduled at now()+RetryIn using the
// DATABASE clock. Every scheduling decision uses the same clock, so skew
// between application servers can never make a retry fire early or late.
func (s *Store) RecordAttempt(ctx context.Context, rec AttemptRecord) (RecordResult, error) {
	var status string
	switch rec.Outcome {
	case OutcomeSucceeded:
		status = DeliverySucceeded
	case OutcomeRetry:
		status = DeliveryPending
	case OutcomeDead:
		status = DeliveryDead
	default:
		return RecordResult{}, fmt.Errorf("record attempt: unknown outcome %d", rec.Outcome)
	}
	job, res := rec.Job, rec.Result
	var out RecordResult

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO delivery_attempts (id, delivery_id, attempt_number, status_code, error, response_body, duration_ms)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			id.New(id.PrefixAttempt), job.DeliveryID, job.AttemptNumber,
			nullIfZero(res.StatusCode), nullIfEmpty(res.Error), nullIfEmpty(res.ResponseBody),
			res.Duration.Milliseconds()); err != nil {
			return err
		}
		// Fencing: only the holder of the current lease may change the
		// delivery. If our lease expired and someone else claimed it, or it
		// was cancelled, this matches zero rows and we change nothing.
		tag, err := tx.Exec(ctx, `
			UPDATE deliveries SET
				status           = $2,
				locked_by        = NULL,
				lease_token      = NULL,
				locked_until     = NULL,
				last_attempt_at  = now(),
				last_status_code = $4,
				last_error       = $5,
				next_attempt_at  = CASE WHEN $2 = 'pending'
				                        THEN now() + make_interval(secs => $6)
				                        ELSE next_attempt_at END,
				completed_at     = CASE WHEN $2 = 'pending' THEN NULL ELSE now() END
			WHERE id = $1 AND status = 'delivering' AND lease_token = $3`,
			job.DeliveryID, status, job.LeaseToken, nullIfZero(res.StatusCode), nullIfEmpty(res.Error),
			rec.RetryIn.Seconds())
		if err != nil {
			return err
		}
		out.Applied = tag.RowsAffected() == 1
		return nil
	})
	if err != nil || !out.Applied {
		return out, err
	}

	// Endpoint health is bookkeeping, not part of the delivery's state, so
	// it lives in its own transaction. Keeping the two apart means no
	// transaction ever locks a delivery and then an endpoint, which avoids
	// lock-order deadlocks with DeleteEndpoint (endpoint, then deliveries).
	switch rec.Outcome {
	case OutcomeSucceeded:
		_, err = s.pool.Exec(ctx, `
			UPDATE endpoints SET consecutive_failures = 0
			WHERE id = $1 AND consecutive_failures <> 0`, job.EndpointID)
	case OutcomeDead:
		out.EndpointDisabled, err = s.recordEndpointFailure(ctx, job.EndpointID, rec.DisableEndpointAfter)
	}
	return out, err
}

// recordEndpointFailure bumps the failure counter and disables the endpoint
// when it reaches threshold. It reports whether THIS call disabled it.
func (s *Store) recordEndpointFailure(ctx context.Context, endpointID string, threshold int) (bool, error) {
	var disabledNow bool
	err := s.pool.QueryRow(ctx, `
		WITH old AS (
			SELECT id, status FROM endpoints WHERE id = $1 FOR UPDATE
		)
		UPDATE endpoints e SET
			consecutive_failures = e.consecutive_failures + 1,
			status = CASE WHEN $2 > 0 AND old.status = 'active' AND e.consecutive_failures + 1 >= $2
			              THEN 'disabled' ELSE e.status END,
			disabled_reason = CASE WHEN $2 > 0 AND old.status = 'active' AND e.consecutive_failures + 1 >= $2
			              THEN format('disabled automatically after %s consecutive dead deliveries', e.consecutive_failures + 1)
			              ELSE e.disabled_reason END
		FROM old
		WHERE e.id = old.id
		RETURNING old.status = 'active' AND e.status = 'disabled'`, endpointID, threshold).Scan(&disabledNow)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return disabledNow, err
}

// ReplayDelivery puts a dead (or succeeded) delivery back in the queue with
// a fresh retry budget. Its attempt history is kept.
func (s *Store) ReplayDelivery(ctx context.Context, deliveryID string) (Delivery, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE deliveries d SET
			status          = 'pending',
			next_attempt_at = now(),
			attempt_offset  = d.attempt_count,
			replay_count    = d.replay_count + 1,
			completed_at    = NULL
		FROM endpoints ep
		WHERE d.id = $1
		  AND ep.id = d.endpoint_id
		  AND ep.deleted_at IS NULL
		  AND d.status IN ('dead', 'succeeded')
		RETURNING `+deliveryColumns, deliveryID)
	if err != nil {
		return Delivery{}, err
	}
	d, err := pgx.CollectExactlyOneRow(rows, scanDelivery)
	if errors.Is(err, pgx.ErrNoRows) {
		// Distinguish "no such delivery" from "wrong state".
		if _, getErr := s.GetDelivery(ctx, deliveryID); getErr != nil {
			return Delivery{}, getErr
		}
		return Delivery{}, ErrNotReplayable
	}
	if err == nil {
		s.notifyWork(ctx)
	}
	return d, err
}

// ReplayDeadForEndpoint re-queues every dead delivery of an endpoint whose
// completed_at falls in [since, until). It works in batches of batchSize so
// replaying a million dead letters never holds one giant transaction.
func (s *Store) ReplayDeadForEndpoint(ctx context.Context, endpointID string, since, until time.Time, batchSize int) (int, error) {
	if _, err := s.GetEndpoint(ctx, endpointID); err != nil {
		return 0, err
	}
	total := 0
	for {
		tag, err := s.pool.Exec(ctx, `
			UPDATE deliveries SET
				status          = 'pending',
				next_attempt_at = now(),
				attempt_offset  = attempt_count,
				replay_count    = replay_count + 1,
				completed_at    = NULL
			WHERE id IN (
				SELECT id FROM deliveries
				WHERE endpoint_id = $1 AND status = 'dead'
				  AND completed_at >= $2 AND completed_at < $3
				ORDER BY completed_at
				LIMIT $4
				FOR UPDATE SKIP LOCKED
			)`, endpointID, since, until, batchSize)
		if err != nil {
			return total, err
		}
		n := int(tag.RowsAffected())
		total += n
		if n > 0 {
			s.notifyWork(ctx)
		}
		if n < batchSize {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}

func nullIfZero(n int) *int {
	if n == 0 {
		return nil
	}
	return &n
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
