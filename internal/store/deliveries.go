package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/GarineSaiAjay/webevent/internal/id"
)

// Delivery statuses.
const (
	DeliveryPending   = "pending"
	DeliverySucceeded = "succeeded"
	DeliveryFailed    = "failed"
)

// Delivery is the state of one event being sent to one endpoint.
type Delivery struct {
	ID             string     `json:"id"`
	EventID        string     `json:"event_id"`
	EndpointID     string     `json:"endpoint_id"`
	Status         string     `json:"status"`
	AttemptCount   int        `json:"attempt_count"`
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

const deliveryColumns = `d.id, d.event_id, d.endpoint_id, d.status, d.attempt_count, d.next_attempt_at,
	d.last_attempt_at, d.last_status_code, d.last_error, d.created_at, d.updated_at, d.completed_at`

func scanDelivery(row pgx.CollectableRow) (Delivery, error) {
	var d Delivery
	err := row.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptCount, &d.NextAttemptAt,
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
	DeliveryID     string
	AttemptNumber  int // 1-based number of the attempt about to be made
	EventID        string
	EventType      string
	Payload        json.RawMessage
	EventCreatedAt time.Time
	EndpointID     string
	URL            string
}

// NextDueJob returns the oldest due pending delivery, or ErrNotFound.
//
// WALKING SKELETON: this is only safe with exactly one worker. Two workers
// calling it at the same time would both get the same row. Stage 6 replaces
// it with a SKIP LOCKED claim.
func (s *Store) NextDueJob(ctx context.Context) (Job, error) {
	var j Job
	err := s.pool.QueryRow(ctx, `
		SELECT d.id, d.attempt_count + 1, e.id, e.event_type, e.payload, e.created_at, ep.id, ep.url
		FROM deliveries d
		JOIN events e     ON e.id = d.event_id
		JOIN endpoints ep ON ep.id = d.endpoint_id
		WHERE d.status = 'pending'
		  AND d.next_attempt_at <= now()
		  AND ep.status = 'active'
		  AND ep.deleted_at IS NULL
		ORDER BY d.next_attempt_at, d.id
		LIMIT 1`).Scan(&j.DeliveryID, &j.AttemptNumber, &j.EventID, &j.EventType, &j.Payload,
		&j.EventCreatedAt, &j.EndpointID, &j.URL)
	return j, notFoundIfNoRows(err)
}

// AttemptResult is what happened when a worker sent a job.
type AttemptResult struct {
	StatusCode   int    // 0 if no HTTP response was received
	Error        string // transport error or description of a non-2xx response
	ResponseBody string // truncated
	Duration     time.Duration
}

// RecordAttempt appends to the attempt log and moves the delivery to its
// final state. WALKING SKELETON: there are no retries yet, so any failure is
// final.
func (s *Store) RecordAttempt(ctx context.Context, job Job, res AttemptResult, success bool) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO delivery_attempts (id, delivery_id, attempt_number, status_code, error, response_body, duration_ms)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			id.New(id.PrefixAttempt), job.DeliveryID, job.AttemptNumber,
			nullIfZero(res.StatusCode), nullIfEmpty(res.Error), nullIfEmpty(res.ResponseBody),
			res.Duration.Milliseconds()); err != nil {
			return err
		}
		status := DeliveryFailed
		if success {
			status = DeliverySucceeded
		}
		_, err := tx.Exec(ctx, `
			UPDATE deliveries SET
				status           = $2,
				attempt_count    = $3,
				last_attempt_at  = now(),
				last_status_code = $4,
				last_error       = $5,
				completed_at     = now()
			WHERE id = $1`,
			job.DeliveryID, status, job.AttemptNumber, nullIfZero(res.StatusCode), nullIfEmpty(res.Error))
		return err
	})
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
