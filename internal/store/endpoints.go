package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/GarineSaiAjay/webevent/internal/id"
)

// Endpoint statuses.
const (
	EndpointActive   = "active"   // receives deliveries
	EndpointPaused   = "paused"   // paused by the user; deliveries queue up
	EndpointDisabled = "disabled" // disabled by the system after repeated failures
)

// Endpoint is a URL subscribed to a set of event types.
type Endpoint struct {
	ID                  string    `json:"id"`
	URL                 string    `json:"url"`
	Description         string    `json:"description"`
	EventTypes          []string  `json:"event_types"`
	Status              string    `json:"status"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	DisabledReason      *string   `json:"disabled_reason,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// NewEndpoint is the input to CreateEndpoint. It must already be validated.
type NewEndpoint struct {
	URL         string
	Description string
	EventTypes  []string
}

// EndpointPatch holds optional changes; nil fields are left untouched.
type EndpointPatch struct {
	URL         *string
	Description *string
	EventTypes  *[]string
	Status      *string
}

const endpointColumns = `id, url, description, event_types, status, consecutive_failures, disabled_reason, created_at, updated_at`

func scanEndpoint(row pgx.CollectableRow) (Endpoint, error) {
	var e Endpoint
	err := row.Scan(&e.ID, &e.URL, &e.Description, &e.EventTypes, &e.Status, &e.ConsecutiveFailures,
		&e.DisabledReason, &e.CreatedAt, &e.UpdatedAt)
	return e, err
}

// CreateEndpoint inserts a new endpoint.
func (s *Store) CreateEndpoint(ctx context.Context, in NewEndpoint) (Endpoint, error) {
	rows, err := s.pool.Query(ctx, `
		INSERT INTO endpoints (id, url, description, event_types)
		VALUES ($1, $2, $3, $4)
		RETURNING `+endpointColumns,
		id.New(id.PrefixEndpoint), in.URL, in.Description, in.EventTypes)
	if err != nil {
		return Endpoint{}, err
	}
	return pgx.CollectExactlyOneRow(rows, scanEndpoint)
}

// GetEndpoint returns a live (not deleted) endpoint.
func (s *Store) GetEndpoint(ctx context.Context, endpointID string) (Endpoint, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+endpointColumns+` FROM endpoints
		WHERE id = $1 AND deleted_at IS NULL`, endpointID)
	if err != nil {
		return Endpoint{}, err
	}
	e, err := pgx.CollectExactlyOneRow(rows, scanEndpoint)
	return e, notFoundIfNoRows(err)
}

// ListEndpoints returns live endpoints, newest first.
func (s *Store) ListEndpoints(ctx context.Context, p Page) ([]Endpoint, string, error) {
	p = p.normalize()
	rows, err := s.pool.Query(ctx, `
		SELECT `+endpointColumns+` FROM endpoints
		WHERE deleted_at IS NULL AND ($1 = '' OR id < $1)
		ORDER BY id DESC
		LIMIT $2`, p.Cursor, p.Limit+1)
	if err != nil {
		return nil, "", err
	}
	list, err := pgx.CollectRows(rows, scanEndpoint)
	if err != nil {
		return nil, "", err
	}
	list, next := trimPage(list, p.Limit, func(e Endpoint) string { return e.ID })
	return list, next, nil
}

// UpdateEndpoint applies a partial update. COALESCE keeps columns whose
// patch field is NULL. Any explicit status change (e.g. re-enabling a
// disabled endpoint) clears the failure counter and disabled reason.
func (s *Store) UpdateEndpoint(ctx context.Context, endpointID string, p EndpointPatch) (Endpoint, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE endpoints SET
			url                  = COALESCE($2, url),
			description          = COALESCE($3, description),
			event_types          = COALESCE($4, event_types),
			status               = COALESCE($5, status),
			consecutive_failures = CASE WHEN $5::text IS NULL THEN consecutive_failures ELSE 0 END,
			disabled_reason      = CASE WHEN $5::text IS NULL THEN disabled_reason ELSE NULL END
		WHERE id = $1 AND deleted_at IS NULL
		RETURNING `+endpointColumns,
		endpointID, p.URL, p.Description, p.EventTypes, p.Status)
	if err != nil {
		return Endpoint{}, err
	}
	e, err := pgx.CollectExactlyOneRow(rows, scanEndpoint)
	return e, notFoundIfNoRows(err)
}

// DeleteEndpoint soft-deletes an endpoint and cancels its pending deliveries
// in the same transaction.
//
// Ordering matters: we lock the endpoint row first. CreateEvent holds a FOR
// SHARE lock on endpoints it fans out to, so this UPDATE waits for any
// in-flight fan-out to commit. The second statement then runs with a fresh
// snapshot (READ COMMITTED) and sees, and cancels, those new deliveries too.
func (s *Store) DeleteEndpoint(ctx context.Context, endpointID string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE endpoints SET deleted_at = now()
			WHERE id = $1 AND deleted_at IS NULL`, endpointID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		_, err = tx.Exec(ctx, `
			UPDATE deliveries SET
				status       = 'cancelled',
				last_error   = 'endpoint deleted',
				completed_at = now()
			WHERE endpoint_id = $1 AND status = 'pending'`, endpointID)
		return err
	})
}
