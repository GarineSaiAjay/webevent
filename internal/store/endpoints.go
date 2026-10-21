package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/GarineSaiAjay/webevent/internal/id"
	"github.com/GarineSaiAjay/webevent/pkg/webhook"
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
	Kind                string    `json:"kind"`               // "http" or "listen"
	RateLimitPerSec     *int      `json:"rate_limit_per_sec"` // null = unlimited
	MaxInFlight         int       `json:"max_in_flight"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	DisabledReason      *string   `json:"disabled_reason,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	// Secret is only filled in by calls that deliberately reveal it
	// (create, get-secret, rotate); it is never part of ordinary reads.
	Secret string `json:"secret,omitempty"`
}

// DefaultMaxInFlight is used when an endpoint does not specify one.
const DefaultMaxInFlight = 10

// NewEndpoint is the input to CreateEndpoint. It must already be validated.
type NewEndpoint struct {
	URL             string
	Description     string
	EventTypes      []string
	RateLimitPerSec *int // nil or 0 = unlimited
	MaxInFlight     int  // 0 = DefaultMaxInFlight
}

// EndpointPatch holds optional changes; nil fields are left untouched.
type EndpointPatch struct {
	URL             *string
	Description     *string
	EventTypes      *[]string
	Status          *string
	RateLimitPerSec *int // 0 removes the limit
	MaxInFlight     *int
}

const endpointColumns = `id, url, description, event_types, status, kind, rate_limit_per_sec, max_in_flight,
	consecutive_failures, disabled_reason, created_at, updated_at`

func scanEndpoint(row pgx.CollectableRow) (Endpoint, error) {
	var e Endpoint
	err := row.Scan(&e.ID, &e.URL, &e.Description, &e.EventTypes, &e.Status, &e.Kind, &e.RateLimitPerSec, &e.MaxInFlight,
		&e.ConsecutiveFailures, &e.DisabledReason, &e.CreatedAt, &e.UpdatedAt)
	return e, err
}

func rateLimitValue(r *int) *int {
	if r == nil || *r == 0 {
		return nil
	}
	return r
}

// CreateEndpoint inserts a new endpoint with a freshly generated signing
// secret. The returned Endpoint includes the secret.
func (s *Store) CreateEndpoint(ctx context.Context, in NewEndpoint) (Endpoint, error) {
	secret := webhook.NewSecret()
	sealed, err := s.box.Seal(secret)
	if err != nil {
		return Endpoint{}, err
	}
	maxInFlight := in.MaxInFlight
	if maxInFlight == 0 {
		maxInFlight = DefaultMaxInFlight
	}
	rows, err := s.pool.Query(ctx, `
		INSERT INTO endpoints (id, url, description, event_types, secret, rate_limit_per_sec, max_in_flight)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING `+endpointColumns,
		id.New(id.PrefixEndpoint), in.URL, in.Description, in.EventTypes, sealed, rateLimitValue(in.RateLimitPerSec), maxInFlight)
	if err != nil {
		return Endpoint{}, err
	}
	ep, err := pgx.CollectExactlyOneRow(rows, scanEndpoint)
	ep.Secret = secret
	return ep, err
}

// GetEndpointSecret returns the current signing secret of an endpoint.
func (s *Store) GetEndpointSecret(ctx context.Context, endpointID string) (string, error) {
	var sealed string
	err := s.pool.QueryRow(ctx, `SELECT secret FROM endpoints WHERE id = $1 AND deleted_at IS NULL`, endpointID).Scan(&sealed)
	if err != nil {
		return "", notFoundIfNoRows(err)
	}
	return s.box.Open(sealed)
}

// RotateEndpointSecret replaces the signing secret. The old secret stays
// valid (requests are signed with both) for grace, so receivers can deploy
// the new secret without dropping webhooks. grace = 0 revokes it at once.
func (s *Store) RotateEndpointSecret(ctx context.Context, endpointID string, grace time.Duration) (string, error) {
	secret := webhook.NewSecret()
	sealed, err := s.box.Seal(secret)
	if err != nil {
		return "", err
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE endpoints SET
			previous_secret            = CASE WHEN $3 > 0 THEN secret END,
			previous_secret_expires_at = CASE WHEN $3 > 0 THEN now() + make_interval(secs => $3) END,
			secret                     = $2
		WHERE id = $1 AND deleted_at IS NULL`, endpointID, sealed, grace.Seconds())
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		return "", ErrNotFound
	}
	return secret, nil
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
			disabled_reason      = CASE WHEN $5::text IS NULL THEN disabled_reason ELSE NULL END,
			rate_limit_per_sec   = CASE WHEN $6::int IS NULL THEN rate_limit_per_sec
			                            WHEN $6 = 0 THEN NULL ELSE $6 END,
			-- A new limit starts with a full bucket.
			rl_tokens            = CASE WHEN $6::int IS NULL THEN rl_tokens ELSE NULL END,
			max_in_flight        = COALESCE($7, max_in_flight)
		WHERE id = $1 AND deleted_at IS NULL
		RETURNING `+endpointColumns,
		endpointID, p.URL, p.Description, p.EventTypes, p.Status, p.RateLimitPerSec, p.MaxInFlight)
	if err != nil {
		return Endpoint{}, err
	}
	e, err := pgx.CollectExactlyOneRow(rows, scanEndpoint)
	return e, notFoundIfNoRows(err)
}

// DeleteEndpoint soft-deletes an endpoint and cancels its pending deliveries
// in the same transaction.
//
// Ordering matters: we first lock the endpoint row FOR UPDATE, the one lock
// mode that conflicts with the FOR KEY SHARE locks CreateEvent holds on the
// endpoints it fans out to. So we wait for any in-flight fan-out to commit;
// the cancelling statement then runs with a fresh snapshot (READ COMMITTED)
// and sees, and cancels, those new deliveries too. Fan-outs that start after
// our commit re-check deleted_at and skip the endpoint.
func (s *Store) DeleteEndpoint(ctx context.Context, endpointID string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var found string
		err := tx.QueryRow(ctx, `
			SELECT id FROM endpoints
			WHERE id = $1 AND deleted_at IS NULL
			FOR UPDATE`, endpointID).Scan(&found)
		if err != nil {
			return notFoundIfNoRows(err)
		}
		tag, err := tx.Exec(ctx, `
			UPDATE endpoints SET deleted_at = now()
			WHERE id = $1 AND deleted_at IS NULL`, endpointID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		// In-flight ('delivering') deliveries are cancelled too; their
		// worker's lease token no longer matches, so its result is ignored.
		_, err = tx.Exec(ctx, `
			UPDATE deliveries SET
				status       = 'cancelled',
				locked_by    = NULL,
				lease_token  = NULL,
				locked_until = NULL,
				last_error   = 'endpoint deleted',
				completed_at = now()
			WHERE endpoint_id = $1 AND status IN ('pending', 'delivering')`, endpointID)
		return err
	})
}
