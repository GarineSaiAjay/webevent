package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/GarineSaiAjay/webevent/internal/id"
	"github.com/GarineSaiAjay/webevent/pkg/webhook"
)

// Endpoint kinds.
const (
	KindHTTP   = "http"   // workers POST to the URL
	KindListen = "listen" // the `webevent listen` CLI pulls deliveries
)

// CreateListenSession creates a temporary endpoint of kind "listen". It is
// served to the CLI by long-polling instead of being POSTed to by workers,
// and it is deleted automatically if not polled for ttl.
func (s *Store) CreateListenSession(ctx context.Context, eventTypes []string, ttl time.Duration) (Endpoint, error) {
	secret := webhook.NewSecret()
	sealed, err := s.box.Seal(secret)
	if err != nil {
		return Endpoint{}, err
	}
	epID := id.New(id.PrefixEndpoint)
	rows, err := s.pool.Query(ctx, `
		INSERT INTO endpoints (id, url, description, event_types, secret, kind, listen_expires_at, max_in_flight)
		VALUES ($1, 'listen://' || $1, 'webevent listen session', $2, $3, 'listen', now() + make_interval(secs => $4), 100)
		RETURNING `+endpointColumns,
		epID, eventTypes, sealed, ttl.Seconds())
	if err != nil {
		return Endpoint{}, err
	}
	ep, err := pgx.CollectExactlyOneRow(rows, scanEndpoint)
	ep.Secret = secret
	return ep, err
}

// TouchListenSession extends a session's expiry. It returns ErrNotFound if
// the session does not exist, expired, or is not a listen endpoint.
func (s *Store) TouchListenSession(ctx context.Context, sessionID string, ttl time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE endpoints SET listen_expires_at = now() + make_interval(secs => $2)
		WHERE id = $1 AND kind = 'listen' AND deleted_at IS NULL`, sessionID, ttl.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ClaimListenJobs claims up to n due deliveries of one listen session for
// the CLI, with the same leasing and fencing as worker claims.
func (s *Store) ClaimListenJobs(ctx context.Context, sessionID, workerID string, n int, leaseFor time.Duration) ([]Job, error) {
	var jobs []Job
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var found string
		if err := tx.QueryRow(ctx, `
			SELECT id FROM endpoints
			WHERE id = $1 AND kind = 'listen' AND deleted_at IS NULL AND status = 'active'
			FOR NO KEY UPDATE`, sessionID).Scan(&found); err != nil {
			return notFoundIfNoRows(err)
		}
		var err error
		jobs, err = s.claimForEndpoint(ctx, tx, sessionID, n, workerID, newLeaseToken(), leaseFor)
		return err
	})
	return jobs, err
}

// LoadLeasedJob rebuilds the Job for a delivery that the caller claimed and
// still holds the lease on, so its result can be recorded.
func (s *Store) LoadLeasedJob(ctx context.Context, endpointID, deliveryID, leaseToken string) (Job, error) {
	var j Job
	err := s.pool.QueryRow(ctx, `
		SELECT d.id, d.attempt_count, d.attempt_count - d.attempt_offset, d.event_id, d.endpoint_id, d.lease_token
		FROM deliveries d
		WHERE d.id = $1 AND d.endpoint_id = $2 AND d.status = 'delivering' AND d.lease_token = $3`,
		deliveryID, endpointID, leaseToken).Scan(&j.DeliveryID, &j.AttemptNumber, &j.RetryAttempt, &j.EventID, &j.EndpointID, &j.LeaseToken)
	return j, notFoundIfNoRows(err)
}

// ExpireListenSessions deletes listen sessions that stopped polling and
// cancels their pending deliveries. It returns how many it removed.
func (s *Store) ExpireListenSessions(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM endpoints
		WHERE kind = 'listen' AND deleted_at IS NULL AND listen_expires_at < now()
		LIMIT 100`)
	if err != nil {
		return 0, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	n := 0
	for _, epID := range ids {
		err := s.DeleteEndpoint(ctx, epID)
		switch {
		case err == nil:
			n++
		case errors.Is(err, ErrNotFound): // deleted concurrently
		default:
			return n, err
		}
	}
	return n, nil
}
