package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/GarineSaiAjay/webevent/internal/id"
)

// ErrIdempotencyMismatch means an Idempotency-Key was reused with a
// different request body.
var ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")

// Event is something that happened in the producer's system.
type Event struct {
	ID             string          `json:"id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// NewEvent is the input to CreateEvent.
type NewEvent struct {
	EventType      string
	Payload        json.RawMessage
	IdempotencyKey string // optional
	TraceParent    string // optional W3C traceparent of the creating request
}

// CreateEventResult reports what CreateEvent did.
type CreateEventResult struct {
	Event         Event
	DeliveryCount int
	// Replayed is true when an identical request with the same idempotency
	// key was already accepted; Event is the original and nothing was enqueued.
	Replayed bool
}

const eventColumns = `id, event_type, payload, idempotency_key, created_at`

func scanEvent(row pgx.CollectableRow) (Event, error) {
	var e Event
	err := row.Scan(&e.ID, &e.EventType, &e.Payload, &e.IdempotencyKey, &e.CreatedAt)
	return e, err
}

func requestHash(eventType string, payload json.RawMessage) []byte {
	h := sha256.New()
	h.Write([]byte(eventType))
	h.Write([]byte{0})
	// Compact so semantically identical JSON with different whitespace
	// counts as the same request.
	var buf bytes.Buffer
	if err := json.Compact(&buf, payload); err != nil {
		buf.Reset()
		buf.Write(payload)
	}
	h.Write(buf.Bytes())
	return h.Sum(nil)
}

// CreateEvent stores an event and enqueues one delivery per subscribed
// endpoint, atomically. Either the event and all its deliveries exist, or
// none of them do: there is no window where a crash loses deliveries.
func (s *Store) CreateEvent(ctx context.Context, in NewEvent) (CreateEventResult, error) {
	var res CreateEventResult
	hash := requestHash(in.EventType, in.Payload)
	var key *string
	if in.IdempotencyKey != "" {
		key = &in.IdempotencyKey
	}

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			INSERT INTO events (id, event_type, payload, idempotency_key, request_hash, trace_parent)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
			RETURNING `+eventColumns,
			id.New(id.PrefixEvent), in.EventType, in.Payload, key, hash, nullIfEmpty(in.TraceParent))
		if err != nil {
			return err
		}
		ev, err := pgx.CollectExactlyOneRow(rows, scanEvent)
		if errors.Is(err, pgx.ErrNoRows) {
			// The key already exists. ON CONFLICT waited for the other
			// transaction to commit, so the row is visible now.
			return s.loadIdempotentReplay(ctx, tx, in.IdempotencyKey, hash, &res)
		}
		if err != nil {
			return err
		}
		res.Event = ev

		// Lock the matching endpoints FOR SHARE so a concurrent delete waits
		// for us (and then sees and cancels our deliveries) instead of racing.
		rows, err = tx.Query(ctx, `
			SELECT id FROM endpoints
			WHERE deleted_at IS NULL
			  AND status IN ('active', 'paused')
			  AND event_types && ARRAY[$1, '*']::text[]
			ORDER BY id
			FOR SHARE`, in.EventType)
		if err != nil {
			return err
		}
		endpointIDs, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		if len(endpointIDs) == 0 {
			return nil
		}
		deliveryIDs := make([]string, len(endpointIDs))
		for i := range endpointIDs {
			deliveryIDs[i] = id.New(id.PrefixDelivery)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO deliveries (id, event_id, endpoint_id)
			SELECT d, $2, e FROM unnest($1::text[], $3::text[]) AS t(d, e)`,
			deliveryIDs, ev.ID, endpointIDs)
		if err != nil {
			return err
		}
		res.DeliveryCount = len(endpointIDs)
		// Wake idle workers. NOTIFY inside a transaction is delivered only
		// at COMMIT, so no worker can be woken for rows it cannot see yet.
		_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`, NotifyChannel)
		return err
	})
	return res, err
}

func (s *Store) loadIdempotentReplay(ctx context.Context, tx pgx.Tx, key string, hash []byte, res *CreateEventResult) error {
	var stored []byte
	rows, err := tx.Query(ctx, `SELECT `+eventColumns+`, request_hash FROM events WHERE idempotency_key = $1`, key)
	if err != nil {
		return err
	}
	ev, err := pgx.CollectExactlyOneRow(rows, func(row pgx.CollectableRow) (Event, error) {
		var e Event
		err := row.Scan(&e.ID, &e.EventType, &e.Payload, &e.IdempotencyKey, &e.CreatedAt, &stored)
		return e, err
	})
	if err != nil {
		return err
	}
	if !bytes.Equal(stored, hash) {
		return ErrIdempotencyMismatch
	}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE event_id = $1`, ev.ID).Scan(&res.DeliveryCount); err != nil {
		return err
	}
	res.Event = ev
	res.Replayed = true
	return nil
}

// GetEvent returns one event.
func (s *Store) GetEvent(ctx context.Context, eventID string) (Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+eventColumns+` FROM events WHERE id = $1`, eventID)
	if err != nil {
		return Event{}, err
	}
	e, err := pgx.CollectExactlyOneRow(rows, scanEvent)
	return e, notFoundIfNoRows(err)
}

// ListEvents returns events newest first, optionally filtered by type.
func (s *Store) ListEvents(ctx context.Context, eventType string, p Page) ([]Event, string, error) {
	p = p.normalize()
	rows, err := s.pool.Query(ctx, `
		SELECT `+eventColumns+` FROM events
		WHERE ($1 = '' OR event_type = $1) AND ($2 = '' OR id < $2)
		ORDER BY id DESC
		LIMIT $3`, eventType, p.Cursor, p.Limit+1)
	if err != nil {
		return nil, "", err
	}
	list, err := pgx.CollectRows(rows, scanEvent)
	if err != nil {
		return nil, "", err
	}
	list, next := trimPage(list, p.Limit, func(e Event) string { return e.ID })
	return list, next, nil
}
