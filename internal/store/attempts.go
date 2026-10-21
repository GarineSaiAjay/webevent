package store

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// AttemptLog is an attempt joined with the context needed to read it on its
// own: which event and which endpoint it was for.
type AttemptLog struct {
	Attempt
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	EndpointID string `json:"endpoint_id"`
	URL        string `json:"url"`
}

// AttemptFilter narrows ListAttemptLog. Empty fields are ignored.
type AttemptFilter struct {
	EndpointID string
	EventID    string
	DeliveryID string
}

// ListAttemptLog returns attempts across deliveries.
//
// Two modes:
//   - after == "": newest first, paginated backwards with p.Cursor (browse).
//   - after != "": oldest first, only attempts with id > after (tail -f).
//     The returned cursor is the id of the last row, to pass as the next
//     `after`.
//
// Attempt ids are time-ordered, so "id > after" means "newer than". An
// attempt recorded by a worker whose clock runs a few milliseconds behind
// another's could sort just before the cursor and be skipped by a tail;
// that is acceptable for a live log view, and the browse mode never misses.
func (s *Store) ListAttemptLog(ctx context.Context, f AttemptFilter, p Page, after string) ([]AttemptLog, string, error) {
	p = p.normalize()
	order, cursorCond, cursor := "DESC", "($5 = '' OR a.id < $5)", p.Cursor
	if after != "" {
		order, cursorCond, cursor = "ASC", "a.id > $5", after
	}
	rows, err := s.pool.Query(ctx, `
		SELECT a.id, a.delivery_id, a.attempt_number, a.status_code, a.error, a.response_body, a.duration_ms, a.created_at,
		       e.id, e.event_type, ep.id, ep.url
		FROM delivery_attempts a
		JOIN deliveries d ON d.id = a.delivery_id
		JOIN events e     ON e.id = d.event_id
		JOIN endpoints ep ON ep.id = d.endpoint_id
		WHERE ($1 = '' OR d.endpoint_id = $1)
		  AND ($2 = '' OR d.event_id = $2)
		  AND ($3 = '' OR a.delivery_id = $3)
		  AND `+cursorCond+`
		ORDER BY a.id `+order+`
		LIMIT $4`, f.EndpointID, f.EventID, f.DeliveryID, p.Limit+1, cursor)
	if err != nil {
		return nil, "", err
	}
	list, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (AttemptLog, error) {
		var l AttemptLog
		err := row.Scan(&l.ID, &l.DeliveryID, &l.AttemptNumber, &l.StatusCode, &l.Error, &l.ResponseBody, &l.DurationMS,
			&l.CreatedAt, &l.EventID, &l.EventType, &l.EndpointID, &l.URL)
		return l, err
	})
	if err != nil {
		return nil, "", err
	}
	if after != "" {
		if len(list) > p.Limit {
			list = list[:p.Limit]
		}
		next := after
		if len(list) > 0 {
			next = list[len(list)-1].ID
		}
		return list, next, nil
	}
	list, next := trimPage(list, p.Limit, func(l AttemptLog) string { return l.ID })
	return list, next, nil
}
