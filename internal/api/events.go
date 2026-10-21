package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/GarineSaiAjay/webevent/internal/id"
	"github.com/GarineSaiAjay/webevent/internal/observability"
	"github.com/GarineSaiAjay/webevent/internal/store"
)

const (
	maxPayloadBytes      = 256 << 10
	maxIdempotencyKeyLen = 255
)

type createEventRequest struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type eventResponse struct {
	store.Event
	DeliveryCount int `json:"delivery_count"`
}

func validIdempotencyKey(k string) bool {
	if len(k) == 0 || len(k) > maxIdempotencyKeyLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e { // printable ASCII, no spaces
			return false
		}
	}
	return true
}

// handleCreateEvent accepts an event and enqueues its deliveries.
//
// It answers 202 Accepted: the event is durably stored, delivery happens
// asynchronously. With an Idempotency-Key, retrying the same request returns
// the original event (200) instead of creating a duplicate.
func (s *Server) handleCreateEvent(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key != "" && !validIdempotencyKey(key) {
		writeError(w, r, s.log, errBadRequest("Idempotency-Key must be 1-255 printable ASCII characters without spaces"))
		return
	}
	var req createEventRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, r, s.log, err)
		return
	}
	fields := map[string]string{}
	if !validEventType(req.EventType) {
		fields["event_type"] = "must be 1-128 characters of letters, digits, '.', '_' or '-', starting with a letter or digit"
	}
	trimmed := bytes.TrimSpace(req.Payload)
	switch {
	case len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")):
		fields["payload"] = "is required"
	case len(trimmed) > maxPayloadBytes:
		fields["payload"] = "must be at most " + strconv.Itoa(maxPayloadBytes) + " bytes"
	}
	if len(fields) > 0 {
		writeError(w, r, s.log, errValidation(fields))
		return
	}

	res, err := s.store.CreateEvent(r.Context(), store.NewEvent{
		EventType:      req.EventType,
		Payload:        trimmed,
		IdempotencyKey: key,
		TraceParent:    observability.TraceParentFrom(r.Context()),
	})
	if errors.Is(err, store.ErrIdempotencyMismatch) {
		err = errConflict("Idempotency-Key was already used with a different request body")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}

	status := http.StatusAccepted
	if !res.Replayed {
		s.metrics.EventIngested(res.DeliveryCount)
	}
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		status = http.StatusOK
	}
	writeJSON(w, status, eventResponse{Event: res.Event, DeliveryCount: res.DeliveryCount})
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	evID := r.PathValue("id")
	if !id.Valid(id.PrefixEvent, evID) {
		writeError(w, r, s.log, errNotFound("event"))
		return
	}
	ev, err := s.store.GetEvent(r.Context(), evID)
	if errors.Is(err, store.ErrNotFound) {
		err = errNotFound("event")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	eventType := r.URL.Query().Get("event_type")
	if eventType != "" && !validEventType(eventType) {
		writeError(w, r, s.log, errBadRequest("event_type is invalid"))
		return
	}
	list, next, err := s.store.ListEvents(r.Context(), eventType, page)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse[store.Event]{Data: list, NextCursor: next})
}
