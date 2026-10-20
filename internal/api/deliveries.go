package api

import (
	"errors"
	"net/http"

	"github.com/GarineSaiAjay/webevent/internal/id"
	"github.com/GarineSaiAjay/webevent/internal/store"
)

var validDeliveryStatuses = map[string]bool{
	store.DeliveryPending:   true,
	store.DeliverySucceeded: true,
	store.DeliveryDead:      true,
	store.DeliveryCancelled: true,
}

func (s *Server) handleListDeliveries(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	q := r.URL.Query()
	f := store.DeliveryFilter{EventID: q.Get("event_id"), EndpointID: q.Get("endpoint_id"), Status: q.Get("status")}
	if f.Status != "" && !validDeliveryStatuses[f.Status] {
		writeError(w, r, s.log, errBadRequest("status is invalid"))
		return
	}
	list, next, err := s.store.ListDeliveries(r.Context(), f, page)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse[store.Delivery]{Data: list, NextCursor: next})
}

type deliveryResponse struct {
	store.Delivery
	Attempts []store.Attempt `json:"attempts"`
}

// handleGetDelivery returns a delivery together with its full attempt log.
func (s *Server) handleGetDelivery(w http.ResponseWriter, r *http.Request) {
	dID := r.PathValue("id")
	if !id.Valid(id.PrefixDelivery, dID) {
		writeError(w, r, s.log, errNotFound("delivery"))
		return
	}
	d, err := s.store.GetDelivery(r.Context(), dID)
	if errors.Is(err, store.ErrNotFound) {
		err = errNotFound("delivery")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	attempts, err := s.store.ListAttempts(r.Context(), dID)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	if attempts == nil {
		attempts = []store.Attempt{}
	}
	writeJSON(w, http.StatusOK, deliveryResponse{Delivery: d, Attempts: attempts})
}

// handleReplayDelivery puts a dead (or succeeded) delivery back in the queue
// with a fresh retry budget. Its attempt history is kept.
func (s *Server) handleReplayDelivery(w http.ResponseWriter, r *http.Request) {
	dID := r.PathValue("id")
	if !id.Valid(id.PrefixDelivery, dID) {
		writeError(w, r, s.log, errNotFound("delivery"))
		return
	}
	d, err := s.store.ReplayDelivery(r.Context(), dID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		err = errNotFound("delivery")
	case errors.Is(err, store.ErrNotReplayable):
		err = errConflict("only dead or succeeded deliveries can be replayed")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusAccepted, d)
}
