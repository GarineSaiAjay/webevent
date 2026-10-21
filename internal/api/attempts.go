package api

import (
	"net/http"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

// handleListAttempts serves the attempt log across deliveries. With
// ?after=<attempt id> it returns only newer attempts, oldest first, which is
// what `webevent logs --follow` polls.
func (s *Server) handleListAttempts(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	q := r.URL.Query()
	after := q.Get("after")
	if len(after) > 64 {
		writeError(w, r, s.log, errBadRequest("after is invalid"))
		return
	}
	f := store.AttemptFilter{EndpointID: q.Get("endpoint_id"), EventID: q.Get("event_id"), DeliveryID: q.Get("delivery_id")}
	list, next, err := s.store.ListAttemptLog(r.Context(), f, page, after)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, newList(list, next))
}
