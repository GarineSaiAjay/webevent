package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/id"
	"github.com/GarineSaiAjay/webevent/internal/store"
)

type createEndpointRequest struct {
	URL             string   `json:"url"`
	Description     string   `json:"description"`
	EventTypes      []string `json:"event_types"`
	RateLimitPerSec *int     `json:"rate_limit_per_sec"`
	MaxInFlight     *int     `json:"max_in_flight"`
}

func validateLimits(rate, inFlight *int, fields map[string]string) {
	if rate != nil && (*rate < 0 || *rate > 10000) {
		fields["rate_limit_per_sec"] = "must be between 1 and 10000 (0 or null for unlimited)"
	}
	if inFlight != nil && (*inFlight < 1 || *inFlight > 1000) {
		fields["max_in_flight"] = "must be between 1 and 1000"
	}
}

func (s *Server) handleCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	var req createEndpointRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, r, s.log, err)
		return
	}
	fields := map[string]string{}
	if msg := validateEndpointURL(req.URL, s.allowPrivate); msg != "" {
		fields["url"] = msg
	}
	if len(req.Description) > maxDescriptionLength {
		fields["description"] = "must be at most " + strconv.Itoa(maxDescriptionLength) + " characters"
	}
	types, msg := normalizeEventTypes(req.EventTypes)
	if msg != "" {
		fields["event_types"] = msg
	}
	validateLimits(req.RateLimitPerSec, req.MaxInFlight, fields)
	if len(fields) > 0 {
		writeError(w, r, s.log, errValidation(fields))
		return
	}

	in := store.NewEndpoint{URL: req.URL, Description: req.Description, EventTypes: types, RateLimitPerSec: req.RateLimitPerSec}
	if req.MaxInFlight != nil {
		in.MaxInFlight = *req.MaxInFlight
	}
	ep, err := s.store.CreateEndpoint(r.Context(), in)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusCreated, ep)
}

func (s *Server) handleListEndpoints(w http.ResponseWriter, r *http.Request) {
	page, err := parsePage(r)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	list, next, err := s.store.ListEndpoints(r.Context(), page)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse[store.Endpoint]{Data: list, NextCursor: next})
}

func (s *Server) handleGetEndpoint(w http.ResponseWriter, r *http.Request) {
	epID := r.PathValue("id")
	if !id.Valid(id.PrefixEndpoint, epID) {
		writeError(w, r, s.log, errNotFound("endpoint"))
		return
	}
	ep, err := s.store.GetEndpoint(r.Context(), epID)
	if errors.Is(err, store.ErrNotFound) {
		err = errNotFound("endpoint")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, ep)
}

// updateEndpointRequest uses pointers so we can tell "absent" from "empty".
type updateEndpointRequest struct {
	URL             *string   `json:"url"`
	Description     *string   `json:"description"`
	EventTypes      *[]string `json:"event_types"`
	Status          *string   `json:"status"`
	RateLimitPerSec *int      `json:"rate_limit_per_sec"`
	MaxInFlight     *int      `json:"max_in_flight"`
}

func (s *Server) handleUpdateEndpoint(w http.ResponseWriter, r *http.Request) {
	epID := r.PathValue("id")
	if !id.Valid(id.PrefixEndpoint, epID) {
		writeError(w, r, s.log, errNotFound("endpoint"))
		return
	}
	var req updateEndpointRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, r, s.log, err)
		return
	}
	fields := map[string]string{}
	patch := store.EndpointPatch{
		URL: req.URL, Description: req.Description, Status: req.Status,
		RateLimitPerSec: req.RateLimitPerSec, MaxInFlight: req.MaxInFlight,
	}
	validateLimits(req.RateLimitPerSec, req.MaxInFlight, fields)
	if req.URL != nil {
		if msg := validateEndpointURL(*req.URL, s.allowPrivate); msg != "" {
			fields["url"] = msg
		}
	}
	if req.Description != nil && len(*req.Description) > maxDescriptionLength {
		fields["description"] = "must be at most " + strconv.Itoa(maxDescriptionLength) + " characters"
	}
	if req.EventTypes != nil {
		types, msg := normalizeEventTypes(*req.EventTypes)
		if msg != "" {
			fields["event_types"] = msg
		}
		patch.EventTypes = &types
	}
	if req.Status != nil && !validEndpointStatus(*req.Status) {
		fields["status"] = `must be "active" or "paused"`
	}
	if len(fields) > 0 {
		writeError(w, r, s.log, errValidation(fields))
		return
	}

	ep, err := s.store.UpdateEndpoint(r.Context(), epID, patch)
	if errors.Is(err, store.ErrNotFound) {
		err = errNotFound("endpoint")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, ep)
}

func (s *Server) handleDeleteEndpoint(w http.ResponseWriter, r *http.Request) {
	epID := r.PathValue("id")
	if !id.Valid(id.PrefixEndpoint, epID) {
		writeError(w, r, s.log, errNotFound("endpoint"))
		return
	}
	err := s.store.DeleteEndpoint(r.Context(), epID)
	if errors.Is(err, store.ErrNotFound) {
		err = errNotFound("endpoint")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type replayRequest struct {
	Since *time.Time `json:"since"`
	Until *time.Time `json:"until"`
}

// replayBatchSize bounds each UPDATE issued by a bulk replay.
const replayBatchSize = 1000

// handleReplayEndpoint re-queues every dead delivery of an endpoint that
// died in [since, until). since defaults to 7 days ago, until to now.
func (s *Server) handleReplayEndpoint(w http.ResponseWriter, r *http.Request) {
	epID := r.PathValue("id")
	if !id.Valid(id.PrefixEndpoint, epID) {
		writeError(w, r, s.log, errNotFound("endpoint"))
		return
	}
	var req replayRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(w, r, &req); err != nil {
			writeError(w, r, s.log, err)
			return
		}
	}
	now := time.Now()
	since, until := now.Add(-7*24*time.Hour), now
	if req.Since != nil {
		since = *req.Since
	}
	if req.Until != nil {
		until = *req.Until
	}
	if !since.Before(until) {
		writeError(w, r, s.log, errValidation(map[string]string{"since": "must be before until"}))
		return
	}

	n, err := s.store.ReplayDeadForEndpoint(r.Context(), epID, since, until, replayBatchSize)
	if errors.Is(err, store.ErrNotFound) {
		err = errNotFound("endpoint")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"replayed": n, "since": since.UTC(), "until": until.UTC()})
}

// handleGetEndpointSecret reveals the signing secret. It is a separate
// call (not a field on GET /endpoints/{id}) so secrets never leak into
// list responses or logs by accident.
func (s *Server) handleGetEndpointSecret(w http.ResponseWriter, r *http.Request) {
	epID := r.PathValue("id")
	if !id.Valid(id.PrefixEndpoint, epID) {
		writeError(w, r, s.log, errNotFound("endpoint"))
		return
	}
	secret, err := s.store.GetEndpointSecret(r.Context(), epID)
	if errors.Is(err, store.ErrNotFound) {
		err = errNotFound("endpoint")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"secret": secret})
}

type rotateSecretRequest struct {
	// GracePeriod is a Go duration string, e.g. "24h". Default 24h, max 7 days.
	GracePeriod *string `json:"grace_period"`
}

const maxGracePeriod = 7 * 24 * time.Hour

func (s *Server) handleRotateEndpointSecret(w http.ResponseWriter, r *http.Request) {
	epID := r.PathValue("id")
	if !id.Valid(id.PrefixEndpoint, epID) {
		writeError(w, r, s.log, errNotFound("endpoint"))
		return
	}
	var req rotateSecretRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(w, r, &req); err != nil {
			writeError(w, r, s.log, err)
			return
		}
	}
	grace := 24 * time.Hour
	if req.GracePeriod != nil {
		d, err := time.ParseDuration(*req.GracePeriod)
		if err != nil || d < 0 || d > maxGracePeriod {
			writeError(w, r, s.log, errValidation(map[string]string{"grace_period": "must be a duration between 0s and 168h"}))
			return
		}
		grace = d
	}
	secret, err := s.store.RotateEndpointSecret(r.Context(), epID, grace)
	if errors.Is(err, store.ErrNotFound) {
		err = errNotFound("endpoint")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"secret":                      secret,
		"previous_secret_valid_until": time.Now().Add(grace).UTC().Truncate(time.Second),
	})
}
