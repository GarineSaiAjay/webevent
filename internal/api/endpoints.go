package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/GarineSaiAjay/webevent/internal/id"
	"github.com/GarineSaiAjay/webevent/internal/store"
)

type createEndpointRequest struct {
	URL         string   `json:"url"`
	Description string   `json:"description"`
	EventTypes  []string `json:"event_types"`
}

func (s *Server) handleCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	var req createEndpointRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, r, s.log, err)
		return
	}
	fields := map[string]string{}
	if msg := validateEndpointURL(req.URL); msg != "" {
		fields["url"] = msg
	}
	if len(req.Description) > maxDescriptionLength {
		fields["description"] = "must be at most " + strconv.Itoa(maxDescriptionLength) + " characters"
	}
	types, msg := normalizeEventTypes(req.EventTypes)
	if msg != "" {
		fields["event_types"] = msg
	}
	if len(fields) > 0 {
		writeError(w, r, s.log, errValidation(fields))
		return
	}

	ep, err := s.store.CreateEndpoint(r.Context(), store.NewEndpoint{
		URL: req.URL, Description: req.Description, EventTypes: types,
	})
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
	URL         *string   `json:"url"`
	Description *string   `json:"description"`
	EventTypes  *[]string `json:"event_types"`
	Status      *string   `json:"status"`
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
	patch := store.EndpointPatch{URL: req.URL, Description: req.Description, Status: req.Status}
	if req.URL != nil {
		if msg := validateEndpointURL(*req.URL); msg != "" {
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
