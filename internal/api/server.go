// Package api implements the HTTP API of the webhook delivery service.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/observability"
	"github.com/GarineSaiAjay/webevent/internal/store"
)

// Options are the dependencies of the API server.
type Options struct {
	Logger  *slog.Logger
	Store   *store.Store
	APIKeys []string
	Version string
	// AllowPrivateNetworks accepts endpoint URLs pointing at private or
	// loopback IP literals (development only).
	AllowPrivateNetworks bool
	// Metrics may be nil.
	Metrics *observability.Metrics
	// Draining, when set to true, makes /readyz fail so load balancers
	// stop sending traffic before the server shuts down. May be nil.
	Draining *atomic.Bool
}

// Server holds handler dependencies.
type Server struct {
	log          *slog.Logger
	store        *store.Store
	auth         *apiKeyAuth
	version      string
	allowPrivate bool
	metrics      *observability.Metrics
	draining     *atomic.Bool
}

// New builds the API and returns its root handler.
func New(opts Options) http.Handler {
	s := &Server{
		log:     opts.Logger,
		store:   opts.Store,
		auth:    newAPIKeyAuth(opts.APIKeys),
		version: opts.Version,

		allowPrivate: opts.AllowPrivateNetworks,
		metrics:      opts.Metrics,
		draining:     opts.Draining,
	}
	if s.draining == nil {
		s.draining = &atomic.Bool{}
	}

	// Unauthenticated operational routes.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	// Authenticated public API, versioned under /v1.
	v1 := http.NewServeMux()
	v1.HandleFunc("POST /v1/endpoints", s.handleCreateEndpoint)
	v1.HandleFunc("GET /v1/endpoints", s.handleListEndpoints)
	v1.HandleFunc("GET /v1/endpoints/{id}", s.handleGetEndpoint)
	v1.HandleFunc("PATCH /v1/endpoints/{id}", s.handleUpdateEndpoint)
	v1.HandleFunc("DELETE /v1/endpoints/{id}", s.handleDeleteEndpoint)
	v1.HandleFunc("POST /v1/endpoints/{id}/replay", s.handleReplayEndpoint)
	v1.HandleFunc("GET /v1/endpoints/{id}/secret", s.handleGetEndpointSecret)
	v1.HandleFunc("POST /v1/endpoints/{id}/rotate-secret", s.handleRotateEndpointSecret)
	v1.HandleFunc("POST /v1/events", s.handleCreateEvent)
	v1.HandleFunc("GET /v1/events", s.handleListEvents)
	v1.HandleFunc("GET /v1/events/{id}", s.handleGetEvent)
	v1.HandleFunc("GET /v1/deliveries", s.handleListDeliveries)
	v1.HandleFunc("GET /v1/deliveries/{id}", s.handleGetDelivery)
	v1.HandleFunc("POST /v1/deliveries/{id}/replay", s.handleReplayDelivery)
	v1.HandleFunc("GET /v1/attempts", s.handleListAttempts)
	v1.HandleFunc("/v1/", s.handleNoRoute)
	mux.Handle("/v1/", s.requireAPIKey(v1))

	mux.HandleFunc("/", s.handleNoRoute)

	return chain(mux, requestID, observe(s.log, s.metrics), recoverer(s.log))
}

func (s *Server) handleNoRoute(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, s.log, &Error{Status: http.StatusNotFound, Code: "not_found", Message: "no such route"})
}

// handleHealthz is a liveness probe: the process is up and serving HTTP.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": s.version})
}

// handleReadyz is a readiness probe: we can reach the database. Load
// balancers should only route traffic to ready instances.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Pool().Ping(ctx); err != nil {
		s.log.WarnContext(r.Context(), "readiness check failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
