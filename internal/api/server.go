// Package api implements the HTTP API of the webhook delivery service.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Options are the dependencies of the API server.
type Options struct {
	Logger  *slog.Logger
	Pool    *pgxpool.Pool
	Version string
}

// Server holds handler dependencies.
type Server struct {
	log     *slog.Logger
	pool    *pgxpool.Pool
	version string
}

// New builds the API and returns its root handler.
func New(opts Options) http.Handler {
	s := &Server{log: opts.Logger, pool: opts.Pool, version: opts.Version}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, r, s.log, &Error{Status: http.StatusNotFound, Code: "not_found", Message: "no such route"})
	})

	return chain(mux, requestID, recoverer(s.log), accessLog(s.log))
}

// handleHealthz is a liveness probe: the process is up and serving HTTP.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": s.version})
}

// handleReadyz is a readiness probe: we can reach the database. Load
// balancers should only route traffic to ready instances.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.pool.Ping(ctx); err != nil {
		s.log.WarnContext(r.Context(), "readiness check failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
