package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
)

// Error is an error that knows how it should be rendered over HTTP.
// Handlers return these; writeError turns them into the standard envelope:
//
//	{"error": {"code": "not_found", "message": "endpoint not found"}}
type Error struct {
	Status  int               `json:"-"`
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

// writeError renders err. Unknown errors become a generic 500 and are logged
// with the request id; internal details never leak to clients.
func writeError(w http.ResponseWriter, r *http.Request, log *slog.Logger, err error) {
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		log.ErrorContext(r.Context(), "internal error",
			"err", err, "method", r.Method, "path", r.URL.Path, "request_id", RequestIDFrom(r.Context()))
		apiErr = &Error{Status: http.StatusInternalServerError, Code: "internal", Message: "internal server error"}
	}
	writeJSON(w, apiErr.Status, map[string]any{"error": apiErr})
}
