package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
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

func errBadRequest(msg string) *Error {
	return &Error{Status: http.StatusBadRequest, Code: "bad_request", Message: msg}
}

func errValidation(fields map[string]string) *Error {
	return &Error{Status: http.StatusUnprocessableEntity, Code: "validation_failed", Message: "request failed validation", Fields: fields}
}

func errNotFound(what string) *Error {
	return &Error{Status: http.StatusNotFound, Code: "not_found", Message: what + " not found"}
}

func errConflict(msg string) *Error {
	return &Error{Status: http.StatusConflict, Code: "conflict", Message: msg}
}

func errUnauthorized() *Error {
	return &Error{Status: http.StatusUnauthorized, Code: "unauthorized", Message: "missing or invalid API key"}
}

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

// maxBodyBytes caps every JSON request body. Event payloads are the largest
// thing we accept; 512 KiB leaves headroom over the 256 KiB payload limit.
const maxBodyBytes = 512 << 10

// decodeJSON strictly decodes a single JSON object into dst.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		return &Error{Status: http.StatusUnsupportedMediaType, Code: "unsupported_media_type", Message: "Content-Type must be application/json"}
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		var syntaxErr *json.SyntaxError
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.As(err, &maxErr):
			return &Error{Status: http.StatusRequestEntityTooLarge, Code: "payload_too_large", Message: fmt.Sprintf("request body exceeds %d bytes", maxBodyBytes)}
		case errors.Is(err, io.EOF):
			return errBadRequest("request body is empty")
		case errors.As(err, &syntaxErr):
			return errBadRequest(fmt.Sprintf("malformed JSON at offset %d", syntaxErr.Offset))
		case errors.As(err, &typeErr):
			return errBadRequest(fmt.Sprintf("field %q has the wrong type", typeErr.Field))
		case strings.HasPrefix(err.Error(), "json: unknown field"):
			return errBadRequest(strings.TrimPrefix(err.Error(), "json: "))
		default:
			return errBadRequest("invalid JSON body")
		}
	}
	if dec.More() {
		return errBadRequest("request body must contain a single JSON object")
	}
	return nil
}
