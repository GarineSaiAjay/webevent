package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"time"
)

type ctxKey int

const requestIDKey ctxKey = iota

// RequestIDFrom returns the request id stored by the requestID middleware.
func RequestIDFrom(ctx context.Context) string {
	s, _ := ctx.Value(requestIDKey).(string)
	return s
}

var safeRequestID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// requestID propagates a caller-supplied X-Request-Id (if it is safe to log)
// or generates one, and echoes it back in the response.
func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-Id")
		if !safeRequestID.MatchString(rid) {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			rid = "req_" + hex.EncodeToString(b)
		}
		w.Header().Set("X-Request-Id", rid)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, rid)))
	})
}

// statusRecorder remembers the status code a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer
// (needed later for flushing streamed responses).
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// accessLog writes one structured line per request.
func accessLog(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}
			next.ServeHTTP(rec, r)
			if rec.status == 0 {
				rec.status = http.StatusOK
			}
			level := slog.LevelInfo
			if rec.status >= 500 {
				level = slog.LevelError
			}
			log.Log(r.Context(), level, "http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"bytes", rec.bytes,
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", RequestIDFrom(r.Context()),
			)
		})
	}
}

// recoverer converts a panic in a handler into a 500 instead of killing the
// connection, and logs the stack trace.
func recoverer(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if v := recover(); v != nil {
					if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
						panic(v)
					}
					log.ErrorContext(r.Context(), "panic in handler",
						"panic", v, "stack", string(debug.Stack()), "request_id", RequestIDFrom(r.Context()))
					writeJSON(w, http.StatusInternalServerError, map[string]any{
						"error": &Error{Code: "internal", Message: "internal server error"},
					})
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
