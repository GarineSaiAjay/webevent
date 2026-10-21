package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/delivery"
	"github.com/GarineSaiAjay/webevent/internal/id"
	"github.com/GarineSaiAjay/webevent/internal/store"
)

// `webevent listen` support.
//
// Instead of exposing the developer's laptop to the internet (tunnelling),
// the CLI connects OUT to the server: it opens a "listen session" (a
// temporary endpoint of kind "listen"), long-polls for deliveries, forwards
// each one to a local URL, and reports the result. The deliveries go
// through exactly the same queue, leases, retries and signatures as real
// endpoints, so what you test locally is what production does.

const (
	listenSessionTTL = 2 * time.Minute  // session deleted if not polled for this long
	listenLease      = 60 * time.Second // time the CLI has to forward and report
	listenMaxWait    = 25 * time.Second // long-poll cap (below the server's 30s write timeout)
	listenMaxBatch   = 20
)

type createListenRequest struct {
	EventTypes []string `json:"event_types"`
}

func (s *Server) handleCreateListenSession(w http.ResponseWriter, r *http.Request) {
	var req createListenRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(w, r, &req); err != nil {
			writeError(w, r, s.log, err)
			return
		}
	}
	if len(req.EventTypes) == 0 {
		req.EventTypes = []string{"*"}
	}
	types, msg := normalizeEventTypes(req.EventTypes)
	if msg != "" {
		writeError(w, r, s.log, errValidation(map[string]string{"event_types": msg}))
		return
	}
	ep, err := s.store.CreateListenSession(r.Context(), types, listenSessionTTL)
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"session_id":  ep.ID,
		"secret":      ep.Secret,
		"event_types": ep.EventTypes,
	})
}

// listenDelivery is one delivery handed to the CLI: the exact headers and
// body a real endpoint would receive, plus what it needs to report back.
type listenDelivery struct {
	DeliveryID    string            `json:"delivery_id"`
	LeaseToken    string            `json:"lease_token"`
	AttemptNumber int               `json:"attempt_number"`
	EventID       string            `json:"event_id"`
	EventType     string            `json:"event_type"`
	Headers       map[string]string `json:"headers"`
	Body          string            `json:"body"`
}

// handleListenPoll long-polls for up to ?wait= (max 25s) until deliveries
// are due for the session, claims them, and returns them signed.
func (s *Server) handleListenPoll(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	if !id.Valid(id.PrefixEndpoint, sessionID) {
		writeError(w, r, s.log, errNotFound("listen session"))
		return
	}
	wait := 20 * time.Second
	if v := r.URL.Query().Get("wait"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 || d > listenMaxWait {
			writeError(w, r, s.log, errBadRequest("wait must be a duration between 0s and 25s"))
			return
		}
		wait = d
	}
	ctx := r.Context()
	deadline := time.Now().Add(wait)
	for {
		if err := s.store.TouchListenSession(ctx, sessionID, listenSessionTTL); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				err = errNotFound("listen session")
			}
			writeError(w, r, s.log, err)
			return
		}
		jobs, err := s.store.ClaimListenJobs(ctx, sessionID, "listen:"+sessionID, listenMaxBatch, listenLease)
		if err != nil {
			writeError(w, r, s.log, err)
			return
		}
		if len(jobs) > 0 || !time.Now().Before(deadline) {
			out := make([]listenDelivery, 0, len(jobs))
			for _, j := range jobs {
				h, body, err := delivery.SignedRequest(j, time.Now(), "webevent/"+s.version)
				if err != nil {
					writeError(w, r, s.log, err)
					return
				}
				headers := map[string]string{}
				for k := range h {
					headers[k] = h.Get(k)
				}
				out = append(out, listenDelivery{
					DeliveryID: j.DeliveryID, LeaseToken: j.LeaseToken, AttemptNumber: j.AttemptNumber,
					EventID: j.EventID, EventType: j.EventType, Headers: headers, Body: string(body),
				})
			}
			writeJSON(w, http.StatusOK, map[string]any{"data": out})
			return
		}
		select {
		case <-ctx.Done():
			return // client went away; leases (if any) were not taken
		case <-time.After(250 * time.Millisecond):
		}
	}
}

type listenResult struct {
	DeliveryID string `json:"delivery_id"`
	LeaseToken string `json:"lease_token"`
	StatusCode int    `json:"status_code"`
	Error      string `json:"error"`
	DurationMS int    `json:"duration_ms"`
	Response   string `json:"response_body"`
}

// handleListenResults records what the local server answered, applying the
// normal retry policy (so a 500 from your laptop is retried like any other).
func (s *Server) handleListenResults(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	if !id.Valid(id.PrefixEndpoint, sessionID) {
		writeError(w, r, s.log, errNotFound("listen session"))
		return
	}
	var results []listenResult
	if err := decodeJSON(w, r, &results); err != nil {
		writeError(w, r, s.log, err)
		return
	}
	if len(results) > 100 {
		writeError(w, r, s.log, errBadRequest("at most 100 results per request"))
		return
	}
	recorded := 0
	for _, res := range results {
		job, err := s.store.LoadLeasedJob(r.Context(), sessionID, res.DeliveryID, res.LeaseToken)
		if errors.Is(err, store.ErrNotFound) {
			continue // lease expired or delivery gone: the queue will retry it
		}
		if err != nil {
			writeError(w, r, s.log, err)
			return
		}
		ar := store.AttemptResult{
			StatusCode:   res.StatusCode,
			Error:        truncateString(res.Error, 1000),
			ResponseBody: truncateString(res.Response, 4096),
			Duration:     time.Duration(res.DurationMS) * time.Millisecond,
		}
		if ar.Error == "" && !delivery.Successful(ar.StatusCode) {
			ar.Error = "local server responded " + strconv.Itoa(ar.StatusCode)
		}
		dec := s.policy.Decide(job.RetryAttempt, ar)
		rr, err := s.store.RecordAttempt(r.Context(), store.AttemptRecord{Job: job, Result: ar, Outcome: dec.Outcome, RetryIn: dec.Delay})
		if err != nil {
			writeError(w, r, s.log, err)
			return
		}
		if rr.Applied {
			recorded++
		}
	}
	writeJSON(w, http.StatusOK, map[string]int{"recorded": recorded})
}

func (s *Server) handleDeleteListenSession(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	if !id.Valid(id.PrefixEndpoint, sessionID) {
		writeError(w, r, s.log, errNotFound("listen session"))
		return
	}
	ep, err := s.store.GetEndpoint(r.Context(), sessionID)
	if err == nil && ep.Kind != store.KindListen {
		err = store.ErrNotFound
	}
	if err == nil {
		err = s.store.DeleteEndpoint(r.Context(), sessionID)
	}
	if errors.Is(err, store.ErrNotFound) {
		err = errNotFound("listen session")
	}
	if err != nil {
		writeError(w, r, s.log, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// truncateString caps client-supplied text and makes it safe for a Postgres
// text column (valid UTF-8, no NUL bytes).
func truncateString(s string, n int) string {
	if len(s) > n {
		s = s[:n]
	}
	return strings.ToValidUTF8(strings.ReplaceAll(s, "\x00", ""), "\uFFFD")
}
