// Package delivery sends webhooks: it turns queued deliveries into HTTP
// requests and records the outcome.
package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

// maxStoredResponse is how much of a receiver's response body we keep for
// debugging. We read at most maxReadResponse so a hostile receiver cannot
// make us buffer gigabytes.
const (
	maxStoredResponse = 4 << 10
	maxReadResponse   = 64 << 10
)

// Envelope is the JSON body POSTed to receivers.
type Envelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

// BuildBody renders the exact bytes we send for a job.
func BuildBody(j store.Job) ([]byte, error) {
	return json.Marshal(Envelope{ID: j.EventID, Type: j.EventType, CreatedAt: j.EventCreatedAt.UTC(), Data: j.Payload})
}

// Sender performs the HTTP request for a job.
type Sender struct {
	client    *http.Client
	userAgent string
}

// NewSender returns a Sender whose requests time out after timeout.
func NewSender(timeout time.Duration, userAgent string) *Sender {
	return &Sender{
		userAgent: userAgent,
		client: &http.Client{
			Timeout: timeout,
			// A webhook URL that redirects is a misconfiguration; following
			// redirects would also let a receiver bounce us to internal hosts.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// Send POSTs the job and reports what happened. A non-nil error is never
// returned: every outcome (including transport failures) is a result.
func (s *Sender) Send(ctx context.Context, j store.Job) store.AttemptResult {
	start := time.Now()
	body, err := BuildBody(j)
	if err != nil {
		return store.AttemptResult{Error: "build body: " + err.Error(), Duration: time.Since(start), Permanent: true}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.URL, bytes.NewReader(body))
	if err != nil {
		return store.AttemptResult{Error: "build request: " + err.Error(), Duration: time.Since(start), Permanent: true}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	req.Header.Set("Webhook-Id", j.EventID)

	resp, err := s.client.Do(req)
	if err != nil {
		return store.AttemptResult{Error: err.Error(), Duration: time.Since(start)}
	}
	defer func() { _ = resp.Body.Close() }()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxReadResponse))
	res := store.AttemptResult{
		StatusCode:   resp.StatusCode,
		ResponseBody: sanitizeBody(raw, maxStoredResponse),
		Duration:     time.Since(start),
	}
	if !Successful(resp.StatusCode) {
		res.Error = fmt.Sprintf("receiver responded %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
		res.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	}
	return res
}

// Successful reports whether an HTTP status counts as delivered.
func Successful(code int) bool { return code >= 200 && code < 300 }

// sanitizeBody keeps at most n bytes of a response body and makes it safe to
// store in a Postgres text column: NUL bytes are removed and invalid UTF-8
// (including a rune cut in half by the truncation) becomes U+FFFD.
func sanitizeBody(b []byte, n int) string {
	if len(b) > n {
		b = b[:n]
	}
	b = bytes.ReplaceAll(b, []byte{0}, nil)
	return string(bytes.ToValidUTF8(b, []byte("\uFFFD")))
}
