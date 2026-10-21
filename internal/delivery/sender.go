// Package delivery sends webhooks: it turns queued deliveries into HTTP
// requests and records the outcome.
package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/GarineSaiAjay/webevent/internal/netguard"
	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/pkg/webhook"
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

// SignedRequest builds the body and webhook headers for a job, signed at
// time now. The worker sends them itself; for `webevent listen` the API
// hands them to the CLI, which forwards them to a local server unchanged.
func SignedRequest(j store.Job, now time.Time, userAgent string) (http.Header, []byte, error) {
	if j.SecretErr != nil {
		return nil, nil, fmt.Errorf("signing secret unavailable: %w", j.SecretErr)
	}
	body, err := BuildBody(j)
	if err != nil {
		return nil, nil, fmt.Errorf("build body: %w", err)
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("User-Agent", userAgent)
	// webhook-id is the event id: identical across retries and replays, so
	// receivers can use it to drop duplicates (delivery is at-least-once).
	if err := webhook.SetHeaders(h, j.EventID, now, body, j.Secrets...); err != nil {
		return nil, nil, fmt.Errorf("sign: %w", err)
	}
	return h, body, nil
}

// SenderOptions configures a Sender.
type SenderOptions struct {
	Timeout   time.Duration
	UserAgent string
	// AllowPrivateNetworks disables SSRF protection. Only for local
	// development and tests, where receivers run on localhost.
	AllowPrivateNetworks bool
	// Now is the signing clock; nil means time.Now.
	Now func() time.Time
}

// Sender performs the HTTP request for a job.
type Sender struct {
	client    *http.Client
	userAgent string
	now       func() time.Time
}

// NewSender builds a Sender with a hardened HTTP client.
func NewSender(o SenderOptions) *Sender {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	if !o.AllowPrivateNetworks {
		dialer.Control = netguard.Control
	}
	transport := &http.Transport{
		// Never use HTTP(S)_PROXY from the environment: the guard checks
		// the IP we dial, and with a proxy that would be the proxy's IP.
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: o.Timeout,
		ExpectContinueTimeout: time.Second,
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	return &Sender{
		userAgent: o.UserAgent,
		now:       now,
		client: &http.Client{
			Transport: transport,
			Timeout:   o.Timeout,
			// A webhook URL that redirects is a misconfiguration; following
			// redirects would also let a receiver bounce us to internal hosts.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// Send signs and POSTs the job and reports what happened. Every outcome,
// including transport failures, is returned as a result.
func (s *Sender) Send(ctx context.Context, j store.Job) store.AttemptResult {
	start := time.Now()
	fail := func(msg string, permanent bool) store.AttemptResult {
		return store.AttemptResult{Error: msg, Duration: time.Since(start), Permanent: permanent}
	}
	headers, body, err := SignedRequest(j, s.now(), s.userAgent)
	if err != nil {
		return fail(err.Error(), true)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.URL, bytes.NewReader(body))
	if err != nil {
		return fail("build request: "+err.Error(), true)
	}
	req.Header = headers
	// Propagate our trace (traceparent header) so receivers that use
	// OpenTelemetry can continue it.
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := s.client.Do(req)
	if err != nil {
		// A blocked destination will stay blocked; don't burn retries on it.
		return fail(err.Error(), errors.Is(err, netguard.ErrBlocked))
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
	return string(bytes.ToValidUTF8(b, []byte("�")))
}
