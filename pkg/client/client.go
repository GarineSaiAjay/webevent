// Package client is a Go SDK for the webevent HTTP API. The CLI is built on
// it, and producers can use it to send events from their own services.
//
//	c := client.New("https://webhooks.internal.example.com", os.Getenv("WEBEVENT_API_KEY"))
//	ev, err := c.CreateEvent(ctx, client.CreateEventInput{
//		EventType:      "user.created",
//		Payload:        map[string]any{"id": 42},
//		IdempotencyKey: "user-42-created",
//	})
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to one webevent server.
type Client struct {
	baseURL   string
	apiKey    string
	http      *http.Client
	UserAgent string
}

// Option customises a Client.
type Option func(*Client)

// WithHTTPClient replaces the default HTTP client (30s timeout).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// New returns a client for baseURL (e.g. "http://localhost:8080").
func New(baseURL, apiKey string, opts ...Option) *Client {
	c := &Client{
		baseURL:   strings.TrimRight(baseURL, "/"),
		apiKey:    apiKey,
		http:      &http.Client{Timeout: 30 * time.Second},
		UserAgent: "webevent-go-client",
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// APIError is a non-2xx response from the server.
type APIError struct {
	StatusCode int
	Code       string            `json:"code"`
	Message    string            `json:"message"`
	Fields     map[string]string `json:"fields,omitempty"`
	RequestID  string
}

func (e *APIError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s (HTTP %d)", e.Message, e.StatusCode)
	for k, v := range e.Fields {
		fmt.Fprintf(&b, "\n  %s: %s", k, v)
	}
	return b.String()
}

// IsNotFound reports whether err is a 404 from the API.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}

// do performs one request and decodes the JSON response into out. It returns
// the response headers (the body is always fully read and closed here).
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, headers map[string]string, out any) (http.Header, error) {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rdr = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return resp.Header, err
	}
	if resp.StatusCode >= 300 {
		apiErr := &APIError{StatusCode: resp.StatusCode, RequestID: resp.Header.Get("X-Request-Id")}
		var env struct {
			Error *APIError `json:"error"`
		}
		if json.Unmarshal(data, &env) == nil && env.Error != nil {
			apiErr.Code, apiErr.Message, apiErr.Fields = env.Error.Code, env.Error.Message, env.Error.Fields
		} else {
			apiErr.Message = strings.TrimSpace(string(data))
			if apiErr.Message == "" {
				apiErr.Message = http.StatusText(resp.StatusCode)
			}
		}
		return resp.Header, apiErr
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.Header, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp.Header, nil
}

// ListOptions are common pagination parameters.
type ListOptions struct {
	Limit  int
	Cursor string
}

func (o ListOptions) values() url.Values {
	v := url.Values{}
	if o.Limit > 0 {
		v.Set("limit", strconv.Itoa(o.Limit))
	}
	if o.Cursor != "" {
		v.Set("cursor", o.Cursor)
	}
	return v
}

// Page is one page of a list response.
type Page[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"next_cursor"`
}

// ---- Endpoints -------------------------------------------------------------

// Endpoint mirrors the API's endpoint object.
type Endpoint struct {
	ID                  string    `json:"id"`
	URL                 string    `json:"url"`
	Description         string    `json:"description"`
	EventTypes          []string  `json:"event_types"`
	Status              string    `json:"status"`
	RateLimitPerSec     *int      `json:"rate_limit_per_sec"`
	MaxInFlight         int       `json:"max_in_flight"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	DisabledReason      *string   `json:"disabled_reason,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	Secret              string    `json:"secret,omitempty"`
}

// CreateEndpointInput is the body of POST /v1/endpoints.
type CreateEndpointInput struct {
	URL             string   `json:"url"`
	Description     string   `json:"description,omitempty"`
	EventTypes      []string `json:"event_types"`
	RateLimitPerSec *int     `json:"rate_limit_per_sec,omitempty"`
	MaxInFlight     *int     `json:"max_in_flight,omitempty"`
}

// UpdateEndpointInput is the body of PATCH /v1/endpoints/{id}; nil fields are unchanged.
type UpdateEndpointInput struct {
	URL             *string   `json:"url,omitempty"`
	Description     *string   `json:"description,omitempty"`
	EventTypes      *[]string `json:"event_types,omitempty"`
	Status          *string   `json:"status,omitempty"`
	RateLimitPerSec *int      `json:"rate_limit_per_sec,omitempty"` // 0 removes the limit
	MaxInFlight     *int      `json:"max_in_flight,omitempty"`
}

// CreateEndpoint registers a new endpoint. The response includes its secret.
func (c *Client) CreateEndpoint(ctx context.Context, in CreateEndpointInput) (*Endpoint, error) {
	var ep Endpoint
	_, err := c.do(ctx, http.MethodPost, "/v1/endpoints", nil, in, nil, &ep)
	return &ep, err
}

// ListEndpoints returns one page of endpoints, newest first.
func (c *Client) ListEndpoints(ctx context.Context, o ListOptions) (*Page[Endpoint], error) {
	var p Page[Endpoint]
	_, err := c.do(ctx, http.MethodGet, "/v1/endpoints", o.values(), nil, nil, &p)
	return &p, err
}

// GetEndpoint fetches one endpoint.
func (c *Client) GetEndpoint(ctx context.Context, id string) (*Endpoint, error) {
	var ep Endpoint
	_, err := c.do(ctx, http.MethodGet, "/v1/endpoints/"+url.PathEscape(id), nil, nil, nil, &ep)
	return &ep, err
}

// UpdateEndpoint applies a partial update.
func (c *Client) UpdateEndpoint(ctx context.Context, id string, in UpdateEndpointInput) (*Endpoint, error) {
	var ep Endpoint
	_, err := c.do(ctx, http.MethodPatch, "/v1/endpoints/"+url.PathEscape(id), nil, in, nil, &ep)
	return &ep, err
}

// DeleteEndpoint deletes an endpoint and cancels its pending deliveries.
func (c *Client) DeleteEndpoint(ctx context.Context, id string) error {
	_, err := c.do(ctx, http.MethodDelete, "/v1/endpoints/"+url.PathEscape(id), nil, nil, nil, nil)
	return err
}

// GetEndpointSecret reveals an endpoint's signing secret.
func (c *Client) GetEndpointSecret(ctx context.Context, id string) (string, error) {
	var out struct {
		Secret string `json:"secret"`
	}
	_, err := c.do(ctx, http.MethodGet, "/v1/endpoints/"+url.PathEscape(id)+"/secret", nil, nil, nil, &out)
	return out.Secret, err
}

// RotateSecretResult is the response of a secret rotation.
type RotateSecretResult struct {
	Secret                   string    `json:"secret"`
	PreviousSecretValidUntil time.Time `json:"previous_secret_valid_until"`
}

// RotateEndpointSecret issues a new secret; the old one stays valid for grace.
func (c *Client) RotateEndpointSecret(ctx context.Context, id string, grace time.Duration) (*RotateSecretResult, error) {
	var out RotateSecretResult
	body := map[string]string{"grace_period": grace.String()}
	_, err := c.do(ctx, http.MethodPost, "/v1/endpoints/"+url.PathEscape(id)+"/rotate-secret", nil, body, nil, &out)
	return &out, err
}

// ReplayResult is the response of a bulk replay.
type ReplayResult struct {
	Replayed int       `json:"replayed"`
	Since    time.Time `json:"since"`
	Until    time.Time `json:"until"`
}

// ReplayEndpoint re-queues dead deliveries of an endpoint that died in
// [since, until). Zero times use the server defaults (last 7 days).
func (c *Client) ReplayEndpoint(ctx context.Context, id string, since, until time.Time) (*ReplayResult, error) {
	body := map[string]any{}
	if !since.IsZero() {
		body["since"] = since
	}
	if !until.IsZero() {
		body["until"] = until
	}
	var out ReplayResult
	_, err := c.do(ctx, http.MethodPost, "/v1/endpoints/"+url.PathEscape(id)+"/replay", nil, body, nil, &out)
	return &out, err
}

// ---- Events ----------------------------------------------------------------

// Event mirrors the API's event object.
type Event struct {
	ID             string          `json:"id"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	DeliveryCount  int             `json:"delivery_count,omitempty"`
}

// CreateEventInput describes an event to publish.
type CreateEventInput struct {
	EventType string
	// Payload is any JSON-serialisable value, or a json.RawMessage.
	Payload        any
	IdempotencyKey string
}

// CreateEventResult adds whether the server returned an earlier event for
// the same idempotency key.
type CreateEventResult struct {
	Event
	Replayed bool
}

// CreateEvent publishes an event.
func (c *Client) CreateEvent(ctx context.Context, in CreateEventInput) (*CreateEventResult, error) {
	headers := map[string]string{}
	if in.IdempotencyKey != "" {
		headers["Idempotency-Key"] = in.IdempotencyKey
	}
	body := map[string]any{"event_type": in.EventType, "payload": in.Payload}
	var out CreateEventResult
	h, err := c.do(ctx, http.MethodPost, "/v1/events", nil, body, headers, &out.Event)
	if err != nil {
		return nil, err
	}
	out.Replayed = h.Get("Idempotent-Replayed") == "true"
	return &out, nil
}

// ListEvents returns events newest first, optionally of one type.
func (c *Client) ListEvents(ctx context.Context, eventType string, o ListOptions) (*Page[Event], error) {
	q := o.values()
	if eventType != "" {
		q.Set("event_type", eventType)
	}
	var p Page[Event]
	_, err := c.do(ctx, http.MethodGet, "/v1/events", q, nil, nil, &p)
	return &p, err
}

// GetEvent fetches one event.
func (c *Client) GetEvent(ctx context.Context, id string) (*Event, error) {
	var ev Event
	_, err := c.do(ctx, http.MethodGet, "/v1/events/"+url.PathEscape(id), nil, nil, nil, &ev)
	return &ev, err
}

// ---- Deliveries and attempts -------------------------------------------------

// Delivery mirrors the API's delivery object.
type Delivery struct {
	ID             string     `json:"id"`
	EventID        string     `json:"event_id"`
	EndpointID     string     `json:"endpoint_id"`
	Status         string     `json:"status"`
	AttemptCount   int        `json:"attempt_count"`
	ReplayCount    int        `json:"replay_count"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	LastAttemptAt  *time.Time `json:"last_attempt_at,omitempty"`
	LastStatusCode *int       `json:"last_status_code,omitempty"`
	LastError      *string    `json:"last_error,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	Attempts       []Attempt  `json:"attempts,omitempty"`
}

// Attempt is one HTTP request made for a delivery.
type Attempt struct {
	ID            string    `json:"id"`
	DeliveryID    string    `json:"delivery_id"`
	AttemptNumber int       `json:"attempt_number"`
	StatusCode    *int      `json:"status_code,omitempty"`
	Error         *string   `json:"error,omitempty"`
	ResponseBody  *string   `json:"response_body,omitempty"`
	DurationMS    int       `json:"duration_ms"`
	CreatedAt     time.Time `json:"created_at"`
	// Present in the attempt log (ListAttempts):
	EventID    string `json:"event_id,omitempty"`
	EventType  string `json:"event_type,omitempty"`
	EndpointID string `json:"endpoint_id,omitempty"`
	URL        string `json:"url,omitempty"`
}

// DeliveryFilter narrows ListDeliveries.
type DeliveryFilter struct {
	EventID, EndpointID, Status string
}

// ListDeliveries returns deliveries newest first.
func (c *Client) ListDeliveries(ctx context.Context, f DeliveryFilter, o ListOptions) (*Page[Delivery], error) {
	q := o.values()
	for k, v := range map[string]string{"event_id": f.EventID, "endpoint_id": f.EndpointID, "status": f.Status} {
		if v != "" {
			q.Set(k, v)
		}
	}
	var p Page[Delivery]
	_, err := c.do(ctx, http.MethodGet, "/v1/deliveries", q, nil, nil, &p)
	return &p, err
}

// GetDelivery fetches a delivery with its full attempt history.
func (c *Client) GetDelivery(ctx context.Context, id string) (*Delivery, error) {
	var d Delivery
	_, err := c.do(ctx, http.MethodGet, "/v1/deliveries/"+url.PathEscape(id), nil, nil, nil, &d)
	return &d, err
}

// ReplayDelivery re-queues a dead or succeeded delivery.
func (c *Client) ReplayDelivery(ctx context.Context, id string) (*Delivery, error) {
	var d Delivery
	_, err := c.do(ctx, http.MethodPost, "/v1/deliveries/"+url.PathEscape(id)+"/replay", nil, nil, nil, &d)
	return &d, err
}

// AttemptFilter narrows ListAttempts.
type AttemptFilter struct {
	EndpointID, EventID, DeliveryID string
	// After returns only attempts newer than this attempt id, oldest first
	// (tail mode). NextCursor of the result is the next After.
	After string
}

// ListAttempts reads the attempt log.
func (c *Client) ListAttempts(ctx context.Context, f AttemptFilter, o ListOptions) (*Page[Attempt], error) {
	q := o.values()
	for k, v := range map[string]string{"endpoint_id": f.EndpointID, "event_id": f.EventID, "delivery_id": f.DeliveryID, "after": f.After} {
		if v != "" {
			q.Set(k, v)
		}
	}
	var p Page[Attempt]
	_, err := c.do(ctx, http.MethodGet, "/v1/attempts", q, nil, nil, &p)
	return &p, err
}

// Health calls /healthz and returns the server version.
func (c *Client) Health(ctx context.Context) (string, error) {
	var out struct {
		Version string `json:"version"`
	}
	_, err := c.do(ctx, http.MethodGet, "/healthz", nil, nil, nil, &out)
	return out.Version, err
}
