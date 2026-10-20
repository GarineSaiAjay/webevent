package api_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

type eventResp struct {
	store.Event
	DeliveryCount int `json:"delivery_count"`
}

func TestCreateEvent(t *testing.T) {
	a := newTestAPI(t)
	createEndpoint(t, a, "https://example.com/a", "user.created")
	createEndpoint(t, a, "https://example.com/b", "*")

	rec := a.do(http.MethodPost, "/v1/events", map[string]any{
		"event_type": "user.created",
		"payload":    map[string]any{"user_id": 42, "email": "a@b.c"},
	})
	expectStatus(t, rec, http.StatusAccepted)
	ev := decode[eventResp](t, rec)
	if !strings.HasPrefix(ev.ID, "evt_") || ev.EventType != "user.created" || ev.DeliveryCount != 2 {
		t.Fatalf("unexpected response %+v", ev)
	}

	rec = a.do(http.MethodGet, "/v1/events/"+ev.ID, nil)
	expectStatus(t, rec, http.StatusOK)
	got := decode[store.Event](t, rec)
	if !strings.Contains(string(got.Payload), `"user_id": 42`) && !strings.Contains(string(got.Payload), `"user_id":42`) {
		t.Fatalf("payload not stored: %s", got.Payload)
	}

	rec = a.do(http.MethodGet, "/v1/deliveries?event_id="+ev.ID, nil)
	expectStatus(t, rec, http.StatusOK)
	list := decode[struct {
		Data []store.Delivery `json:"data"`
	}](t, rec)
	if len(list.Data) != 2 {
		t.Fatalf("want 2 deliveries, got %d", len(list.Data))
	}

	rec = a.do(http.MethodGet, "/v1/deliveries/"+list.Data[0].ID, nil)
	expectStatus(t, rec, http.StatusOK)
	if !strings.Contains(rec.Body.String(), `"attempts":[]`) {
		t.Fatalf("delivery detail should include an empty attempts array: %s", rec.Body)
	}
}

func TestCreateEventValidation(t *testing.T) {
	a := newTestAPI(t)
	cases := map[string]struct {
		body  any
		field string
	}{
		"missing type":   {map[string]any{"payload": map[string]any{}}, "event_type"},
		"bad type":       {map[string]any{"event_type": ".starts.with.dot", "payload": map[string]any{}}, "event_type"},
		"missing body":   {map[string]any{"event_type": "a.b"}, "payload"},
		"null payload":   {map[string]any{"event_type": "a.b", "payload": nil}, "payload"},
		"huge payload":   {map[string]any{"event_type": "a.b", "payload": strings.Repeat("x", 300<<10)}, "payload"},
		"too long type":  {map[string]any{"event_type": strings.Repeat("a", 129), "payload": 1}, "event_type"},
		"wildcard type":  {map[string]any{"event_type": "*", "payload": 1}, "event_type"},
		"type with ws":   {map[string]any{"event_type": "a b", "payload": 1}, "event_type"},
		"type with ctrl": {map[string]any{"event_type": "a\nb", "payload": 1}, "event_type"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := a.do(http.MethodPost, "/v1/events", tc.body)
			expectStatus(t, rec, http.StatusUnprocessableEntity)
			if body := decode[errorBody](t, rec); body.Error.Fields[tc.field] == "" {
				t.Fatalf("expected error on %s: %+v", tc.field, body.Error)
			}
		})
	}
}

func TestCreateEventIdempotency(t *testing.T) {
	a := newTestAPI(t)
	createEndpoint(t, a, "https://example.com/a")
	body := map[string]any{"event_type": "order.placed", "payload": map[string]any{"n": 1}}

	rec := a.do(http.MethodPost, "/v1/events", body, "Idempotency-Key", "order-1")
	expectStatus(t, rec, http.StatusAccepted)
	first := decode[eventResp](t, rec)

	rec = a.do(http.MethodPost, "/v1/events", body, "Idempotency-Key", "order-1")
	expectStatus(t, rec, http.StatusOK)
	if rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatal("missing Idempotent-Replayed header")
	}
	if second := decode[eventResp](t, rec); second.ID != first.ID || second.DeliveryCount != 1 {
		t.Fatalf("replay returned %+v, want %s", second, first.ID)
	}

	body["payload"] = map[string]any{"n": 2}
	rec = a.do(http.MethodPost, "/v1/events", body, "Idempotency-Key", "order-1")
	expectStatus(t, rec, http.StatusConflict)

	rec = a.do(http.MethodPost, "/v1/events", body, "Idempotency-Key", "has space")
	expectStatus(t, rec, http.StatusBadRequest)
}

func TestListEventsAndDeliveriesValidation(t *testing.T) {
	a := newTestAPI(t)
	expectStatus(t, a.do(http.MethodGet, "/v1/events?event_type=bad%20type", nil), http.StatusBadRequest)
	expectStatus(t, a.do(http.MethodGet, "/v1/deliveries?status=exploded", nil), http.StatusBadRequest)
	expectStatus(t, a.do(http.MethodGet, "/v1/events/evt_nope", nil), http.StatusNotFound)
	expectStatus(t, a.do(http.MethodGet, "/v1/deliveries/dlv_nope", nil), http.StatusNotFound)
	expectStatus(t, a.do(http.MethodGet, "/v1/events?event_type=a.b", nil), http.StatusOK)
}
