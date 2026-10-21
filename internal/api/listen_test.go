package api_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/pkg/webhook"
)

type pollResp struct {
	Data []struct {
		DeliveryID string            `json:"delivery_id"`
		LeaseToken string            `json:"lease_token"`
		EventType  string            `json:"event_type"`
		Headers    map[string]string `json:"headers"`
		Body       string            `json:"body"`
	} `json:"data"`
}

func TestListenFlow(t *testing.T) {
	a := newTestAPI(t)
	rec := a.do(http.MethodPost, "/v1/listen", map[string]any{"event_types": []string{"order.paid"}})
	expectStatus(t, rec, http.StatusCreated)
	sess := decode[map[string]any](t, rec)
	id, secret := sess["session_id"].(string), sess["secret"].(string)

	// Nothing yet: a zero-wait poll returns an empty list immediately.
	rec = a.do(http.MethodGet, "/v1/listen/"+id+"/deliveries?wait=0s", nil)
	expectStatus(t, rec, http.StatusOK)
	if got := decode[pollResp](t, rec); len(got.Data) != 0 {
		t.Fatalf("unexpected deliveries %+v", got)
	}

	expectStatus(t, a.do(http.MethodPost, "/v1/events", map[string]any{"event_type": "order.paid", "payload": map[string]int{"n": 1}}), http.StatusAccepted)

	start := time.Now()
	rec = a.do(http.MethodGet, "/v1/listen/"+id+"/deliveries?wait=5s", nil)
	expectStatus(t, rec, http.StatusOK)
	got := decode[pollResp](t, rec)
	if len(got.Data) != 1 || time.Since(start) > 3*time.Second {
		t.Fatalf("poll = %+v after %v", got, time.Since(start))
	}
	d := got.Data[0]

	// The headers are a real, verifiable signature over the body.
	h := http.Header{}
	for k, v := range d.Headers {
		h.Set(k, v)
	}
	v, _ := webhook.NewVerifier(secret)
	if err := v.Verify(h, []byte(d.Body)); err != nil {
		t.Fatalf("listen delivery signature: %v", err)
	}

	// Report a 500: the normal retry policy applies.
	rec = a.do(http.MethodPost, "/v1/listen/"+id+"/results", []map[string]any{
		{"delivery_id": d.DeliveryID, "lease_token": d.LeaseToken, "status_code": 500, "duration_ms": 3},
	})
	expectStatus(t, rec, http.StatusOK)
	if decode[map[string]int](t, rec)["recorded"] != 1 {
		t.Fatal("result not recorded")
	}
	dl, _ := a.store.GetDelivery(t.Context(), d.DeliveryID)
	if dl.Status != store.DeliveryPending || dl.LastStatusCode == nil || *dl.LastStatusCode != 500 {
		t.Fatalf("after 500: %+v", dl)
	}

	// A stale lease token is ignored.
	rec = a.do(http.MethodPost, "/v1/listen/"+id+"/results", []map[string]any{
		{"delivery_id": d.DeliveryID, "lease_token": d.LeaseToken, "status_code": 200},
	})
	if decode[map[string]int](t, rec)["recorded"] != 0 {
		t.Fatal("stale result must not be recorded")
	}

	expectStatus(t, a.do(http.MethodDelete, "/v1/listen/"+id, nil), http.StatusNoContent)
	expectStatus(t, a.do(http.MethodGet, "/v1/listen/"+id+"/deliveries?wait=0s", nil), http.StatusNotFound)
	expectStatus(t, a.do(http.MethodGet, "/v1/listen/"+id+"/deliveries?wait=1h", nil), http.StatusBadRequest)
}

func TestListenCannotDeleteHTTPEndpoint(t *testing.T) {
	a := newTestAPI(t)
	ep := createEndpoint(t, a, "https://example.com/h")
	expectStatus(t, a.do(http.MethodDelete, "/v1/listen/"+ep.ID, nil), http.StatusNotFound)
	expectStatus(t, a.do(http.MethodGet, "/v1/endpoints/"+ep.ID, nil), http.StatusOK)
}
