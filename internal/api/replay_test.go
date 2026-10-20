package api_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

func deadDelivery(t *testing.T, a *testAPI) store.Delivery {
	t.Helper()
	ctx := context.Background()
	job, err := a.store.NextDueJob(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.store.RecordAttempt(ctx, store.AttemptRecord{
		Job: job, Result: store.AttemptResult{StatusCode: 500, Error: "x"}, Outcome: store.OutcomeDead,
	}); err != nil {
		t.Fatal(err)
	}
	d, _ := a.store.GetDelivery(ctx, job.DeliveryID)
	return d
}

func TestReplayDeliveryAPI(t *testing.T) {
	a := newTestAPI(t)
	createEndpoint(t, a, "https://example.com/h")
	expectStatus(t, a.do(http.MethodPost, "/v1/events", map[string]any{"event_type": "a.b", "payload": 1}), http.StatusAccepted)

	list := decode[struct {
		Data []store.Delivery `json:"data"`
	}](t, a.do(http.MethodGet, "/v1/deliveries?status=pending", nil))
	// Pending -> 409
	expectStatus(t, a.do(http.MethodPost, "/v1/deliveries/"+list.Data[0].ID+"/replay", nil), http.StatusConflict)

	d := deadDelivery(t, a)
	rec := a.do(http.MethodGet, "/v1/deliveries?status=dead", nil)
	expectStatus(t, rec, http.StatusOK)

	rec = a.do(http.MethodPost, "/v1/deliveries/"+d.ID+"/replay", nil)
	expectStatus(t, rec, http.StatusAccepted)
	if got := decode[store.Delivery](t, rec); got.Status != "pending" || got.ReplayCount != 1 {
		t.Fatalf("replayed = %+v", got)
	}
	expectStatus(t, a.do(http.MethodPost, "/v1/deliveries/dlv_nope/replay", nil), http.StatusNotFound)
}

func TestBulkReplayAPI(t *testing.T) {
	a := newTestAPI(t)
	ep := createEndpoint(t, a, "https://example.com/h")
	for i := 0; i < 3; i++ {
		expectStatus(t, a.do(http.MethodPost, "/v1/events", map[string]any{"event_type": "a.b", "payload": i}), http.StatusAccepted)
		deadDelivery(t, a)
	}

	rec := a.do(http.MethodPost, "/v1/endpoints/"+ep.ID+"/replay", nil)
	expectStatus(t, rec, http.StatusAccepted)
	if got := decode[map[string]any](t, rec); got["replayed"] != float64(3) {
		t.Fatalf("response = %v", got)
	}

	// Explicit empty window.
	rec = a.do(http.MethodPost, "/v1/endpoints/"+ep.ID+"/replay", map[string]any{
		"since": time.Now().Add(-48 * time.Hour), "until": time.Now().Add(-24 * time.Hour),
	})
	expectStatus(t, rec, http.StatusAccepted)
	if got := decode[map[string]any](t, rec); got["replayed"] != float64(0) {
		t.Fatalf("response = %v", got)
	}

	rec = a.do(http.MethodPost, "/v1/endpoints/"+ep.ID+"/replay", map[string]any{
		"since": time.Now(), "until": time.Now().Add(-time.Hour),
	})
	expectStatus(t, rec, http.StatusUnprocessableEntity)
	expectStatus(t, a.do(http.MethodPost, "/v1/endpoints/ep_00000000000000000000000000/replay", nil), http.StatusNotFound)
}

func TestCannotSetDisabledManually(t *testing.T) {
	a := newTestAPI(t)
	ep := createEndpoint(t, a, "https://example.com/h")
	expectStatus(t, a.do(http.MethodPatch, "/v1/endpoints/"+ep.ID, map[string]any{"status": "disabled"}), http.StatusUnprocessableEntity)
}
