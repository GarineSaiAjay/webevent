package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

// killDelivery drives a delivery to the dead state through the public API,
// the same way a worker would after exhausting retries.
func killDelivery(t *testing.T, st *store.Store, ctx context.Context) store.Delivery {
	t.Helper()
	job, err := st.NextDueJob(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rr, err := st.RecordAttempt(ctx, store.AttemptRecord{
		Job:     job,
		Result:  store.AttemptResult{StatusCode: 500, Error: "boom", Duration: time.Millisecond},
		Outcome: store.OutcomeDead,
	})
	if err != nil || !rr.Applied {
		t.Fatalf("record: %+v %v", rr, err)
	}
	d, err := st.GetDelivery(ctx, job.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestReplayDelivery(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, st, "https://a.example/h", "*")
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}

	// A pending delivery cannot be replayed.
	list, _, _ := st.ListDeliveries(ctx, store.DeliveryFilter{}, store.Page{})
	if _, err := st.ReplayDelivery(ctx, list[0].ID); !errors.Is(err, store.ErrNotReplayable) {
		t.Fatalf("replay pending: err = %v, want ErrNotReplayable", err)
	}

	dead := killDelivery(t, st, ctx)
	if dead.Status != store.DeliveryDead || dead.CompletedAt == nil {
		t.Fatalf("dead = %+v", dead)
	}
	replayed, err := st.ReplayDelivery(ctx, dead.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Status != store.DeliveryPending || replayed.ReplayCount != 1 || replayed.CompletedAt != nil {
		t.Fatalf("replayed = %+v", replayed)
	}
	job, err := st.NextDueJob(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if job.AttemptNumber != 2 || job.RetryAttempt != 1 {
		t.Fatalf("job after replay: attempt %d retry %d, want 2/1", job.AttemptNumber, job.RetryAttempt)
	}

	if _, err := st.ReplayDelivery(ctx, "dlv_missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing: err = %v", err)
	}
}

func TestReplayRefusedForDeletedEndpoint(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	ep := mustEndpoint(t, st, "https://a.example/h", "*")
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	dead := killDelivery(t, st, ctx)
	if err := st.DeleteEndpoint(ctx, ep.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ReplayDelivery(ctx, dead.ID); !errors.Is(err, store.ErrNotReplayable) {
		t.Fatalf("err = %v, want ErrNotReplayable", err)
	}
}

func TestDeleteEndpointCancelsPending(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	ep := mustEndpoint(t, st, "https://a.example/h", "*")
	for i := 0; i < 3; i++ {
		if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.DeleteEndpoint(ctx, ep.ID); err != nil {
		t.Fatal(err)
	}
	list, _, _ := st.ListDeliveries(ctx, store.DeliveryFilter{EndpointID: ep.ID}, store.Page{})
	for _, d := range list {
		if d.Status != store.DeliveryCancelled || d.CompletedAt == nil {
			t.Fatalf("delivery not cancelled: %+v", d)
		}
	}
	if _, err := st.NextDueJob(ctx); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("cancelled deliveries must not be picked up")
	}
}

func TestBulkReplayInBatches(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	ep := mustEndpoint(t, st, "https://a.example/h", "*")
	for i := 0; i < 7; i++ {
		if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		killDelivery(t, st, ctx)
	}
	n, err := st.ReplayDeadForEndpoint(ctx, ep.ID, time.Now().Add(-time.Hour), time.Now().Add(time.Minute), 3)
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 {
		t.Fatalf("replayed %d, want 7", n)
	}
	pending, _, _ := st.ListDeliveries(ctx, store.DeliveryFilter{Status: store.DeliveryPending}, store.Page{})
	if len(pending) != 7 {
		t.Fatalf("pending = %d", len(pending))
	}

	// Window that excludes everything.
	n, err = st.ReplayDeadForEndpoint(ctx, ep.ID, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour), 3)
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if _, err := st.ReplayDeadForEndpoint(ctx, "ep_missing", time.Now(), time.Now(), 3); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing endpoint: %v", err)
	}
}
