package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	return store.New(testutil.NewDB(t))
}

func mustEndpoint(t *testing.T, st *store.Store, url string, types ...string) store.Endpoint {
	t.Helper()
	ep, err := st.CreateEndpoint(context.Background(), store.NewEndpoint{URL: url, EventTypes: types})
	if err != nil {
		t.Fatal(err)
	}
	return ep
}

func TestCreateEventFansOutToSubscribers(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	all := mustEndpoint(t, st, "https://a.example/h", "*")
	users := mustEndpoint(t, st, "https://b.example/h", "user.created", "user.deleted")
	mustEndpoint(t, st, "https://c.example/h", "invoice.paid") // not subscribed
	paused := mustEndpoint(t, st, "https://d.example/h", "user.created")
	status := store.EndpointPaused
	if _, err := st.UpdateEndpoint(ctx, paused.ID, store.EndpointPatch{Status: &status}); err != nil {
		t.Fatal(err)
	}
	deleted := mustEndpoint(t, st, "https://e.example/h", "*")
	if err := st.DeleteEndpoint(ctx, deleted.ID); err != nil {
		t.Fatal(err)
	}

	res, err := st.CreateEvent(ctx, store.NewEvent{EventType: "user.created", Payload: json.RawMessage(`{"id":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	// all + users + paused (paused endpoints queue deliveries; they are just not sent).
	if res.DeliveryCount != 3 || res.Replayed {
		t.Fatalf("result = %+v, want 3 deliveries", res)
	}

	list, _, err := st.ListDeliveries(ctx, store.DeliveryFilter{EventID: res.Event.ID}, store.Page{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, d := range list {
		got[d.EndpointID] = true
		if d.Status != store.DeliveryPending || d.AttemptCount != 0 {
			t.Errorf("new delivery has state %+v", d)
		}
	}
	for _, want := range []string{all.ID, users.ID, paused.ID} {
		if !got[want] {
			t.Errorf("missing delivery for endpoint %s", want)
		}
	}
}

func TestCreateEventWithNoSubscribers(t *testing.T) {
	st := newStore(t)
	res, err := st.CreateEvent(context.Background(), store.NewEvent{EventType: "nobody.cares", Payload: json.RawMessage(`[]`)})
	if err != nil {
		t.Fatal(err)
	}
	if res.DeliveryCount != 0 || res.Event.ID == "" {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestIdempotencyKey(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, st, "https://a.example/h", "*")

	first, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{"x": 1}`), IdempotencyKey: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	// Same body with different whitespace is the same request.
	second, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{"x":1}`), IdempotencyKey: "k1"})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replayed || second.Event.ID != first.Event.ID || second.DeliveryCount != 1 {
		t.Fatalf("replay = %+v, want original event %s", second, first.Event.ID)
	}

	_, err = st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{"x":2}`), IdempotencyKey: "k1"})
	if !errors.Is(err, store.ErrIdempotencyMismatch) {
		t.Fatalf("err = %v, want ErrIdempotencyMismatch", err)
	}

	list, _, err := st.ListDeliveries(ctx, store.DeliveryFilter{}, store.Page{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("replays must not enqueue deliveries; have %d", len(list))
	}
}

func TestConcurrentIdempotentRequestsCreateOneEvent(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	mustEndpoint(t, st, "https://a.example/h", "*")

	const n = 20
	var wg sync.WaitGroup
	ids := make(chan string, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`), IdempotencyKey: "same"})
			if err != nil {
				errs <- err
				return
			}
			ids <- res.Event.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent create failed: %v", err)
	}
	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	if len(unique) != 1 {
		t.Fatalf("got %d distinct events, want 1", len(unique))
	}
	list, _, _ := st.ListDeliveries(ctx, store.DeliveryFilter{}, store.Page{Limit: 100})
	if len(list) != 1 {
		t.Fatalf("got %d deliveries, want 1", len(list))
	}
}

func TestListEventsFilterAndPaging(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		typ := "a.one"
		if i%2 == 1 {
			typ = "a.two"
		}
		if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: typ, Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	ones, next, err := st.ListEvents(ctx, "a.one", store.Page{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(ones) != 2 || next == "" {
		t.Fatalf("page 1: %d events, next=%q", len(ones), next)
	}
	rest, next, err := st.ListEvents(ctx, "a.one", store.Page{Limit: 2, Cursor: next})
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 1 || next != "" {
		t.Fatalf("page 2: %d events, next=%q", len(rest), next)
	}
}

func TestGetEventNotFound(t *testing.T) {
	st := newStore(t)
	if _, err := st.GetEvent(context.Background(), "evt_missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}
