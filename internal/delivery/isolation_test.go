package delivery_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

func enqueue(t *testing.T, st *store.Store, n int, eventType string) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := st.CreateEvent(context.Background(), store.NewEvent{EventType: eventType, Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
}

func countStatus(t *testing.T, st *store.Store, endpointID, status string) int {
	t.Helper()
	var n int
	if err := st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM deliveries WHERE endpoint_id = $1 AND status = $2`, endpointID, status).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestSlowEndpointDoesNotBlockOthers: one receiver takes a second per
// request and has a big backlog. A healthy endpoint must still get its
// deliveries through promptly.
func TestSlowEndpointDoesNotBlockOthers(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	slow, slowSrv := newReceiver(t)
	slow.delay = time.Second
	_, fastSrv := newReceiver(t)

	slowEP, _ := st.CreateEndpoint(ctx, store.NewEndpoint{URL: slowSrv.URL, EventTypes: []string{"slow.thing"}, MaxInFlight: 2})
	fastEP, _ := st.CreateEndpoint(ctx, store.NewEndpoint{URL: fastSrv.URL, EventTypes: []string{"fast.thing"}})
	enqueue(t, st, 50, "slow.thing") // created first: oldest work belongs to the slow endpoint
	enqueue(t, st, 20, "fast.thing")

	p := newPool(t, st, poolConfig{concurrency: 8, poll: 20 * time.Millisecond})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { p.Run(runCtx); close(done) }()

	start := time.Now()
	waitFor(t, 5*time.Second, "fast endpoint to be fully served", func() bool {
		return countStatus(t, st, fastEP.ID, store.DeliverySucceeded) == 20
	})
	elapsed := time.Since(start)
	cancel()
	<-done

	if elapsed > 2*time.Second {
		t.Fatalf("fast endpoint took %v; the slow endpoint is starving it", elapsed)
	}
	slow.mu.Lock()
	maxSlow := slow.maxSeen
	slow.mu.Unlock()
	if maxSlow > 2 {
		t.Fatalf("slow endpoint saw %d concurrent requests; max_in_flight is 2", maxSlow)
	}
	t.Logf("fast endpoint drained in %v while slow endpoint had %d done", elapsed, countStatus(t, st, slowEP.ID, store.DeliverySucceeded))
}

// TestMaxInFlightHoldsAcrossPools: the per-endpoint concurrency cap is
// enforced by row locks in Postgres, so it holds even with several pools.
func TestMaxInFlightHoldsAcrossPools(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	rcv, srv := newReceiver(t)
	rcv.delay = 50 * time.Millisecond
	ep, _ := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}, MaxInFlight: 3})
	enqueue(t, st, 60, "a.b")

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		p := newPool(t, st, poolConfig{concurrency: 10, poll: 5 * time.Millisecond})
		wg.Add(1)
		go func() { defer wg.Done(); p.Run(runCtx) }()
	}
	waitFor(t, 20*time.Second, "all deliveries", func() bool {
		return countStatus(t, st, ep.ID, store.DeliverySucceeded) == 60
	})
	cancel()
	wg.Wait()

	rcv.mu.Lock()
	defer rcv.mu.Unlock()
	if rcv.maxSeen > 3 {
		t.Fatalf("receiver saw %d concurrent requests; max_in_flight is 3", rcv.maxSeen)
	}
}

// TestRateLimitIsEnforced: 5 requests/second with a burst of 5.
func TestRateLimitIsEnforced(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	rcv, srv := newReceiver(t)
	rate := 5
	ep, _ := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}, RateLimitPerSec: &rate, MaxInFlight: 50})
	enqueue(t, st, 15, "a.b")

	p := newPool(t, st, poolConfig{concurrency: 20, poll: 10 * time.Millisecond})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { p.Run(runCtx); close(done) }()

	time.Sleep(1500 * time.Millisecond)
	inFirstWindow := rcv.count()
	waitFor(t, 10*time.Second, "all deliveries", func() bool {
		return countStatus(t, st, ep.ID, store.DeliverySucceeded) == 15
	})
	total := time.Since(time.Now().Add(-1500 * time.Millisecond))
	cancel()
	<-done

	// Burst of 5 plus 5/s for 1.5s = 12.5, allow a little scheduling slack.
	if inFirstWindow > 13 {
		t.Fatalf("%d requests in the first 1.5s; rate limit not enforced", inFirstWindow)
	}
	if inFirstWindow < 5 {
		t.Fatalf("only %d requests in the first 1.5s; the burst was not used", inFirstWindow)
	}
	t.Logf("first 1.5s: %d requests; all 15 done after ~%v", inFirstWindow, total.Round(100*time.Millisecond))
}

func TestFairnessAcrossEndpoints(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	var rcvs []*receiver
	for i := 0; i < 4; i++ {
		r, srv := newReceiver(t, http.StatusOK)
		rcvs = append(rcvs, r)
		if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}, MaxInFlight: 100}); err != nil {
			t.Fatal(err)
		}
	}
	enqueue(t, st, 10, "a.b") // fan-out: 10 deliveries per endpoint

	p := newPool(t, st, poolConfig{concurrency: 8})
	if n, err := p.RunOnce(ctx); err != nil || n != 8 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	for i, r := range rcvs {
		if c := r.count(); c != 2 {
			t.Fatalf("endpoint %d got %d of the first 8 slots, want 2 each", i, c)
		}
	}
}
