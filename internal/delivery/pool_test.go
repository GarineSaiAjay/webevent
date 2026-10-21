package delivery_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/delivery"
	"github.com/GarineSaiAjay/webevent/internal/logging"
	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

// TestManyPoolsDeliverEachEventExactlyOnce runs three pools (think: three
// server replicas) against one database and checks that every event reaches
// the receiver exactly once and that concurrency limits hold.
func TestManyPoolsDeliverEachEventExactlyOnce(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	rcv, srv := newReceiver(t, http.StatusOK)
	rcv.delay = 5 * time.Millisecond
	if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}}); err != nil {
		t.Fatal(err)
	}
	const events = 300
	for i := 0; i < events; i++ {
		if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "load.test", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		p := newPool(t, st, poolConfig{concurrency: 8, poll: 20 * time.Millisecond})
		wg.Add(1)
		go func() { defer wg.Done(); p.Run(runCtx) }()
	}

	waitFor(t, 30*time.Second, "all deliveries to succeed", func() bool {
		var n int
		_ = st.Pool().QueryRow(ctx, `SELECT count(*) FROM deliveries WHERE status = 'succeeded'`).Scan(&n)
		return n == events
	})
	cancel()
	wg.Wait()

	seen := map[string]int{}
	rcv.mu.Lock()
	for _, req := range rcv.requests {
		seen[req.Header.Get("Webhook-Id")]++
	}
	maxSeen := rcv.maxSeen
	rcv.mu.Unlock()

	if len(seen) != events {
		t.Fatalf("receiver saw %d distinct events, want %d", len(seen), events)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event %s delivered %d times", id, n)
		}
	}
	if maxSeen > 24 {
		t.Fatalf("observed %d concurrent requests; limit is 3 pools x 8", maxSeen)
	}
	t.Logf("max concurrent requests observed: %d", maxSeen)
}

func TestConcurrentClaimsNeverOverlap(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	// A high max_in_flight so the per-endpoint cap does not limit this test.
	if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: "http://127.0.0.1:1", EventTypes: []string{"*"}, MaxInFlight: 1000}); err != nil {
		t.Fatal(err)
	}
	const total = 200
	for i := 0; i < total; i++ {
		if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	claimed := map[string]string{}
	remaining := func() int {
		mu.Lock()
		defer mu.Unlock()
		return total - len(claimed)
	}
	deadline := time.Now().Add(20 * time.Second)
	var wg sync.WaitGroup
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			// An empty claim can just mean another worker holds the endpoint
			// row right now (SKIP LOCKED), so keep going until all is claimed.
			for remaining() > 0 && time.Now().Before(deadline) {
				jobs, err := st.ClaimJobs(ctx, worker, 7, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, j := range jobs {
					if prev, dup := claimed[j.DeliveryID]; dup {
						t.Errorf("delivery %s claimed by %s and %s", j.DeliveryID, prev, worker)
					}
					claimed[j.DeliveryID] = worker
				}
				mu.Unlock()
			}
		}(string(rune('a' + w)))
	}
	wg.Wait()
	if len(claimed) != total {
		t.Fatalf("claimed %d deliveries, want %d", len(claimed), total)
	}
}

// TestCrashedWorkerIsRecovered simulates a worker that claims deliveries and
// dies without finishing them. Another pool's reaper must recover them.
func TestCrashedWorkerIsRecovered(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	rcv, srv := newReceiver(t, http.StatusOK)
	seed(t, st, srv.URL)

	// The "crashed" worker: claims with a short lease, then vanishes.
	jobs, err := st.ClaimJobs(ctx, "crashed-worker", 10, 300*time.Millisecond)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v %v", jobs, err)
	}
	if d := onlyDelivery(t, st); d.Status != store.DeliveryDelivering {
		t.Fatalf("status = %s, want delivering", d.Status)
	}

	// A healthy pool with a fast reaper and a policy whose retry delay is tiny.
	p := newPool(t, st, poolConfig{reap: 50 * time.Millisecond, poll: 20 * time.Millisecond})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { p.Run(runCtx); close(done) }()
	waitFor(t, 10*time.Second, "recovered delivery to succeed", func() bool {
		return onlyDelivery(t, st).Status == store.DeliverySucceeded
	})
	cancel()
	<-done

	d := onlyDelivery(t, st)
	if rcv.count() != 1 || d.AttemptCount != 2 {
		t.Fatalf("receiver saw %d, delivery %+v", rcv.count(), d)
	}
	attempts, _ := st.ListAttempts(ctx, d.ID)
	if len(attempts) != 2 || attempts[0].Error == nil || !strings.Contains(*attempts[0].Error, "lease expired") {
		t.Fatalf("history should show the expired lease first: %+v", attempts)
	}
}

// TestStalledWorkerCannotOverwrite: a worker whose lease expired must not be
// able to record a result over the new owner's state (fencing).
func TestStalledWorkerCannotOverwrite(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	seed(t, st, "http://127.0.0.1:1")

	stale, err := st.ClaimJobs(ctx, "slow", 1, 10*time.Millisecond)
	if err != nil || len(stale) != 1 {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if n, err := st.ReapExpiredLeases(ctx, 0, 100); err != nil || n != 1 {
		t.Fatalf("reap = %d, %v", n, err)
	}
	fresh, err := st.ClaimJobs(ctx, "fast", 1, time.Minute)
	if err != nil || len(fresh) != 1 {
		t.Fatal(err)
	}
	if fresh[0].LeaseToken == stale[0].LeaseToken {
		t.Fatal("new claim must get a new lease token")
	}

	// The slow worker finally finishes and tries to record "succeeded".
	rr, err := st.RecordAttempt(ctx, store.AttemptRecord{
		Job: stale[0], Result: store.AttemptResult{StatusCode: 200}, Outcome: store.OutcomeSucceeded,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rr.Applied {
		t.Fatal("stale lease holder must not change the delivery")
	}
	if d := onlyDelivery(t, st); d.Status != store.DeliveryDelivering {
		t.Fatalf("status = %s; the new owner still holds it", d.Status)
	}

	// The real owner records normally.
	rr, err = st.RecordAttempt(ctx, store.AttemptRecord{
		Job: fresh[0], Result: store.AttemptResult{StatusCode: 200}, Outcome: store.OutcomeSucceeded,
	})
	if err != nil || !rr.Applied {
		t.Fatalf("owner record: %+v %v", rr, err)
	}
}

func TestGracefulShutdownFinishesInFlight(t *testing.T) {
	st := newStore(t)
	rcv, srv := newReceiver(t, http.StatusOK)
	rcv.delay = 400 * time.Millisecond
	seed(t, st, srv.URL)

	p := newPool(t, st, poolConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()

	waitFor(t, 5*time.Second, "request to reach the receiver", func() bool { return rcv.count() == 1 })
	cancel() // SIGTERM arrives while the request is in flight

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pool did not stop")
	}
	if d := onlyDelivery(t, st); d.Status != store.DeliverySucceeded {
		t.Fatalf("in-flight delivery must complete during shutdown, status = %s", d.Status)
	}
}

func TestNotifyWakesIdlePool(t *testing.T) {
	db, _ := testutil.NewDBWithURL(t)
	st := store.New(db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rcv, srv := newReceiver(t, http.StatusOK)
	if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}}); err != nil {
		t.Fatal(err)
	}

	wake := delivery.Listen(ctx, db, logging.Discard())
	// Poll interval of an hour: only a notification can make this pool work.
	p := newPool(t, st, poolConfig{poll: time.Hour, wake: wake})
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	time.Sleep(200 * time.Millisecond) // let LISTEN start and the first claim pass

	start := time.Now()
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "notification-driven delivery", func() bool { return rcv.count() == 1 })
	t.Logf("delivered %v after enqueue", time.Since(start))
	cancel()
	<-done
}

func TestNewPoolValidates(t *testing.T) {
	if _, err := delivery.NewPool(delivery.PoolOptions{Logger: logging.Discard()}); err == nil {
		t.Fatal("expected error for zero concurrency")
	}
}
