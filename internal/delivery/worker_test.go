package delivery_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/delivery"
	"github.com/GarineSaiAjay/webevent/internal/logging"
	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

// receiver is a fake webhook consumer. It answers with the scripted status
// codes in order, then repeats the last one forever.
type receiver struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	script   []int
	headers  http.Header // extra response headers
}

func newReceiver(t *testing.T, script ...int) (*receiver, *httptest.Server) {
	t.Helper()
	if len(script) == 0 {
		script = []int{http.StatusOK}
	}
	r := &receiver{script: script, headers: http.Header{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		b, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.requests = append(r.requests, req)
		r.bodies = append(r.bodies, b)
		i := min(len(r.requests)-1, len(r.script)-1)
		code := r.script[i]
		for k, v := range r.headers {
			w.Header()[k] = v
		}
		r.mu.Unlock()
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return r, srv
}

func (r *receiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

// fastPolicy retries almost immediately so tests run quickly.
func fastPolicy(maxAttempts int) delivery.RetryPolicy {
	return delivery.RetryPolicy{MaxAttempts: maxAttempts, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond}
}

func setup(t *testing.T, policy delivery.RetryPolicy) (*store.Store, *delivery.Worker) {
	t.Helper()
	st := store.New(testutil.NewDB(t))
	w := delivery.NewWorker(st, delivery.NewSender(5*time.Second, "webevent-test"), policy, logging.Discard(), 10*time.Millisecond)
	return st, w
}

func seed(t *testing.T, st *store.Store, url string) store.CreateEventResult {
	t.Helper()
	ctx := context.Background()
	if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: url, EventTypes: []string{"*"}}); err != nil {
		t.Fatal(err)
	}
	res, err := st.CreateEvent(ctx, store.NewEvent{EventType: "user.created", Payload: json.RawMessage(`{"id":7}`)})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func onlyDelivery(t *testing.T, st *store.Store) store.Delivery {
	t.Helper()
	list, _, err := st.ListDeliveries(context.Background(), store.DeliveryFilter{}, store.Page{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("want 1 delivery, have %d", len(list))
	}
	return list[0]
}

// drain keeps processing until the delivery reaches a final state.
func drain(t *testing.T, st *store.Store, w *delivery.Worker) store.Delivery {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := w.ProcessOne(context.Background()); err != nil {
			t.Fatal(err)
		}
		if d := onlyDelivery(t, st); d.Status != store.DeliveryPending {
			return d
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("delivery did not finish in time")
	return store.Delivery{}
}

func TestWorkerDeliversEvent(t *testing.T) {
	st, w := setup(t, fastPolicy(3))
	ctx := context.Background()
	rcv, srv := newReceiver(t, http.StatusOK)
	res := seed(t, st, srv.URL+"/hook")

	did, err := w.ProcessOne(ctx)
	if err != nil || !did {
		t.Fatalf("ProcessOne = %v, %v", did, err)
	}
	if rcv.count() != 1 {
		t.Fatalf("receiver got %d requests", rcv.count())
	}
	req := rcv.requests[0]
	if req.Method != http.MethodPost || req.URL.Path != "/hook" {
		t.Fatalf("got %s %s", req.Method, req.URL.Path)
	}
	if req.Header.Get("Content-Type") != "application/json" || req.Header.Get("Webhook-Id") != res.Event.ID {
		t.Fatalf("headers: %v", req.Header)
	}
	var env delivery.Envelope
	if err := json.Unmarshal(rcv.bodies[0], &env); err != nil {
		t.Fatal(err)
	}
	if env.ID != res.Event.ID || env.Type != "user.created" || string(env.Data) != `{"id":7}` && string(env.Data) != `{"id": 7}` {
		t.Fatalf("envelope = %+v (data %s)", env, env.Data)
	}

	d := onlyDelivery(t, st)
	if d.Status != store.DeliverySucceeded || d.AttemptCount != 1 || d.CompletedAt == nil || *d.LastStatusCode != 200 {
		t.Fatalf("delivery state = %+v", d)
	}
	attempts, err := st.ListAttempts(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || *attempts[0].StatusCode != 200 || !strings.Contains(*attempts[0].ResponseBody, "ok") {
		t.Fatalf("attempts = %+v", attempts)
	}
	if did, err := w.ProcessOne(ctx); err != nil || did {
		t.Fatalf("second ProcessOne = %v, %v; want idle", did, err)
	}
}

func TestFailedAttemptIsRescheduled(t *testing.T) {
	policy := delivery.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Hour, MaxDelay: 2 * time.Hour}
	st, w := setup(t, policy)
	_, srv := newReceiver(t, http.StatusInternalServerError)
	seed(t, st, srv.URL)

	before := time.Now()
	if _, err := w.ProcessOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := onlyDelivery(t, st)
	if d.Status != store.DeliveryPending || d.AttemptCount != 1 || *d.LastStatusCode != 500 || d.CompletedAt != nil {
		t.Fatalf("delivery = %+v", d)
	}
	// Equal jitter on a 1h base: next attempt is 30-60 minutes out.
	wait := d.NextAttemptAt.Sub(before)
	if wait < 29*time.Minute || wait > 61*time.Minute {
		t.Fatalf("next attempt in %v, want 30-60m", wait)
	}
	// And it is not due yet, so the worker has nothing to do.
	if did, _ := w.ProcessOne(context.Background()); did {
		t.Fatal("retry must not run before next_attempt_at")
	}
}

func TestRetriesUntilSuccess(t *testing.T) {
	st, w := setup(t, fastPolicy(5))
	rcv, srv := newReceiver(t, 500, 502, 503, 200)
	seed(t, st, srv.URL)

	d := drain(t, st, w)
	if d.Status != store.DeliverySucceeded || d.AttemptCount != 4 || rcv.count() != 4 {
		t.Fatalf("delivery = %+v, receiver saw %d", d, rcv.count())
	}
	attempts, _ := st.ListAttempts(context.Background(), d.ID)
	codes := []int{}
	for i, a := range attempts {
		if a.AttemptNumber != i+1 {
			t.Fatalf("attempt numbers out of order: %+v", attempts)
		}
		codes = append(codes, *a.StatusCode)
	}
	if len(codes) != 4 || codes[0] != 500 || codes[3] != 200 {
		t.Fatalf("attempt codes = %v", codes)
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	st, w := setup(t, fastPolicy(3))
	rcv, srv := newReceiver(t, http.StatusServiceUnavailable)
	seed(t, st, srv.URL)

	d := drain(t, st, w)
	if d.Status != store.DeliveryFailed || d.AttemptCount != 3 || rcv.count() != 3 || d.CompletedAt == nil {
		t.Fatalf("delivery = %+v, receiver saw %d", d, rcv.count())
	}
}

func TestGoneStopsImmediately(t *testing.T) {
	st, w := setup(t, fastPolicy(10))
	rcv, srv := newReceiver(t, http.StatusGone)
	seed(t, st, srv.URL)

	d := drain(t, st, w)
	if d.Status != store.DeliveryFailed || d.AttemptCount != 1 || rcv.count() != 1 {
		t.Fatalf("410 must not be retried: %+v", d)
	}
}

func TestRetryAfterIsRespected(t *testing.T) {
	policy := delivery.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Hour}
	st, w := setup(t, policy)
	rcv, srv := newReceiver(t, http.StatusTooManyRequests)
	rcv.headers.Set("Retry-After", "120")
	seed(t, st, srv.URL)

	before := time.Now()
	if _, err := w.ProcessOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := onlyDelivery(t, st)
	if wait := d.NextAttemptAt.Sub(before); wait < 119*time.Second || wait > 125*time.Second {
		t.Fatalf("next attempt in %v, want ~120s from Retry-After", wait)
	}
}

func TestConnectionErrorIsRetried(t *testing.T) {
	st, w := setup(t, fastPolicy(2))
	_, srv := newReceiver(t)
	url := srv.URL
	srv.Close() // nothing is listening any more
	seed(t, st, url)

	d := drain(t, st, w)
	if d.Status != store.DeliveryFailed || d.AttemptCount != 2 || d.LastStatusCode != nil ||
		d.LastError == nil || !strings.Contains(*d.LastError, "connect") {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestPausedEndpointIsSkipped(t *testing.T) {
	st, w := setup(t, fastPolicy(3))
	ctx := context.Background()
	rcv, srv := newReceiver(t)
	ep, _ := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}})
	paused := store.EndpointPaused
	if _, err := st.UpdateEndpoint(ctx, ep.ID, store.EndpointPatch{Status: &paused}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if did, _ := w.ProcessOne(ctx); did {
		t.Fatal("paused endpoint must not receive deliveries")
	}
	active := store.EndpointActive
	if _, err := st.UpdateEndpoint(ctx, ep.ID, store.EndpointPatch{Status: &active}); err != nil {
		t.Fatal(err)
	}
	if did, _ := w.ProcessOne(ctx); !did || rcv.count() != 1 {
		t.Fatal("resumed endpoint should receive the queued delivery")
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	_, w := setup(t, fastPolicy(3))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop after cancel")
	}
}
