package delivery_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
	headers  http.Header   // extra response headers
	delay    time.Duration // how long to take before answering
	inFlight int
	maxSeen  int // highest number of concurrent requests observed
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
		r.inFlight++
		r.maxSeen = max(r.maxSeen, r.inFlight)
		r.requests = append(r.requests, req)
		r.bodies = append(r.bodies, b)
		code := r.script[min(len(r.requests)-1, len(r.script)-1)]
		delay := r.delay
		for k, v := range r.headers {
			w.Header()[k] = v
		}
		r.mu.Unlock()

		time.Sleep(delay)

		r.mu.Lock()
		r.inFlight--
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

type poolConfig struct {
	policy       delivery.RetryPolicy
	disableAfter int
	concurrency  int
	lease        time.Duration
	poll         time.Duration
	reap         time.Duration
	wake         <-chan struct{}
}

func newPool(t *testing.T, st *store.Store, c poolConfig) *delivery.Pool {
	t.Helper()
	if c.policy.MaxAttempts == 0 {
		c.policy = fastPolicy(3)
	}
	if c.concurrency == 0 {
		c.concurrency = 4
	}
	if c.lease == 0 {
		c.lease = 30 * time.Second
	}
	if c.poll == 0 {
		c.poll = 10 * time.Millisecond
	}
	if c.reap == 0 {
		c.reap = time.Hour
	}
	p, err := delivery.NewPool(delivery.PoolOptions{
		Store:                st,
		Sender:               delivery.NewSender(delivery.SenderOptions{Timeout: 5 * time.Second, UserAgent: "webevent-test", AllowPrivateNetworks: true}),
		Policy:               c.policy,
		Logger:               logging.Discard(),
		Concurrency:          c.concurrency,
		PollInterval:         c.poll,
		LeaseDuration:        c.lease,
		ReapInterval:         c.reap,
		DisableEndpointAfter: c.disableAfter,
		Wake:                 c.wake,
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	return store.New(testutil.NewDB(t))
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

// drain keeps running batches until the single delivery reaches a final state.
func drain(t *testing.T, st *store.Store, p *delivery.Pool) store.Delivery {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := p.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if d := onlyDelivery(t, st); d.Status != store.DeliveryPending && d.Status != store.DeliveryDelivering {
			return d
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("delivery did not finish in time")
	return store.Delivery{}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
