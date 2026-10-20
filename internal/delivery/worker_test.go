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

// receiver is a fake webhook consumer that records what it receives.
type receiver struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	status   int
}

func newReceiver(t *testing.T, status int) (*receiver, *httptest.Server) {
	r := &receiver{status: status}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		b, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.requests = append(r.requests, req)
		r.bodies = append(r.bodies, b)
		code := r.status
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

func setup(t *testing.T) (*store.Store, *delivery.Worker) {
	t.Helper()
	st := store.New(testutil.NewDB(t))
	w := delivery.NewWorker(st, delivery.NewSender(5*time.Second, "webevent-test"), logging.Discard(), 10*time.Millisecond)
	return st, w
}

func TestWorkerDeliversEvent(t *testing.T) {
	st, w := setup(t)
	ctx := context.Background()
	rcv, srv := newReceiver(t, http.StatusOK)

	ep, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL + "/hook", EventTypes: []string{"*"}})
	if err != nil {
		t.Fatal(err)
	}
	res, err := st.CreateEvent(ctx, store.NewEvent{EventType: "user.created", Payload: json.RawMessage(`{"id":7}`)})
	if err != nil {
		t.Fatal(err)
	}

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
	if env.ID != res.Event.ID || env.Type != "user.created" || string(env.Data) != `{"id": 7}` && string(env.Data) != `{"id":7}` {
		t.Fatalf("envelope = %+v (data %s)", env, env.Data)
	}

	list, _, err := st.ListDeliveries(ctx, store.DeliveryFilter{EndpointID: ep.ID}, store.Page{})
	if err != nil {
		t.Fatal(err)
	}
	d := list[0]
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

	// Nothing left to do.
	did, err = w.ProcessOne(ctx)
	if err != nil || did {
		t.Fatalf("second ProcessOne = %v, %v; want idle", did, err)
	}
}

func TestWorkerRecordsFailure(t *testing.T) {
	st, w := setup(t)
	ctx := context.Background()
	_, srv := newReceiver(t, http.StatusInternalServerError)
	if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	list, _, _ := st.ListDeliveries(ctx, store.DeliveryFilter{}, store.Page{})
	d := list[0]
	if d.Status != store.DeliveryFailed || *d.LastStatusCode != 500 || d.LastError == nil {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestWorkerRecordsConnectionError(t *testing.T) {
	st, w := setup(t)
	ctx := context.Background()
	_, srv := newReceiver(t, http.StatusOK)
	url := srv.URL
	srv.Close() // nothing is listening any more

	if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: url, EventTypes: []string{"*"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.ProcessOne(ctx); err != nil {
		t.Fatal(err)
	}
	list, _, _ := st.ListDeliveries(ctx, store.DeliveryFilter{}, store.Page{})
	d := list[0]
	if d.Status != store.DeliveryFailed || d.LastStatusCode != nil || d.LastError == nil || !strings.Contains(*d.LastError, "connect") {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestPausedEndpointIsSkipped(t *testing.T) {
	st, w := setup(t)
	ctx := context.Background()
	rcv, srv := newReceiver(t, http.StatusOK)
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
	_, w := setup(t)
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
