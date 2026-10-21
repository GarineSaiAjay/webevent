package delivery_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/delivery"
	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/pkg/webhook"
)

func TestDeliversEvent(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{})
	ctx := context.Background()
	rcv, srv := newReceiver(t, http.StatusOK)
	res := seed(t, st, srv.URL+"/hook")

	n, err := p.RunOnce(ctx)
	if err != nil || n != 1 {
		t.Fatalf("RunOnce = %d, %v", n, err)
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
	if env.ID != res.Event.ID || env.Type != "user.created" || string(env.Data) != `{"id":7}` {
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
	if n, err := p.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("second RunOnce = %d, %v; want idle", n, err)
	}
}

func TestFailedAttemptIsRescheduled(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{policy: delivery.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Hour, MaxDelay: 2 * time.Hour}})
	_, srv := newReceiver(t, http.StatusInternalServerError)
	seed(t, st, srv.URL)

	before := time.Now()
	if _, err := p.RunOnce(context.Background()); err != nil {
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
	if n, _ := p.RunOnce(context.Background()); n != 0 {
		t.Fatal("retry must not run before next_attempt_at")
	}
}

func TestRetriesUntilSuccess(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{policy: fastPolicy(5)})
	rcv, srv := newReceiver(t, 500, 502, 503, 200)
	seed(t, st, srv.URL)

	d := drain(t, st, p)
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
	st := newStore(t)
	p := newPool(t, st, poolConfig{policy: fastPolicy(3)})
	rcv, srv := newReceiver(t, http.StatusServiceUnavailable)
	seed(t, st, srv.URL)

	d := drain(t, st, p)
	if d.Status != store.DeliveryDead || d.AttemptCount != 3 || rcv.count() != 3 || d.CompletedAt == nil {
		t.Fatalf("delivery = %+v, receiver saw %d", d, rcv.count())
	}
}

func TestGoneStopsImmediately(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{policy: fastPolicy(10)})
	rcv, srv := newReceiver(t, http.StatusGone)
	seed(t, st, srv.URL)

	d := drain(t, st, p)
	if d.Status != store.DeliveryDead || d.AttemptCount != 1 || rcv.count() != 1 {
		t.Fatalf("410 must not be retried: %+v", d)
	}
}

func TestRetryAfterIsRespected(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{policy: delivery.RetryPolicy{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Hour}})
	rcv, srv := newReceiver(t, http.StatusTooManyRequests)
	rcv.headers.Set("Retry-After", "120")
	seed(t, st, srv.URL)

	before := time.Now()
	if _, err := p.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := onlyDelivery(t, st)
	if wait := d.NextAttemptAt.Sub(before); wait < 119*time.Second || wait > 125*time.Second {
		t.Fatalf("next attempt in %v, want ~120s from Retry-After", wait)
	}
}

func TestConnectionErrorIsRetried(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{policy: fastPolicy(2)})
	_, srv := newReceiver(t)
	url := srv.URL
	srv.Close() // nothing is listening any more
	seed(t, st, url)

	d := drain(t, st, p)
	if d.Status != store.DeliveryDead || d.AttemptCount != 2 || d.LastStatusCode != nil ||
		d.LastError == nil || !strings.Contains(*d.LastError, "connect") {
		t.Fatalf("delivery = %+v", d)
	}
}

func TestPausedEndpointIsSkipped(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{})
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
	if n, _ := p.RunOnce(ctx); n != 0 {
		t.Fatal("paused endpoint must not receive deliveries")
	}
	active := store.EndpointActive
	if _, err := st.UpdateEndpoint(ctx, ep.ID, store.EndpointPatch{Status: &active}); err != nil {
		t.Fatal(err)
	}
	if n, _ := p.RunOnce(ctx); n != 1 || rcv.count() != 1 {
		t.Fatal("resumed endpoint should receive the queued delivery")
	}
}

func TestReplayRestartsRetryBudget(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{policy: fastPolicy(2)})
	ctx := context.Background()
	rcv, srv := newReceiver(t, 500, 500, 200)
	seed(t, st, srv.URL)

	dead := drain(t, st, p)
	if dead.Status != store.DeliveryDead || dead.AttemptCount != 2 {
		t.Fatalf("expected dead after 2 attempts, got %+v", dead)
	}
	if _, err := st.ReplayDelivery(ctx, dead.ID); err != nil {
		t.Fatal(err)
	}
	d := drain(t, st, p)
	if d.Status != store.DeliverySucceeded || d.AttemptCount != 3 || d.ReplayCount != 1 || rcv.count() != 3 {
		t.Fatalf("after replay: %+v (receiver saw %d)", d, rcv.count())
	}
	attempts, _ := st.ListAttempts(ctx, d.ID)
	if len(attempts) != 3 || attempts[2].AttemptNumber != 3 {
		t.Fatalf("attempt log must continue numbering across replays: %+v", attempts)
	}
}

func TestEndpointAutoDisabled(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{policy: fastPolicy(1), disableAfter: 2, concurrency: 1})
	ctx := context.Background()
	rcv, srv := newReceiver(t, http.StatusInternalServerError)
	ep, _ := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}})
	for i := 0; i < 3; i++ {
		if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		if _, err := p.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
	}
	got, err := st.GetEndpoint(ctx, ep.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != store.EndpointDisabled || got.DisabledReason == nil || got.ConsecutiveFailures != 2 {
		t.Fatalf("endpoint = %+v", got)
	}
	if rcv.count() != 2 {
		t.Fatalf("receiver saw %d requests, want 2", rcv.count())
	}
	pending, _, _ := st.ListDeliveries(ctx, store.DeliveryFilter{Status: store.DeliveryPending}, store.Page{})
	if len(pending) != 1 {
		t.Fatalf("want 1 parked delivery, got %d", len(pending))
	}

	res, _ := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)})
	if res.DeliveryCount != 0 {
		t.Fatal("disabled endpoint must not receive new deliveries")
	}

	active := store.EndpointActive
	got, err = st.UpdateEndpoint(ctx, ep.ID, store.EndpointPatch{Status: &active})
	if err != nil {
		t.Fatal(err)
	}
	if got.ConsecutiveFailures != 0 || got.DisabledReason != nil {
		t.Fatalf("re-enabled endpoint = %+v", got)
	}
	if n, _ := p.RunOnce(ctx); n != 1 {
		t.Fatal("parked delivery should run after re-enabling")
	}
}

func TestSuccessResetsFailureCounter(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{policy: fastPolicy(1), disableAfter: 5})
	ctx := context.Background()
	_, srv := newReceiver(t, 500, 200)
	ep, _ := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}})
	for i := 0; i < 2; i++ {
		if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		if _, err := p.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		got, _ := st.GetEndpoint(ctx, ep.ID)
		if want := 1 - i; got.ConsecutiveFailures != want {
			t.Fatalf("after delivery %d: consecutive_failures = %d, want %d", i+1, got.ConsecutiveFailures, want)
		}
	}
}

func TestDeleteDuringFlightIsNotOverwritten(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{})
	ctx := context.Background()

	var epID string
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		// The endpoint is deleted while its request is in flight.
		if err := st.DeleteEndpoint(context.Background(), epID); err != nil {
			t.Errorf("delete: %v", err)
		}
		rw.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	ep, _ := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}})
	epID = ep.ID
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	d := onlyDelivery(t, st)
	if d.Status != store.DeliveryCancelled {
		t.Fatalf("status = %s, want cancelled to stick", d.Status)
	}
	attempts, _ := st.ListAttempts(ctx, d.ID)
	if len(attempts) != 1 {
		t.Fatal("the attempt must still be logged")
	}
}

func TestRotationSignsWithBothSecrets(t *testing.T) {
	st := newStore(t)
	p := newPool(t, st, poolConfig{})
	ctx := context.Background()
	rcv, srv := newReceiver(t)
	ep, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}})
	if err != nil {
		t.Fatal(err)
	}
	oldSecret := ep.Secret
	newSecret, err := st.RotateEndpointSecret(ctx, ep.ID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	for name, secret := range map[string]string{"old": oldSecret, "new": newSecret} {
		v, _ := webhook.NewVerifier(secret)
		if err := v.Verify(rcv.requests[0].Header, rcv.bodies[0]); err != nil {
			t.Errorf("%s secret does not verify during grace period: %v", name, err)
		}
	}

	// Rotating again with no grace period revokes the old one immediately.
	newest, _ := st.RotateEndpointSecret(ctx, ep.ID, 0)
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if sigs := strings.Fields(rcv.requests[1].Header.Get(webhook.HeaderSignature)); len(sigs) != 1 {
		t.Fatalf("want exactly one signature after immediate rotation, got %d", len(sigs))
	}
	v, _ := webhook.NewVerifier(newest)
	if err := v.Verify(rcv.requests[1].Header, rcv.bodies[1]); err != nil {
		t.Fatal(err)
	}
}
