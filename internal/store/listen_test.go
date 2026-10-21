package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

func TestListenSessionLifecycle(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	sess, err := st.CreateListenSession(ctx, []string{"*"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if sess.Kind != store.KindListen || sess.Secret == "" {
		t.Fatalf("session = %+v", sess)
	}
	httpEP := mustEndpoint(t, st, "https://a.example/h", "*")
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}

	// Workers only ever claim the http endpoint's delivery.
	jobs, err := st.ClaimJobs(ctx, "worker", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].EndpointID != httpEP.ID {
		t.Fatalf("workers claimed %+v; listen deliveries must be left for the CLI", jobs)
	}

	// The CLI claims the listen delivery.
	lj, err := st.ClaimListenJobs(ctx, sess.ID, "listen", 10, time.Minute)
	if err != nil || len(lj) != 1 || lj[0].EndpointID != sess.ID || len(lj[0].Secrets) != 1 {
		t.Fatalf("listen claim = %+v, %v", lj, err)
	}
	loaded, err := st.LoadLeasedJob(ctx, sess.ID, lj[0].DeliveryID, lj[0].LeaseToken)
	if err != nil || loaded.AttemptNumber != 1 {
		t.Fatalf("LoadLeasedJob = %+v, %v", loaded, err)
	}
	if _, err := st.LoadLeasedJob(ctx, sess.ID, lj[0].DeliveryID, "wrong-token"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("wrong token: %v", err)
	}

	// Claiming for something that is not a listen session fails.
	if _, err := st.ClaimListenJobs(ctx, httpEP.ID, "listen", 10, time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("http endpoint as session: %v", err)
	}
	if err := st.TouchListenSession(ctx, httpEP.ID, time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("touch http endpoint: %v", err)
	}
}

func TestAbandonedListenSessionsExpire(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	sess, err := st.CreateListenSession(ctx, []string{"*"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if n, _ := st.ExpireListenSessions(ctx); n != 0 {
		t.Fatal("fresh session must not expire")
	}
	time.Sleep(1100 * time.Millisecond)
	if n, err := st.ExpireListenSessions(ctx); err != nil || n != 1 {
		t.Fatalf("expire = %d, %v", n, err)
	}
	if _, err := st.GetEndpoint(ctx, sess.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("expired session must be deleted")
	}
	d, _, _ := st.ListDeliveries(ctx, store.DeliveryFilter{EndpointID: sess.ID}, store.Page{})
	if len(d) != 1 || d[0].Status != store.DeliveryCancelled {
		t.Fatalf("pending delivery must be cancelled: %+v", d)
	}
}
