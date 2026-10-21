//go:build e2e

package e2e

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/testutil"
	"github.com/GarineSaiAjay/webevent/pkg/client"
)

// TestFullLifecycle: a fresh database is migrated by the server on boot,
// events are delivered signed, failures are retried, dead letters can be
// replayed, and the process shuts down cleanly on SIGTERM.
func TestFullLifecycle(t *testing.T) {
	_, dbURL := testutil.NewEmptyDBWithURL(t)
	srv := startServer(t, dbURL)
	c := srv.client()
	rcv := newReceiver(t)

	ep, err := c.CreateEndpoint(ctx(t), client.CreateEndpointInput{URL: rcv.srv.URL, EventTypes: []string{"*"}})
	if err != nil {
		t.Fatal(err)
	}
	rcv.setSecret(t, ep.Secret)

	// The first 3 requests for each event fail: everything must still arrive.
	var mu sync.Mutex
	failures := map[string]int{}
	rcv.setHandler(func(w http.ResponseWriter, r *http.Request) bool {
		mu.Lock()
		defer mu.Unlock()
		id := r.Header.Get("Webhook-Id")
		if failures[id] < 3 {
			failures[id]++
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		return false
	})

	publish(t, c, 50, "life")
	waitFor(t, 60*time.Second, "50 deliveries", func() bool {
		distinct, _, _, _ := rcv.snapshot()
		return distinct == 50
	})
	distinct, total, dups, bad := rcv.snapshot()
	if distinct != 50 || dups != 0 || bad != 0 {
		t.Fatalf("distinct=%d total=%d duplicates=%d bad signatures=%d", distinct, total, dups, bad)
	}

	// Dead letter + replay: an endpoint that always fails, with 2 attempts max.
	failing := newReceiver(t)
	var attempts atomic.Int32
	failing.setHandler(func(w http.ResponseWriter, r *http.Request) bool {
		attempts.Add(1)
		if attempts.Load() <= 15 { // more than the retry budget
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		return false
	})
	ep2, _ := c.CreateEndpoint(ctx(t), client.CreateEndpointInput{URL: failing.srv.URL, EventTypes: []string{"dead.letter"}})
	failing.setSecret(t, ep2.Secret)
	res, err := c.CreateEvent(ctx(t), client.CreateEventInput{EventType: "dead.letter", Payload: map[string]bool{"x": true}})
	if err != nil {
		t.Fatal(err)
	}
	var deadID string
	waitFor(t, 120*time.Second, "delivery to die", func() bool {
		p, err := c.ListDeliveries(ctx(t), client.DeliveryFilter{EventID: res.ID, Status: "dead"}, client.ListOptions{})
		if err == nil && len(p.Data) == 1 {
			deadID = p.Data[0].ID
			return true
		}
		return false
	})
	if _, err := c.ReplayDelivery(ctx(t), deadID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 60*time.Second, "replayed delivery to succeed", func() bool {
		d, err := c.GetDelivery(ctx(t), deadID)
		return err == nil && d.Status == "succeeded"
	})

	if err := srv.stop(syscall.SIGTERM); err != nil {
		t.Fatalf("graceful shutdown returned %v; logs:\n%s", err, srv.logs.String())
	}
}

// TestCrashRecovery kills a server (SIGKILL: no cleanup at all) while its
// deliveries are in flight. A second server must recover them once the
// leases expire, and nothing may be lost.
func TestCrashRecovery(t *testing.T) {
	dbURL := newDatabase(t)
	rcv := newReceiver(t)

	// The receiver hangs while the first server is alive.
	var block atomic.Bool
	block.Store(true)
	release := make(chan struct{})
	rcv.setHandler(func(w http.ResponseWriter, r *http.Request) bool {
		if block.Load() {
			select {
			case <-release:
			case <-time.After(10 * time.Second):
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		return false
	})

	a := startServer(t, dbURL)
	ep, err := a.client().CreateEndpoint(ctx(t), client.CreateEndpointInput{URL: rcv.srv.URL, EventTypes: []string{"*"}})
	if err != nil {
		t.Fatal(err)
	}
	rcv.setSecret(t, ep.Secret)
	publish(t, a.client(), 5, "crash")

	waitFor(t, 10*time.Second, "deliveries to be in flight", func() bool {
		p, _ := a.client().ListDeliveries(ctx(t), client.DeliveryFilter{Status: "delivering"}, client.ListOptions{})
		return p != nil && len(p.Data) == 5
	})
	if err := a.cmd.Process.Kill(); err != nil { // SIGKILL
		t.Fatal(err)
	}
	<-a.done //nolint:errcheck // exit status is irrelevant here
	a.done <- nil
	block.Store(false)
	close(release)

	b := startServer(t, dbURL)
	start := time.Now()
	waitFor(t, 60*time.Second, "recovered deliveries", func() bool {
		distinct, _, _, _ := rcv.snapshot()
		return distinct == 5
	})
	t.Logf("recovered 5 in-flight deliveries %v after restart", time.Since(start).Round(time.Second))

	p, err := b.client().ListAttempts(ctx(t), client.AttemptFilter{}, client.ListOptions{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	expired := 0
	for _, at := range p.Data {
		if at.Error != nil && strings.Contains(*at.Error, "lease expired") {
			expired++
		}
	}
	if expired != 5 {
		t.Fatalf("want 5 lease-expired attempts in the log, got %d", expired)
	}
}

// TestRollingRestartLosesNothing publishes continuously while two server
// instances are restarted one after the other with SIGTERM, like a rolling
// deploy. Every event must be delivered, and graceful shutdown means no
// duplicates either.
func TestRollingRestartLosesNothing(t *testing.T) {
	dbURL := newDatabase(t)
	rcv := newReceiver(t)
	// Two "replicas". The publisher reads the slots while the test swaps
	// them, so each slot is an atomic pointer.
	var servers [2]atomic.Pointer[server]
	servers[0].Store(startServer(t, dbURL))
	servers[1].Store(startServer(t, dbURL))
	ep, err := servers[0].Load().client().CreateEndpoint(ctx(t), client.CreateEndpointInput{URL: rcv.srv.URL, EventTypes: []string{"*"}, MaxInFlight: intPtr(50)})
	if err != nil {
		t.Fatal(err)
	}
	rcv.setSecret(t, ep.Secret)

	var published atomic.Int32
	stop := make(chan struct{})
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			// Publish through whichever server is up; retry on the other one.
			for attempt := 0; attempt < 10; attempt++ {
				s := servers[(i+attempt)%2].Load()
				_, err := s.client().CreateEvent(ctx(t), client.CreateEventInput{
					EventType: "rolling", Payload: map[string]int{"i": i}, IdempotencyKey: "rolling-" + strconv.Itoa(i),
				})
				if err == nil {
					published.Add(1)
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	for i := range servers {
		time.Sleep(time.Second)
		if err := servers[i].Load().stop(syscall.SIGTERM); err != nil {
			t.Fatalf("server %d did not shut down cleanly: %v", i, err)
		}
		servers[i].Store(startServer(t, dbURL))
	}
	time.Sleep(time.Second)
	close(stop)
	<-pubDone

	want := int(published.Load())
	waitFor(t, 60*time.Second, "all published events to be delivered", func() bool {
		distinct, _, _, _ := rcv.snapshot()
		return distinct == want
	})
	distinct, total, dups, bad := rcv.snapshot()
	t.Logf("published=%d delivered=%d total requests=%d duplicates=%d", want, distinct, total, dups)
	if dups != 0 || bad != 0 {
		t.Fatalf("duplicates=%d bad signatures=%d during a graceful rolling restart", dups, bad)
	}
}

func intPtr(n int) *int { return &n }
