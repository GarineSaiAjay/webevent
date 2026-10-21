package store_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

// TestClaimsProgressDuringHeavyIngest is a regression test for lock
// starvation found by load testing: fan-out used to take FOR SHARE locks on
// endpoint rows and claims took FOR UPDATE SKIP LOCKED, which conflict. With
// many concurrent publishers some publisher almost always held a share lock,
// so claimers skipped the endpoint and delivery stalled until ingest
// stopped. Fan-out now takes FOR KEY SHARE and claims FOR NO KEY UPDATE,
// which do not conflict.
func TestClaimsProgressDuringHeavyIngest(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: "https://a.example/h", EventTypes: []string{"*"}, MaxInFlight: 1000}); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var published atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`)}); err != nil {
					t.Error(err)
					return
				}
				published.Add(1)
			}
		}()
	}

	claimed := 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		jobs, err := st.ClaimJobs(ctx, "w", 50, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		claimed += len(jobs)
		for _, j := range jobs { // complete them so max_in_flight never caps us
			if _, err := st.RecordAttempt(ctx, store.AttemptRecord{Job: j, Result: store.AttemptResult{StatusCode: 200}, Outcome: store.OutcomeSucceeded}); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(stop)
	wg.Wait()

	t.Logf("published %d, claimed %d while publishing", published.Load(), claimed)
	// Before the fix this was exactly 0. A single claim loop competes with 8
	// publishers for CPU and I/O, so only require that delivery clearly
	// keeps moving.
	if claimed < 100 {
		t.Fatalf("claims starved during ingest: claimed %d of %d published", claimed, published.Load())
	}
}
