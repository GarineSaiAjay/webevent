package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

// Run with: make bench   (or go test -run XXX -bench . ./internal/store)

func BenchmarkCreateEventFanOut5(b *testing.B) {
	st := store.New(testutil.NewDB(b))
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: fmt.Sprintf("https://e%d.example", i), EventTypes: []string{"*"}}); err != nil {
			b.Fatal(err)
		}
	}
	payload := json.RawMessage(`{"user_id":42,"email":"a@example.com","plan":"pro"}`)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "user.created", Payload: payload}); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkClaimAndRecord(b *testing.B) {
	st := store.New(testutil.NewDB(b))
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: fmt.Sprintf("https://e%d.example", i), EventTypes: []string{"*"}, MaxInFlight: 1000}); err != nil {
			b.Fatal(err)
		}
	}
	// 20 endpoints x (b.N/20 + 1) events = at least b.N deliveries.
	for i := 0; i < b.N/20+1; i++ {
		if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "x", Payload: json.RawMessage(`{}`)}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	done := 0
	for done < b.N {
		jobs, err := st.ClaimJobs(ctx, "bench", 32, time.Minute)
		if err != nil {
			b.Fatal(err)
		}
		if len(jobs) == 0 {
			b.Fatal("ran out of work")
		}
		for _, j := range jobs {
			if _, err := st.RecordAttempt(ctx, store.AttemptRecord{Job: j, Result: store.AttemptResult{StatusCode: 200}, Outcome: store.OutcomeSucceeded}); err != nil {
				b.Fatal(err)
			}
		}
		done += len(jobs)
	}
}
