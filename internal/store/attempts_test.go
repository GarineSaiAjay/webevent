package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

func recordN(t *testing.T, st *store.Store, ctx context.Context, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "log.test", Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		job := claimOne(t, st, ctx)
		if _, err := st.RecordAttempt(ctx, store.AttemptRecord{
			Job: job, Result: store.AttemptResult{StatusCode: 200}, Outcome: store.OutcomeSucceeded,
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // distinct millisecond timestamps keep ordering obvious
	}
}

func TestAttemptLogBrowseAndTail(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	ep := mustEndpoint(t, st, "https://a.example/h", "*")
	recordN(t, st, ctx, 5)

	// Browse: newest first, paginated.
	page1, next, err := st.ListAttemptLog(ctx, store.AttemptFilter{EndpointID: ep.ID}, store.Page{Limit: 3}, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 3 || next == "" || page1[0].EventType != "log.test" || page1[0].URL != ep.URL {
		t.Fatalf("page1 = %+v next=%q", page1, next)
	}
	page2, next2, _ := st.ListAttemptLog(ctx, store.AttemptFilter{}, store.Page{Limit: 3, Cursor: next}, "")
	if len(page2) != 2 || next2 != "" {
		t.Fatalf("page2 len=%d next=%q", len(page2), next2)
	}

	// Tail: everything after the oldest, oldest first.
	oldest := page2[len(page2)-1].ID
	tail, after, err := st.ListAttemptLog(ctx, store.AttemptFilter{}, store.Page{Limit: 100}, oldest)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail) != 4 || tail[0].ID <= oldest || after != tail[3].ID {
		t.Fatalf("tail = %d rows, after=%q", len(tail), after)
	}
	// Nothing new: cursor stays put.
	empty, same, _ := st.ListAttemptLog(ctx, store.AttemptFilter{}, store.Page{Limit: 100}, after)
	if len(empty) != 0 || same != after {
		t.Fatalf("empty tail = %d rows, cursor %q", len(empty), same)
	}
}
