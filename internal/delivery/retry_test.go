package delivery

import (
	"net/http"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

func fixed(v float64) func() float64 { return func() float64 { return v } }

func TestBackoffGrowsAndCaps(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 50, BaseDelay: 10 * time.Second, MaxDelay: time.Hour, Rand: fixed(0.999999999)}
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second}
	for i, w := range want {
		if got := p.Backoff(i + 1).Round(time.Second); got != w {
			t.Errorf("Backoff(%d) = %v, want %v", i+1, got, w)
		}
	}
	if got := p.Backoff(30).Round(time.Second); got != time.Hour {
		t.Errorf("Backoff(30) = %v, want capped at 1h", got)
	}
	if got := p.Backoff(10_000); got > time.Hour || got <= 0 {
		t.Errorf("huge attempt numbers must saturate, got %v", got)
	}
}

func TestBackoffEqualJitterBounds(t *testing.T) {
	low := RetryPolicy{BaseDelay: 8 * time.Second, MaxDelay: time.Hour, Rand: fixed(0)}
	high := RetryPolicy{BaseDelay: 8 * time.Second, MaxDelay: time.Hour, Rand: fixed(0.9999999)}
	for attempt := 1; attempt <= 6; attempt++ {
		nominal := 8 * time.Second << (attempt - 1)
		if got := low.Backoff(attempt); got != nominal/2 {
			t.Errorf("attempt %d min = %v, want %v", attempt, got, nominal/2)
		}
		if got := high.Backoff(attempt); got > nominal || got < nominal-time.Millisecond {
			t.Errorf("attempt %d max = %v, want ~%v", attempt, got, nominal)
		}
	}
}

func TestBackoffIsActuallyRandom(t *testing.T) {
	p := DefaultRetryPolicy()
	seen := map[time.Duration]bool{}
	for i := 0; i < 50; i++ {
		seen[p.Backoff(5)] = true
	}
	if len(seen) < 10 {
		t.Fatalf("expected jittered delays, got only %d distinct values", len(seen))
	}
}

func TestDecide(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: time.Minute, Rand: fixed(0)}
	cases := []struct {
		name    string
		attempt int
		res     store.AttemptResult
		want    store.Outcome
		delay   time.Duration
	}{
		{"2xx", 1, store.AttemptResult{StatusCode: 204}, store.OutcomeSucceeded, 0},
		{"500 retries", 1, store.AttemptResult{StatusCode: 500, Error: "x"}, store.OutcomeRetry, 500 * time.Millisecond},
		{"404 retries", 2, store.AttemptResult{StatusCode: 404, Error: "x"}, store.OutcomeRetry, time.Second},
		{"network retries", 1, store.AttemptResult{Error: "connection refused"}, store.OutcomeRetry, 500 * time.Millisecond},
		{"exhausted", 3, store.AttemptResult{StatusCode: 500, Error: "x"}, store.OutcomeFailed, 0},
		{"410 is final", 1, store.AttemptResult{StatusCode: 410, Error: "x"}, store.OutcomeFailed, 0},
		{"permanent", 1, store.AttemptResult{Error: "bad url", Permanent: true}, store.OutcomeFailed, 0},
		{"retry-after honoured", 1, store.AttemptResult{StatusCode: 429, Error: "x", RetryAfter: 30 * time.Second}, store.OutcomeRetry, 30 * time.Second},
		{"retry-after capped", 1, store.AttemptResult{StatusCode: 503, Error: "x", RetryAfter: time.Hour}, store.OutcomeRetry, time.Minute},
		{"short retry-after ignored", 2, store.AttemptResult{StatusCode: 429, Error: "x", RetryAfter: time.Millisecond}, store.OutcomeRetry, time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := p.Decide(tc.attempt, tc.res)
			if d.Outcome != tc.want {
				t.Fatalf("outcome = %v, want %v (%s)", d.Outcome, tc.want, d.Reason)
			}
			if tc.want == store.OutcomeRetry && d.Delay != tc.delay {
				t.Fatalf("delay = %v, want %v", d.Delay, tc.delay)
			}
			if tc.want == store.OutcomeFailed && d.Reason == "" {
				t.Fatal("failed decisions must carry a reason")
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := map[string]time.Duration{
		"":     0,
		"120":  2 * time.Minute,
		"5":    5 * time.Second,
		"0":    0,
		"-3":   0,
		"soon": 0,
		now.Add(90 * time.Second).Format(http.TimeFormat): 90 * time.Second,
		now.Add(-time.Hour).Format(http.TimeFormat):       0,
	}
	for in, want := range cases {
		if got := parseRetryAfter(in, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}
