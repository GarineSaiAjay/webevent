package delivery

import (
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

// RetryPolicy decides what happens after an attempt.
//
// Delays grow exponentially: BaseDelay * 2^(attempt-1), capped at MaxDelay.
// We then apply "equal jitter": the actual delay is uniformly random in
// [d/2, d]. Jitter matters: when a receiver comes back after an outage,
// thousands of deliveries that failed at the same moment would otherwise all
// retry at the same moment and knock it over again (the thundering herd).
// Equal jitter (rather than "full" jitter in [0, d]) guarantees we always
// wait at least half the nominal delay, so a struggling receiver gets real
// breathing room.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	// Rand returns a float in [0,1). Nil means math/rand/v2. Tests inject a
	// fixed function for determinism.
	Rand func() float64
}

// DefaultRetryPolicy retries for roughly a day: 15 attempts, 10s base, 6h cap.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 15, BaseDelay: 10 * time.Second, MaxDelay: 6 * time.Hour}
}

// Decision is the outcome of evaluating one attempt.
type Decision struct {
	Outcome store.Outcome
	Delay   time.Duration // only meaningful for store.OutcomeRetry
	Reason  string
}

// Decide classifies an attempt result. attempt is the 1-based number of the
// attempt that just finished.
func (p RetryPolicy) Decide(attempt int, res store.AttemptResult) Decision {
	if res.Error == "" && Successful(res.StatusCode) {
		return Decision{Outcome: store.OutcomeSucceeded}
	}
	if res.Permanent {
		return Decision{Outcome: store.OutcomeFailed, Reason: "permanent failure: " + res.Error}
	}
	// 410 Gone is the receiver telling us, explicitly, to stop.
	if res.StatusCode == http.StatusGone {
		return Decision{Outcome: store.OutcomeFailed, Reason: "receiver returned 410 Gone"}
	}
	if attempt >= p.MaxAttempts {
		return Decision{Outcome: store.OutcomeFailed, Reason: "gave up after " + strconv.Itoa(attempt) + " attempts"}
	}
	d := p.Backoff(attempt)
	// Respect Retry-After when the receiver asks for more time than we
	// planned (but never beyond our own cap).
	if res.RetryAfter > d {
		d = min(res.RetryAfter, p.MaxDelay)
	}
	return Decision{Outcome: store.OutcomeRetry, Delay: d}
}

// Backoff returns the jittered delay before attempt+1.
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// Compute in float64 so huge attempt numbers saturate instead of
	// overflowing int64.
	nominal := float64(p.BaseDelay) * math.Pow(2, float64(attempt-1))
	if nominal > float64(p.MaxDelay) || math.IsInf(nominal, 0) {
		nominal = float64(p.MaxDelay)
	}
	r := p.Rand
	if r == nil {
		r = rand.Float64
	}
	return time.Duration(nominal/2 + r()*nominal/2)
}

// parseRetryAfter understands both forms allowed by RFC 9110: delay-seconds
// ("120") and an HTTP-date. Unparseable or past values yield 0.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}
