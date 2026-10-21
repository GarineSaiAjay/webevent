package store

import (
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }

func TestAllocateSharesFairly(t *testing.T) {
	cands := []candidate{{allowance: 10}, {allowance: 10}, {allowance: 10}}
	got := allocate(10, cands)
	if got[0]+got[1]+got[2] != 10 {
		t.Fatalf("grants %v do not use all slots", got)
	}
	for _, g := range got {
		if g < 3 || g > 4 {
			t.Fatalf("unfair split %v", got)
		}
	}
}

func TestAllocateRespectsAllowances(t *testing.T) {
	cands := []candidate{{allowance: 1}, {allowance: 0}, {allowance: 50}}
	got := allocate(10, cands)
	if got[0] != 1 || got[1] != 0 || got[2] != 9 {
		t.Fatalf("grants = %v, want [1 0 9]", got)
	}
	if got := allocate(5, []candidate{{allowance: 2}}); got[0] != 2 {
		t.Fatalf("grants = %v, want [2]", got)
	}
	if got := allocate(0, []candidate{{allowance: 2}}); got[0] != 0 {
		t.Fatalf("no slots must grant nothing: %v", got)
	}
}

func TestAllowanceConcurrencyCap(t *testing.T) {
	c := candidate{maxInFlight: 5, inFlight: 3}
	if got := c.allowanceNow(); got != 2 {
		t.Fatalf("allowance = %d, want 2", got)
	}
	c.inFlight = 7 // can happen briefly after max_in_flight is lowered
	if got := c.allowanceNow(); got != 0 {
		t.Fatalf("allowance = %d, want 0", got)
	}
}

func TestTokenBucket(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Fresh bucket: full burst (= one second of traffic).
	c := candidate{maxInFlight: 100, ratePerSec: ptr(5), now: now}
	if got := c.allowanceNow(); got != 5 {
		t.Fatalf("fresh bucket allowance = %d, want 5", got)
	}
	if left := *c.tokensAfter(5); left != 0 {
		t.Fatalf("tokens after taking 5 = %v", left)
	}

	// Empty bucket refills at rate per second, capped at burst.
	c.tokens, c.tokensAt = ptr(0.0), ptr(now.Add(-400*time.Millisecond))
	if got := c.allowanceNow(); got != 2 { // 0.4s * 5/s = 2 tokens
		t.Fatalf("allowance after 400ms = %d, want 2", got)
	}
	c.tokensAt = ptr(now.Add(-time.Hour))
	if got := c.allowanceNow(); got != 5 {
		t.Fatalf("bucket must cap at burst, got %d", got)
	}

	// Unlimited endpoints are bounded only by max_in_flight, and store NULL.
	u := candidate{maxInFlight: 7}
	if got := u.allowanceNow(); got != 7 || u.tokensAfter(3) != nil {
		t.Fatalf("unlimited allowance = %d", got)
	}
}
