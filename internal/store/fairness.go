package store

import (
	"math"
	"time"
)

// candidate is an endpoint considered by one ClaimJobs call.
type candidate struct {
	endpointID  string
	maxInFlight int
	ratePerSec  *int     // nil = unlimited
	tokens      *float64 // nil = bucket never used (starts full)
	tokensAt    *time.Time
	now         time.Time // database clock
	inFlight    int

	allowance int // computed: how many it may start now
}

// maxCandidates bounds how many endpoints one claim looks at. Looking at a
// few more endpoints than free slots lets us skip rate-limited ones and
// still fill every slot.
func maxCandidates(limit int) int {
	return min(max(limit*2, 8), 200)
}

// burst is the token-bucket capacity: one second's worth of requests.
func (c candidate) burst() float64 {
	if c.ratePerSec == nil {
		return math.Inf(1)
	}
	return float64(max(*c.ratePerSec, 1))
}

// availableTokens refills the bucket for the time elapsed since it was last
// updated: tokens = min(burst, tokens + elapsed * rate).
func (c candidate) availableTokens() float64 {
	if c.ratePerSec == nil {
		return math.Inf(1)
	}
	if c.tokens == nil || c.tokensAt == nil {
		return c.burst()
	}
	elapsed := c.now.Sub(*c.tokensAt).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	return math.Min(c.burst(), *c.tokens+elapsed*float64(*c.ratePerSec))
}

// allowanceNow is how many deliveries this endpoint may start right now.
func (c candidate) allowanceNow() int {
	n := c.maxInFlight - c.inFlight
	if n <= 0 {
		return 0
	}
	if t := c.availableTokens(); !math.IsInf(t, 1) {
		n = min(n, int(math.Floor(t)))
	}
	return max(n, 0)
}

// tokensAfter is the bucket level to store after claiming `claimed` items.
// Unlimited endpoints store NULL.
func (c candidate) tokensAfter(claimed int) *float64 {
	if c.ratePerSec == nil {
		return nil
	}
	t := c.availableTokens() - float64(claimed)
	if t < 0 {
		t = 0
	}
	return &t
}

// allocate shares `slots` free workers between candidates, never giving one
// more than its allowance. It deals one slot at a time round-robin, so with
// 10 slots and 3 busy endpoints each gets 3 or 4, instead of the first one
// taking all 10. Candidates arrive in round-robin order (least recently
// served first), so leftover single slots also rotate between calls.
func allocate(slots int, cands []candidate) []int {
	grants := make([]int, len(cands))
	for slots > 0 {
		progressed := false
		for i := range cands {
			if slots == 0 {
				break
			}
			if grants[i] < cands[i].allowance {
				grants[i]++
				slots--
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	return grants
}
