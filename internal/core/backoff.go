package core

import (
	"math"
	"math/rand/v2"
	"time"
)

// BackoffPolicy schedules the next attempt after a retryable failure.
//
// Two properties the previous version lacked:
//
//   - a ceiling. Unbounded doubling of a 60s base reaches days by the tenth
//     attempt, long after anyone would consider the message live.
//   - jitter. Without it, every message that failed while a broker was down
//     becomes due at the same instant when it comes back, and the recovered
//     broker is met with the entire backlog at once.
type BackoffPolicy struct {
	Base   time.Duration
	Max    time.Duration
	Jitter float64 // fraction of the delay, 0…1; 0.2 means ±20%
}

// Next returns the delay before attempt number attempts+1, where attempts is
// the number of attempts already made (so the first failure passes 1).
//
// The result is Base * 2^(attempts-1), capped at Max, then spread by Jitter.
// It is never negative and never exceeds Max by more than the jitter fraction.
func (p BackoffPolicy) Next(attempts int) time.Duration {
	base := p.Base
	if base <= 0 {
		base = time.Second
	}

	exp := attempts - 1
	if exp < 0 {
		exp = 0
	}

	delay := float64(base)
	// Cap the exponent before computing the power: 2^63 overflows float64's
	// exact range and, more to the point, a delay that large is meaningless.
	if exp > 62 {
		exp = 62
	}
	delay *= math.Pow(2, float64(exp))

	if p.Max > 0 && delay > float64(p.Max) {
		delay = float64(p.Max)
	}

	if p.Jitter > 0 {
		// rand.Float64() spans [0,1), so 2*x-1 spans [-1,1): the delay is
		// spread symmetrically around its nominal value.
		//nolint:gosec // spreading retries in time is not a security decision
		delay *= 1 + p.Jitter*(2*rand.Float64()-1)
	}

	if delay < 0 {
		return 0
	}

	return time.Duration(delay)
}
