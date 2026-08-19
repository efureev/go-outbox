package dispatch

import (
	"testing"
	"time"
)

var (
	unreachable = Result{Claimed: 5, Deferred: 5}
	delivering  = Result{Claimed: 5, Delivered: 5}
	empty       = Result{}
)

func TestBreakerOpensWhenTheBrokerCannotBeReached(t *testing.T) {
	b := newBreaker(time.Second, time.Minute)
	now := time.Unix(0, 0)

	if b.blocks(now) {
		t.Fatal("a fresh breaker holds claims back")
	}

	if changed := b.observe(now, unreachable); !changed {
		t.Error("opening was not reported, so nothing would log it or export it")
	}
	if !b.blocks(now.Add(999 * time.Millisecond)) {
		t.Error("claims are not held back during the pause")
	}
	if b.blocks(now.Add(time.Second)) {
		t.Error("claims are still held back after the pause elapsed")
	}
}

// A partial failure is not an outage. One message getting through proves the
// broker is there, and holding claims back would stall a stream over messages
// it is refusing individually.
func TestBreakerIgnoresPartialFailures(t *testing.T) {
	b := newBreaker(time.Second, time.Minute)
	now := time.Unix(0, 0)

	b.observe(now, Result{Claimed: 5, Delivered: 1, Deferred: 4})

	if b.blocks(now) {
		t.Error("claims were held back although a message got through")
	}
}

func TestBreakerDoublesThePauseUpToTheCeiling(t *testing.T) {
	b := newBreaker(time.Second, 4*time.Second)
	now := time.Unix(0, 0)

	for _, want := range []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second, 4 * time.Second,
	} {
		b.observe(now, unreachable)

		if got := b.wait(); got != want {
			t.Fatalf("pause = %s, want %s", got, want)
		}
	}
}

// Once a trial gets through, claiming resumes at once rather than waiting out
// the pause it had already reached.
func TestBreakerClosesWhenTheBrokerReturns(t *testing.T) {
	b := newBreaker(time.Second, time.Minute)
	now := time.Unix(0, 0)

	b.observe(now, unreachable)
	b.observe(now, unreachable)

	if changed := b.observe(now, delivering); !changed {
		t.Error("closing was not reported")
	}
	if b.blocks(now) {
		t.Error("claims are still held back after the broker answered")
	}
	if b.wait() != 0 {
		t.Errorf("the pause was not reset: %s", b.wait())
	}
}

// A trial that finds nothing due says nothing about the broker, but there is
// nothing being held back either. Staying open would leave a stale pause in
// front of the first message to arrive after a quiet period.
func TestBreakerClosesOnAnEmptyClaim(t *testing.T) {
	b := newBreaker(time.Second, time.Minute)
	now := time.Unix(0, 0)

	b.observe(now, unreachable)
	b.observe(now, empty)

	if b.blocks(now) {
		t.Error("claims are held back although nothing is waiting")
	}
}

// Repeated failures must not each count as a change, or every trial during an
// outage would log a fresh warning and re-export the same gauge.
func TestBreakerReportsOnlyTransitions(t *testing.T) {
	b := newBreaker(time.Second, time.Minute)
	now := time.Unix(0, 0)

	if !b.observe(now, unreachable) {
		t.Fatal("the first failure did not report a change")
	}
	if b.observe(now, unreachable) {
		t.Error("a second failure reported a change; it was already open")
	}
	if !b.observe(now, delivering) {
		t.Error("recovery did not report a change")
	}
	if b.observe(now, delivering) {
		t.Error("a second success reported a change; it was already closed")
	}
}

// The escape hatch, and what makes it possible to prove the breaker is the
// thing making a difference elsewhere.
func TestBreakerCeilingOfZeroDisablesIt(t *testing.T) {
	b := newBreaker(time.Second, 0)
	now := time.Unix(0, 0)

	if changed := b.observe(now, unreachable); changed {
		t.Error("a disabled breaker reported a change")
	}
	if b.blocks(now) {
		t.Error("a disabled breaker held claims back")
	}
}

// The first pause is defaulted rather than trusted. Zero would make the breaker
// open and immediately expire — it would report claims held back while holding
// nothing back, which is worse than not having one: the metric says the stream
// stopped and it did not.
func TestTheFirstPauseIsDefaultedButNotOverridden(t *testing.T) {
	cases := []struct {
		name  string
		first time.Duration
		want  time.Duration
	}{
		{"zero is defaulted", 0, time.Second},
		{"negative is defaulted", -time.Minute, time.Second},
		{"a configured value is kept", 250 * time.Millisecond, 250 * time.Millisecond},
		{"even one shorter than the default", time.Millisecond, time.Millisecond},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b := newBreaker(c.first, time.Hour)

			now := time.Now()
			if !b.observe(now, Result{Claimed: 1, Deferred: 1}) {
				t.Fatal("an unreachable broker did not open the breaker")
			}

			if got := b.wait(); got != c.want {
				t.Errorf("the first pause is %s, want %s", got, c.want)
			}
			// A pause that does not actually hold anything back is the failure
			// this defaulting exists to prevent.
			if !b.blocks(now) {
				t.Error("the breaker is open but blocks nothing")
			}
		})
	}
}
