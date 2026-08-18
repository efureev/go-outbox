package core

import (
	"testing"
	"time"
)

func TestBackoffGrowsExponentially(t *testing.T) {
	p := BackoffPolicy{Base: time.Minute, Max: 24 * time.Hour}

	want := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute}
	for i, w := range want {
		attempts := i + 1
		if got := p.Next(attempts); got != w {
			t.Errorf("Next(%d) = %s, want %s", attempts, got, w)
		}
	}
}

// Without a ceiling, doubling a one-minute base passes a day by the eleventh
// attempt and a year not much later.
func TestBackoffIsCapped(t *testing.T) {
	p := BackoffPolicy{Base: time.Minute, Max: time.Hour}

	for attempts := 1; attempts <= 40; attempts++ {
		if got := p.Next(attempts); got > time.Hour {
			t.Fatalf("Next(%d) = %s, above the one-hour ceiling", attempts, got)
		}
	}
	if got := p.Next(20); got != time.Hour {
		t.Errorf("Next(20) = %s, want the ceiling of 1h", got)
	}
}

// A very large attempt count must not overflow into a negative or absurd delay.
func TestBackoffSurvivesAnAbsurdAttemptCount(t *testing.T) {
	p := BackoffPolicy{Base: time.Minute, Max: time.Hour}

	for _, attempts := range []int{0, -5, 1 << 20} {
		got := p.Next(attempts)
		if got < 0 || got > time.Hour {
			t.Errorf("Next(%d) = %s, want a sane delay within the ceiling", attempts, got)
		}
	}
}

// Jitter is what stops every message that failed while a broker was down from
// becoming due at the same instant when it comes back.
func TestBackoffJitterSpreadsTheDelay(t *testing.T) {
	p := BackoffPolicy{Base: time.Minute, Max: time.Hour, Jitter: 0.2}

	const (
		samples = 500
		nominal = time.Minute
		lo      = time.Duration(float64(nominal) * 0.8)
		hi      = time.Duration(float64(nominal) * 1.2)
	)

	seen := map[time.Duration]struct{}{}
	for range samples {
		got := p.Next(1)
		if got < lo || got > hi {
			t.Fatalf("Next(1) = %s, outside the +-20%% band [%s, %s]", got, lo, hi)
		}
		seen[got] = struct{}{}
	}

	// Distinct values are the whole point; a constant would satisfy the range
	// check above and defeat the purpose.
	if len(seen) < samples/2 {
		t.Errorf("only %d distinct delays across %d samples: the jitter is not spreading anything",
			len(seen), samples)
	}
}

func TestBackoffWithoutJitterIsDeterministic(t *testing.T) {
	p := BackoffPolicy{Base: 30 * time.Second, Max: time.Hour}

	first := p.Next(3)
	for range 10 {
		if got := p.Next(3); got != first {
			t.Fatalf("Next(3) returned %s then %s with no jitter configured", first, got)
		}
	}
}

func TestBackoffDefaultsAZeroBase(t *testing.T) {
	if got := (BackoffPolicy{}).Next(1); got <= 0 {
		t.Errorf("Next(1) = %s with a zero policy, want a positive fallback", got)
	}
}
