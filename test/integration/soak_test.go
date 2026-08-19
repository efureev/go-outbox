//go:build integration && soak

package integration

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/core"
)

// The soak run: the resilience scenarios, but for as long as somebody is
// willing to wait and with messages arriving throughout.
//
// The ordinary resilience tests break one thing, observe, and heal. That
// establishes that each failure is handled; it does not establish that the
// dispatcher survives them happening repeatedly, overlapping, while work keeps
// arriving — which is the shape of a bad afternoon in production. Leases expire
// mid-outage, breakers open and close, deferred messages accumulate and drain,
// and the retention sweep runs through all of it.
//
// Behind its own build tag because it is not a test in the sense the others
// are: it takes as long as it is told to, and its purpose is to be run
// deliberately before trusting a release with somebody's money.
//
//	make soak SOAK=1h
//
// The assertion at the end is the only one that matters and it is the one the
// whole program exists for: every message that was committed is delivered, and
// none is lost.
func TestSoak(t *testing.T) {
	duration := soakDuration(t)

	r := newResilient(t, []string{"alpha", "beta"},
		// Production-shaped rather than test-shaped: the point is to run the
		// real timings for a long time, not to rush them.
		"OUTBOX_DISPATCH_POLL_INTERVAL=1s",
		"OUTBOX_DISPATCH_BACKOFF_BASE=2s",
		"OUTBOX_DISPATCH_BACKOFF_MAX=30s",
		"OUTBOX_DISPATCH_PAUSE_MAX=10s",
		"OUTBOX_DISPATCH_LEASE_TTL=15s",
	)
	r.Start(t)

	t.Logf("soaking for %s across %d streams", duration, len(r.Brokers))

	var (
		inserted atomic.Int64
		stop     = make(chan struct{})
		done     = make(chan struct{})
	)

	// The producer. Steady rather than fast: a soak is about time under load,
	// and saturating the machine would only measure the machine.
	go func() {
		defer close(done)

		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				n := inserted.Add(1)
				stream := "alpha"
				if n%2 == 0 {
					stream = "beta"
				}
				r.insert(t, stream, "soak.event", fmt.Appendf(nil, `{"n":%d}`, n), nil)
			}
		}
	}()

	chaos(t, r, duration, stop)

	<-done

	// Everything comes back, and then the backlog has to drain on its own. No
	// requeue, no restart: an outage inside the retry budget is not supposed to
	// need an operator.
	for _, p := range r.Brokers {
		p.Heal()
	}
	r.DB.Heal()

	total := int(inserted.Load())
	t.Logf("inserted %d messages, waiting for the backlog to drain", total)

	waitFor(t, 5*time.Minute, func() bool {
		return r.statusCount(t, core.StatusSent) == total
	}, "every message committed during the soak is delivered")

	if failed := r.statusCount(t, core.StatusFailed); failed != 0 {
		t.Errorf("%d messages ended up failed; every outage here was inside the retry budget", failed)
	}
	if pending := r.statusCount(t, core.StatusPending); pending != 0 {
		t.Errorf("%d messages are still pending after the drain", pending)
	}
}

// chaos breaks things on a rotation for the length of the run, then stops the
// producer. Overlapping failures are the point: one broker down while the other
// recovers, and the database going away underneath both.
func chaos(t *testing.T, r *resilient, duration time.Duration, stop chan struct{}) {
	t.Helper()

	deadline := time.Now().Add(duration)
	round := 0

	for time.Now().Before(deadline) {
		round++

		switch round % 4 {
		case 1:
			t.Logf("round %d: alpha's broker goes away", round)
			r.Brokers["alpha"].Break()
		case 2:
			t.Logf("round %d: beta's broker joins it, alpha comes back", round)
			r.Brokers["beta"].Break()
			r.Brokers["alpha"].Heal()
		case 3:
			t.Logf("round %d: the database goes away too", round)
			r.DB.Break()
		default:
			t.Logf("round %d: everything comes back", round)
			r.DB.Heal()
			r.Brokers["beta"].Heal()
		}

		select {
		case <-time.After(min(15*time.Second, time.Until(deadline))):
		case <-stop:
			return
		}
	}

	close(stop)
}

// soakDuration is how long to run for. The Makefile passes it; the default is
// short enough that running the target by accident is not an afternoon lost.
func soakDuration(t *testing.T) time.Duration {
	t.Helper()

	raw := os.Getenv("OUTBOX_SOAK_DURATION")
	if raw == "" {
		return time.Minute
	}

	d, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("OUTBOX_SOAK_DURATION is not a duration: %v", err)
	}

	return d
}
