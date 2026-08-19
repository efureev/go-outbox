package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
)

// endlessStore always has work, which is what an outage during ordinary
// production looks like: retries are rescheduled into the future, but new
// messages keep arriving and are due at once.
type endlessStore struct {
	mu     sync.Mutex
	claims int
}

func (s *endlessStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.claims
}

func (s *endlessStore) Claim(context.Context, string, int, core.Lease) ([]core.Message, error) {
	s.mu.Lock()
	s.claims++
	s.mu.Unlock()

	return batch(3, 0), nil
}

func (s *endlessStore) Ack(context.Context, []string, string) (store.AckResult, error) {
	return store.AckResult{}, nil
}

func (s *endlessStore) Nack(
	_ context.Context, outcomes []core.Outcome, _ string, _ core.RetryLimits,
) (store.NackResult, error) {
	var res store.NackResult
	for _, o := range outcomes {
		res.Outcomes = append(res.Outcomes, store.NackOutcome{
			ID: o.ID, Status: core.StatusPending, Deferred: o.Deferred,
		})
	}

	return res, nil
}

func (s *endlessStore) ReleaseLease(_ context.Context, ids []string, _ string) (int, error) {
	return len(ids), nil
}

// downRouter answers for a broker nobody can reach.
type downRouter struct{}

func (downRouter) DriverFor(string) (string, bool) { return "test-driver", true }

func (downRouter) Publish(_ context.Context, _ string, msgs []core.Message) []error {
	out := make([]error, len(msgs))
	for i := range out {
		out[i] = core.Unavailable("broker unreachable", errors.New("no connection"))
	}

	return out
}

// The point of the breaker, measured rather than asserted.
//
// Comparing the two settings rather than checking an absolute number keeps this
// from being a test of the machine it runs on: whatever the scheduler does, a
// loop that pauses must claim materially less often than one that does not.
func TestAnUnreachableBrokerStopsTheClaimLoop(t *testing.T) {
	const window = 300 * time.Millisecond

	run := func(pauseMax time.Duration) (int, *recorder) {
		st := &endlessStore{}
		rec := &recorder{}

		cfg := testConfig()
		cfg.Dispatch.PollInterval = 5 * time.Millisecond
		cfg.Dispatch.PauseMax = pauseMax

		ctx, cancel := context.WithTimeout(t.Context(), window)
		defer cancel()

		if err := New("local", st, downRouter{}, rec, cfg, logging.Nop()).Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}

		return st.count(), rec
	}

	unpaused, _ := run(0)
	paused, rec := run(100 * time.Millisecond)

	if unpaused < 10 {
		t.Fatalf("the unpaused loop only claimed %d times; the window is too short to compare against",
			unpaused)
	}
	if paused >= unpaused {
		t.Errorf("claims with the breaker on = %d, off = %d: it is not holding anything back",
			paused, unpaused)
	}
	if paused > unpaused/2 {
		t.Errorf("claims only fell from %d to %d; the pause is barely taking effect", unpaused, paused)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()

	if len(rec.breakers) == 0 {
		t.Fatal("no breaker event was emitted, so nothing would export that the stream is paused")
	}
	if first := rec.breakers[0]; !first.Paused || first.Stream != "local" {
		t.Errorf("first event = %+v, want the local stream paused", first)
	}
}

// A wake-up carries no information the breaker does not already have — that a
// message exists, which is exactly what cannot be delivered. Letting it through
// would undo the pause on every insert, which during an outage is the busiest
// signal there is.
func TestAWakeUpDoesNotBypassThePause(t *testing.T) {
	st := &endlessStore{}

	cfg := testConfig()
	cfg.Dispatch.PollInterval = time.Hour
	cfg.Dispatch.PauseMax = time.Hour

	p := New("local", st, downRouter{}, &recorder{}, cfg, logging.Nop())

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = p.Run(ctx)
	}()

	// Wait for the first cycle, which is what opens the breaker.
	deadline := time.Now().Add(2 * time.Second)
	for st.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	for range 50 {
		p.Wake()
	}
	time.Sleep(50 * time.Millisecond)

	got := st.count()
	cancel()
	<-done

	if got != 1 {
		t.Errorf("claims = %d, want 1: fifty wake-ups pushed past the pause", got)
	}
}

// The breaker must not swallow the shutdown: a paused pipeline still has to
// return when its context is cancelled.
func TestAPausedPipelineStillStops(t *testing.T) {
	cfg := testConfig()
	cfg.Dispatch.PollInterval = 5 * time.Millisecond
	cfg.Dispatch.PauseMax = time.Hour

	p := New("local", &endlessStore{}, downRouter{}, &recorder{}, cfg, logging.Nop())

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	stopped := make(chan error, 1)
	go func() { stopped <- p.Run(ctx) }()

	select {
	case err := <-stopped:
		if err != nil {
			t.Errorf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a paused pipeline did not stop when its context was cancelled")
	}
}

var _ Emitter = (*recorder)(nil)
