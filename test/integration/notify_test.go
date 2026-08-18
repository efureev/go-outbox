//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
)

// The latency claim: a message written by a producer reaches the broker in
// milliseconds, not on the next poll tick.
//
// The poll interval here is deliberately far longer than the deadline, so the
// test can only pass through the notification path: on the tick alone, latency
// is whatever the interval is.
func TestNotifyDeliversWithoutWaitingForTheTick(t *testing.T) {
	f := newFixture(t)

	cfg := scaleConfig(t, f)
	cfg.Dispatch.NotifyEnabled = true
	cfg.Dispatch.NotifyChannel = f.Config.Dispatch.NotifyChannel
	cfg.Dispatch.NotifyDebounce = 20 * time.Millisecond
	cfg.Dispatch.NotifyJitter = 10 * time.Millisecond
	cfg.Dispatch.PollInterval = time.Hour // the tick must never be the reason

	router := newCountingRouter()
	pipeline := dispatch.New("local", f.Store, router, events.NewEmitter(nil, logging.Nop()),
		cfg, logging.Nop())

	listener := dispatch.NewListener(f.Pool, cfg.Dispatch, []dispatch.Waker{pipeline}, logging.Nop())

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go func() { _ = pipeline.Run(ctx) }()
	go func() { _ = listener.Run(ctx) }()

	// Let the listener take its connection and issue LISTEN, and let the
	// pipeline's first claim drain the empty table.
	time.Sleep(500 * time.Millisecond)

	start := time.Now()
	id := f.insert(t, "local", "notify.test", []byte(`{}`), nil)

	waitFor(t, 10*time.Second, func() bool {
		return f.row(t, id).Status == core.StatusSent
	}, "the message reached the broker")

	elapsed := time.Since(start)

	// Generous against the claim of "milliseconds", strict against a poll
	// interval of an hour: only the notification path can satisfy it.
	if elapsed > 2*time.Second {
		t.Errorf("delivery took %s; the notification path is not waking the pipeline", elapsed)
	}
	t.Logf("insert to delivery: %s", elapsed)
}

// With notifications disabled, the tick is the only path — and it still works.
// A deployment whose database role cannot create triggers has to keep running.
func TestPollingStillWorksWithNotificationsDisabled(t *testing.T) {
	f := newFixture(t)

	cfg := scaleConfig(t, f)
	cfg.Dispatch.NotifyEnabled = false
	cfg.Dispatch.PollInterval = 100 * time.Millisecond

	router := newCountingRouter()
	pipeline := dispatch.New("local", f.Store, router, events.NewEmitter(nil, logging.Nop()),
		cfg, logging.Nop())

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go func() { _ = pipeline.Run(ctx) }()

	id := f.insert(t, "local", "poll.test", []byte(`{}`), nil)

	waitFor(t, 10*time.Second, func() bool {
		return f.row(t, id).Status == core.StatusSent
	}, "the message was delivered by the poll loop")
}

// A burst written in one transaction produces a notification per row. The
// debounce window is what keeps that from becoming a claim per row.
func TestNotifyBurstIsCoalesced(t *testing.T) {
	f := newFixture(t)

	cfg := scaleConfig(t, f)
	cfg.Dispatch.NotifyEnabled = true
	cfg.Dispatch.NotifyChannel = f.Config.Dispatch.NotifyChannel
	cfg.Dispatch.NotifyDebounce = 100 * time.Millisecond
	cfg.Dispatch.NotifyJitter = 0
	cfg.Dispatch.PollInterval = time.Hour
	cfg.Dispatch.BatchSize = 500

	router := newCountingRouter()

	rec := &iterationRecorder{}
	pipeline := dispatch.New("local", f.Store, router, rec, cfg, logging.Nop())
	listener := dispatch.NewListener(f.Pool, cfg.Dispatch, []dispatch.Waker{pipeline}, logging.Nop())

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	go func() { _ = pipeline.Run(ctx) }()
	go func() { _ = listener.Run(ctx) }()

	time.Sleep(500 * time.Millisecond)

	const burst = 200
	for range burst {
		f.insert(t, "local", "burst.test", []byte(`{}`), nil)
	}

	waitFor(t, 20*time.Second, func() bool {
		return f.countByStatus(t, core.StatusSent) == burst
	}, "the burst was delivered")

	cancel()

	// Two hundred notifications must not become two hundred claims. A handful
	// of iterations is the point of the debounce window.
	if got := rec.count(); got > 20 {
		t.Errorf("%d iterations for a burst of %d rows; the debounce window is not coalescing",
			got, burst)
	} else {
		t.Logf("%d iterations for %d rows", got, burst)
	}
}

type iterationRecorder struct {
	mu   sync.Mutex
	seen []events.Iteration
}

func (r *iterationRecorder) Iteration(_ context.Context, ev events.Iteration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.seen = append(r.seen, ev)
}

func (r *iterationRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.seen)
}
