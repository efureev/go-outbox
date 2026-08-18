package dispatch

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
)

type fakeWaker struct {
	stream string
	woken  atomic.Int64
}

func (f *fakeWaker) Stream() string { return f.stream }
func (f *fakeWaker) Wake()          { f.woken.Add(1) }

func newListener(t *testing.T, debounce, jitter time.Duration, wakers ...Waker) *Listener {
	t.Helper()

	cfg := config.DispatchConfig{
		NotifyChannel:  "outbox_new",
		NotifyDebounce: debounce,
		NotifyJitter:   jitter,
	}

	return NewListener(nil, cfg, wakers, logging.Nop())
}

// A producer inserting a thousand rows in one transaction emits a thousand
// notifications. Claiming once per notification would run a thousand queries to
// collect what one claim returns.
func TestCoalesceGathersABurstIntoOneWakeup(t *testing.T) {
	l := newListener(t, 50*time.Millisecond, 0)

	var delivered atomic.Int64

	wait := func(ctx context.Context) (*pgconn.Notification, error) {
		// A burst of ten, then nothing until the window closes.
		if delivered.Add(1) <= 10 {
			return &pgconn.Notification{Payload: "local"}, nil
		}

		<-ctx.Done()

		return nil, ctx.Err()
	}

	streams := map[string]struct{}{"local": {}}
	l.coalesce(t.Context(), wait, streams)

	if len(streams) != 1 {
		t.Errorf("collected %d streams, want 1", len(streams))
	}
	if got := delivered.Load(); got < 10 {
		t.Errorf("the window closed after %d notifications, before the burst was drained", got)
	}
}

func TestCoalesceCollectsEveryStreamInTheWindow(t *testing.T) {
	l := newListener(t, 50*time.Millisecond, 0)

	payloads := []string{"global", "archive", "local"}
	var i atomic.Int64

	wait := func(ctx context.Context) (*pgconn.Notification, error) {
		n := i.Add(1)
		if int(n) <= len(payloads) {
			return &pgconn.Notification{Payload: payloads[n-1]}, nil
		}

		<-ctx.Done()

		return nil, ctx.Err()
	}

	streams := map[string]struct{}{"local": {}}
	l.coalesce(t.Context(), wait, streams)

	for _, want := range payloads {
		if _, ok := streams[want]; !ok {
			t.Errorf("stream %q was announced inside the window but not collected", want)
		}
	}
}

func TestCoalesceReturnsImmediatelyWithoutADebounce(t *testing.T) {
	l := newListener(t, 0, 0)

	called := false
	wait := func(context.Context) (*pgconn.Notification, error) {
		called = true

		return nil, nil
	}

	start := time.Now()
	l.coalesce(t.Context(), wait, map[string]struct{}{})

	if called {
		t.Error("a zero debounce must not wait for more notifications")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Errorf("returned after %s with debouncing off", elapsed)
	}
}

func TestWakeOnlyTouchesTheAnnouncedStreams(t *testing.T) {
	local := &fakeWaker{stream: "local"}
	global := &fakeWaker{stream: "global"}

	l := newListener(t, 0, 0, local, global)

	l.wake(t.Context(), map[string]struct{}{"local": {}})

	if local.woken.Load() != 1 {
		t.Errorf("the announced pipeline was woken %d times, want 1", local.woken.Load())
	}
	if global.woken.Load() != 0 {
		t.Errorf("an unannounced pipeline was woken %d times", global.woken.Load())
	}
}

func TestWakeIgnoresAStreamNobodyDispatches(t *testing.T) {
	local := &fakeWaker{stream: "local"}
	l := newListener(t, 0, 0, local)

	// A row written to a stream with no pipeline must not panic anything.
	l.wake(t.Context(), map[string]struct{}{"orphan": {}})

	if local.woken.Load() != 0 {
		t.Error("an unrelated pipeline was woken")
	}
}

// Every replica listening on the channel is woken by the same insert. The
// jitter is what keeps them from all claiming at the same millisecond.
func TestJitterSpreadsTheWakeup(t *testing.T) {
	l := newListener(t, 0, 20*time.Millisecond)

	var (
		mu     sync.Mutex
		delays []time.Duration
	)

	for range 20 {
		start := time.Now()
		l.sleepJitter(t.Context())

		mu.Lock()
		delays = append(delays, time.Since(start))
		mu.Unlock()
	}

	distinct := map[time.Duration]struct{}{}
	for _, d := range delays {
		if d > 40*time.Millisecond {
			t.Errorf("jitter slept %s, far beyond the configured 20ms", d)
		}
		distinct[d.Truncate(time.Millisecond)] = struct{}{}
	}

	if len(distinct) < 3 {
		t.Errorf("only %d distinct delays across 20 samples; replicas would still stampede", len(distinct))
	}
}

func TestJitterHonoursCancellation(t *testing.T) {
	l := newListener(t, 0, time.Hour)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	start := time.Now()
	l.sleepJitter(ctx)

	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("a canceled context still waited %s", elapsed)
	}
}
