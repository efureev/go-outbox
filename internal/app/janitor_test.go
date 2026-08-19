package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/efureev/appmod/adapters/hubmod"
	"github.com/efureev/appmod/v4"
	"github.com/efureev/msghub/v3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
)

// janitorContext assembles the two things start needs from the registry, so the
// hook can be exercised without standing up the whole application.
func janitorContext(t *testing.T, withStore, withHub bool) *appmod.AppContext {
	t.Helper()

	registry := appmod.NewRegistry()

	if withStore {
		// The pool is lazy: nothing connects until a query runs, and this test
		// cancels before one does.
		pool, err := pgxpool.New(t.Context(), "postgres://nobody@127.0.0.1:1/none")
		if err != nil {
			t.Fatalf("build a pool: %v", err)
		}
		t.Cleanup(pool.Close)

		if err := appmod.Provide(registry, store.New(pool, validConfig(t).DB)); err != nil {
			t.Fatalf("provide the store: %v", err)
		}
	}

	if withHub {
		if err := hubmod.Provide(registry, msghub.New()); err != nil {
			t.Fatalf("provide the hub: %v", err)
		}
	}

	return &appmod.AppContext{Registry: registry, Logger: logging.Nop()}
}

// start reaches into the registry for two things, and a graph edited by hand can
// stop providing either. Failing to find one has to be an error the manager can
// act on, not a nil pointer at the first query.
func TestHousekeepingRefusesToStartWithoutWhatItNeeds(t *testing.T) {
	cases := []struct {
		name      string
		withStore bool
		withHub   bool
	}{
		{"no store", false, true},
		{"no hub", true, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := newJanitorModule(validConfig(t), logging.Nop())
			m.SetAppContext(janitorContext(t, c.withStore, c.withHub))

			if err := m.start(t.Context(), nil); err == nil {
				t.Error("start reported success with a dependency missing")
			}
			if m.cancel != nil {
				t.Error("a failed start left a loop running")
			}
		})
	}
}

// The whole hook, start to stop: the loop begins, and shutdown takes it down.
func TestHousekeepingStartsAndComesBackDown(t *testing.T) {
	cfg := validConfig(t)
	// Nothing should tick during the test; the database is not there.
	cfg.Janitor.ReclaimInterval = time.Hour
	cfg.Janitor.StatsInterval = time.Hour
	cfg.Janitor.RetentionInterval = time.Hour

	m := newJanitorModule(cfg, logging.Nop())
	m.SetAppContext(janitorContext(t, true, true))

	if err := m.start(t.Context(), nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	if m.cancel == nil {
		t.Fatal("start reported success without starting anything")
	}

	if err := m.stop(t.Context(), nil); err != nil {
		t.Errorf("stop: %v", err)
	}
}

// Housekeeping switched off must not merely skip the loop: it must leave the
// module in a state that survives shutdown. A stop that dereferenced a cancel
// nobody set would turn a configuration choice into a panic on the way out.
func TestDisabledHousekeepingStartsNothingAndStopsCleanly(t *testing.T) {
	cfg := validConfig(t)
	cfg.Janitor.Enabled = false

	m := newJanitorModule(cfg, logging.Nop())

	if err := m.start(t.Context(), nil); err != nil {
		t.Fatalf("start with housekeeping disabled: %v", err)
	}
	if m.cancel != nil {
		t.Error("a disabled janitor set up a cancellation, so something is running")
	}
	if err := m.stop(t.Context(), nil); err != nil {
		t.Errorf("stop after a disabled start: %v", err)
	}
}

// Stop waits for the loop rather than returning while it is still touching the
// database — the pool is closed behind it.
func TestStopWaitsForTheLoopToLeave(t *testing.T) {
	m := newJanitorModule(validConfig(t), logging.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	var left bool
	var mu sync.Mutex

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		<-ctx.Done()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		left = true
		mu.Unlock()
	}()

	if err := m.stop(t.Context(), nil); err != nil {
		t.Fatalf("stop: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !left {
		t.Error("stop returned while the loop was still running")
	}
}

// A loop that will not leave must not hold the process open past the shutdown
// deadline. The error is how the operator learns it was killed rather than
// having finished.
func TestAStuckLoopDoesNotHoldShutdownOpen(t *testing.T) {
	m := newJanitorModule(validConfig(t), logging.Nop())

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	m.cancel = func() {}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		<-release
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	err := m.stop(ctx, nil)
	if err == nil {
		t.Fatal("stop reported a clean shutdown while the loop was still running")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("stop returned %v, want the deadline", err)
	}
}
