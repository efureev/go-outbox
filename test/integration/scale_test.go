//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
)

// countingRouter stands in for a broker and records exactly which message ids
// it was asked to publish, and how often.
//
// The assertion this enables is the one that matters for running several
// replicas: no message may be published twice while its lease is live, and none
// may be lost.
type countingRouter struct {
	mu       sync.Mutex
	attempts map[string]int
}

func newCountingRouter() *countingRouter {
	return &countingRouter{attempts: map[string]int{}}
}

func (r *countingRouter) Publish(_ context.Context, _ string, msgs []core.Message) []error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, m := range msgs {
		r.attempts[m.ID]++
	}

	return make([]error, len(msgs))
}

func (r *countingRouter) DriverFor(string) (string, bool) { return "rmq", true }

func (r *countingRouter) snapshot() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make(map[string]int, len(r.attempts))
	for k, v := range r.attempts {
		out[k] = v
	}

	return out
}

// Three replicas against one table must divide the work between them: every
// message delivered, none delivered twice, none left behind.
func TestThreeReplicasDivideTheWorkWithoutDuplicating(t *testing.T) {
	f := newFixture(t)

	const (
		total    = 600
		replicas = 3
	)

	for range total {
		f.insert(t, "local", "scale.test", []byte(`{}`), nil)
	}

	router := newCountingRouter()

	cfg := scaleConfig(t, f)

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := range replicas {
		instance := cfg
		instance.App.Instance = replicaName(i)

		p := dispatch.New("local", f.Store, router, events.NewEmitter(nil, logging.Nop()),
			instance, logging.Nop())

		wg.Add(1)

		go func() {
			defer wg.Done()

			_ = p.Run(ctx)
		}()
	}

	waitFor(t, 60*time.Second, func() bool {
		return f.countByStatus(t, core.StatusSent) == total
	}, "every message delivered")

	cancel()
	wg.Wait()

	published := router.snapshot()

	if len(published) != total {
		t.Errorf("%d distinct messages reached the broker, want %d", len(published), total)
	}

	duplicates := 0
	for id, n := range published {
		if n > 1 {
			duplicates++
			if duplicates <= 3 {
				t.Errorf("message %s was published %d times", id, n)
			}
		}
	}
	if duplicates > 0 {
		t.Errorf("%d messages were published more than once", duplicates)
	}

	if left := f.countByStatus(t, core.StatusPending); left != 0 {
		t.Errorf("%d messages left pending", left)
	}
	if stuck := f.countByStatus(t, core.StatusProcessing); stuck != 0 {
		t.Errorf("%d messages left stuck in processing", stuck)
	}
}

// Every replica owns the rows it claimed, and an operator can see which one.
func TestClaimsRecordTheOwningReplica(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 4)

	l := lease("replica-7", time.Minute)
	if _, err := f.Store.Claim(t.Context(), "local", 10, l); err != nil {
		t.Fatalf("claim: %v", err)
	}

	var owners []string
	rows, err := f.Pool.Query(t.Context(),
		`SELECT DISTINCT owner FROM `+quoted(f.Schema)+`.messages WHERE status = 1`)
	if err != nil {
		t.Fatalf("read owners: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var owner string
		if err := rows.Scan(&owner); err != nil {
			t.Fatalf("scan: %v", err)
		}
		owners = append(owners, owner)
	}

	if len(owners) != 1 || owners[0] != "replica-7" {
		t.Errorf("owners = %v, want just replica-7", owners)
	}
}

// A replica that dies mid-batch must not strand its claims. The janitor returns
// them, and another replica delivers them.
func TestWorkOfADeadReplicaIsRecoveredByAnother(t *testing.T) {
	f := newFixture(t)

	const total = 20
	for range total {
		f.insert(t, "local", "recovery.test", []byte(`{}`), nil)
	}

	// The doomed replica claims everything and then stops existing.
	dead := lease("doomed", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", total, dead)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != total {
		t.Fatalf("claimed %d, want %d", len(claimed), total)
	}
	f.expire(t, ids(claimed)...)

	router := newCountingRouter()
	cfg := scaleConfig(t, f)

	janitor := dispatch.NewJanitor(f.Store, events.NewEmitter(nil, logging.Nop()), cfg, logging.Nop())
	janitor.ReclaimExpired(t.Context())

	survivor := dispatch.New("local", f.Store, router, events.NewEmitter(nil, logging.Nop()),
		cfg, logging.Nop())

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- survivor.Run(ctx) }()

	waitFor(t, 30*time.Second, func() bool {
		return f.countByStatus(t, core.StatusSent) == total
	}, "the surviving replica delivered everything")

	cancel()
	<-done

	if got := len(router.snapshot()); got != total {
		t.Errorf("%d messages reached the broker, want %d", got, total)
	}
}

// The advisory lock is what keeps housekeeping from running on every replica at
// once.
func TestHousekeepingRunsOnOneReplicaAtATime(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 5)

	cfg := scaleConfig(t, f)

	// Hold the stats lock as if another replica had it.
	release, ok, err := f.Store.TryLock(t.Context(), cfg.Janitor.LockKey, 2)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !ok {
		t.Fatal("could not take the lock to set the test up")
	}
	defer release()

	rec := &statsRecorder{}
	janitor := dispatch.NewJanitor(f.Store, rec, cfg, logging.Nop())

	janitor.SampleStats(t.Context())

	if rec.count() != 0 {
		t.Errorf("housekeeping ran %d times while another replica held the lock, want 0", rec.count())
	}

	release()

	janitor.SampleStats(t.Context())

	if rec.count() != 1 {
		t.Errorf("housekeeping ran %d times after the lock was free, want 1", rec.count())
	}
}

type statsRecorder struct {
	mu      sync.Mutex
	samples []events.Stats
}

func (r *statsRecorder) Stats(_ context.Context, ev events.Stats) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.samples = append(r.samples, ev)
}

func (r *statsRecorder) Reclaimed(context.Context, events.Reclaimed) {}
func (r *statsRecorder) Retention(context.Context, events.Retention) {}

func (r *statsRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.samples)
}

func scaleConfig(t *testing.T, f *fixture) config.Config {
	t.Helper()

	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_DSN="+f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA="+f.Schema,
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN="+amqpDSN(),
		"OUTBOX_DISPATCH_BATCH_SIZE=25",
		"OUTBOX_DISPATCH_WORKERS=4",
		"OUTBOX_DISPATCH_POLL_INTERVAL=20ms",
		"OUTBOX_DISPATCH_NOTIFY_ENABLED=false",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	// The janitor lock is namespaced per test schema, so parallel tests do not
	// exclude one another.
	cfg.Janitor.LockKey = int32(len(f.Schema)*7919 + 13)

	return cfg
}

func replicaName(i int) string { return "replica-" + string(rune('a'+i)) }
