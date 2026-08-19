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

// The advisory lock is what keeps the work-doing housekeeping — returning
// expired leases, sweeping delivered rows — from running on every replica at
// once.
func TestHousekeepingRunsOnOneReplicaAtATime(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 5)

	cfg := scaleConfig(t, f)

	// Leave an expired lease for the reclaimer to find.
	claimed, err := f.Store.Claim(t.Context(), "local", 5, lease("dead", time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	f.expire(t, ids(claimed)...)

	// Hold the reclaim lock as if another replica had it.
	release, ok, err := f.Store.TryLock(t.Context(), cfg.Janitor.LockKey, reclaimTask)
	if err != nil {
		t.Fatalf("try lock: %v", err)
	}
	if !ok {
		t.Fatal("could not take the lock to set the test up")
	}
	defer release()

	rec := &housekeepingRecorder{}
	janitor := dispatch.NewJanitor(f.Store, rec, cfg, logging.Nop())

	janitor.ReclaimExpired(t.Context())

	if got := rec.reclaimed(); got != 0 {
		t.Errorf("reclaimed %d leases while another replica held the lock, want 0", got)
	}

	release()

	janitor.ReclaimExpired(t.Context())

	if got := rec.reclaimed(); got != 5 {
		t.Errorf("reclaimed %d leases once the lock was free, want 5", got)
	}
}

// Sampling the gauges is deliberately not behind the lock. A gauge only the
// lock holder refreshes leaves every other replica exporting its zero value, so
// a dashboard reading one series reports an empty backlog while the queue grows.
func TestGaugesAreSampledByEveryReplica(t *testing.T) {
	f := newFixture(t)
	f.seed(t, "local", 3)

	cfg := scaleConfig(t, f)

	// Another replica holds every housekeeping lock this janitor might want.
	for _, task := range []int32{reclaimTask, retentionTask} {
		release, ok, err := f.Store.TryLock(t.Context(), cfg.Janitor.LockKey, task)
		if err != nil {
			t.Fatalf("try lock: %v", err)
		}
		if !ok {
			t.Fatalf("could not take lock %d to set the test up", task)
		}
		defer release()
	}

	rec := &housekeepingRecorder{}
	janitor := dispatch.NewJanitor(f.Store, rec, cfg, logging.Nop())

	janitor.SampleStats(t.Context())

	samples := rec.stats()
	if len(samples) != 1 {
		t.Fatalf("%d samples taken while the locks were held, want 1", len(samples))
	}
	if samples[0].Pending != 3 {
		t.Errorf("Pending = %d, want 3", samples[0].Pending)
	}
	if samples[0].OldestPending <= 0 {
		t.Error("OldestPending is zero; every replica must report a real backlog age")
	}
}

// Task identifiers, mirroring the unexported ones in the dispatch package.
const (
	reclaimTask   int32 = 1
	retentionTask int32 = 3
)

type housekeepingRecorder struct {
	mu       sync.Mutex
	samples  []events.Stats
	reclaims []events.Reclaimed
}

func (r *housekeepingRecorder) Stats(_ context.Context, ev events.Stats) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.samples = append(r.samples, ev)
}

func (r *housekeepingRecorder) Reclaimed(_ context.Context, ev events.Reclaimed) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.reclaims = append(r.reclaims, ev)
}

func (r *housekeepingRecorder) Retention(context.Context, events.Retention) {}

func (r *housekeepingRecorder) stats() []events.Stats {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]events.Stats(nil), r.samples...)
}

// Partitions is not observed here; the interface asks for it.
func (*housekeepingRecorder) Partitions(context.Context, events.Partitions) {}

func (r *housekeepingRecorder) reclaimed() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.reclaims)
}

func scaleConfig(t testing.TB, f *fixture) config.Config {
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
