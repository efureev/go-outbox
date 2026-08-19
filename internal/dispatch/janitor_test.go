package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
)

// fakeKeeper stands in for the database side. Every method records what it was
// asked and returns what the test set, so a test can describe a database in the
// state it cares about rather than arranging one.
type fakeKeeper struct {
	mu sync.Mutex

	reclaimed    []store.Reclaimed
	reclaimErr   error
	reclaimLimit int

	stats      store.Stats
	statsErr   error
	statsCalls int

	// purge returns the next element of purgeReturns on each call, so a test can
	// describe a sweep that takes several chunks.
	purgeReturns []int64
	purgeErr     error
	purgeCalls   int
	purgeLimits  []int
	purgeKept    []time.Duration
	onPurge      func()

	partitioned    bool
	partitionedErr error

	created    []string
	createErr  error
	aheadAsked []int

	dropped   []string
	dropErr   error
	orphans   int64
	orphanErr error

	lockOK   bool
	lockErr  error
	locks    [][2]int32
	released int
}

func newKeeper() *fakeKeeper { return &fakeKeeper{lockOK: true} }

func (k *fakeKeeper) Reclaim(_ context.Context, limit int) ([]store.Reclaimed, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.reclaimLimit = limit

	return k.reclaimed, k.reclaimErr
}

func (k *fakeKeeper) Stats(context.Context) (store.Stats, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.statsCalls++

	return k.stats, k.statsErr
}

func (k *fakeKeeper) Purge(_ context.Context, retention time.Duration, limit int) (int64, error) {
	k.mu.Lock()
	k.purgeCalls++
	k.purgeLimits = append(k.purgeLimits, limit)
	k.purgeKept = append(k.purgeKept, retention)
	n := int64(0)
	if len(k.purgeReturns) > 0 {
		n = k.purgeReturns[0]
		k.purgeReturns = k.purgeReturns[1:]
	}
	onPurge := k.onPurge
	err := k.purgeErr
	k.mu.Unlock()

	if onPurge != nil {
		onPurge()
	}

	return n, err
}

func (k *fakeKeeper) TryLock(_ context.Context, class, key int32) (func(), bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.locks = append(k.locks, [2]int32{class, key})

	if k.lockErr != nil {
		return nil, false, k.lockErr
	}
	if !k.lockOK {
		return nil, false, nil
	}

	return func() {
		k.mu.Lock()
		defer k.mu.Unlock()
		k.released++
	}, true, nil
}

func (k *fakeKeeper) IsPartitioned(context.Context) (bool, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.partitioned, k.partitionedErr
}

func (k *fakeKeeper) EnsurePartitions(_ context.Context, ahead int) ([]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.aheadAsked = append(k.aheadAsked, ahead)

	return k.created, k.createErr
}

func (k *fakeKeeper) DropExpiredPartitions(context.Context, time.Duration) ([]string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.dropped, k.dropErr
}

func (k *fakeKeeper) DefaultPartitionRows(context.Context) (int64, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.orphans, k.orphanErr
}

func (k *fakeKeeper) snapshot() (purges, statsCalls, released int, locks [][2]int32) {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.purgeCalls, k.statsCalls, k.released, append([][2]int32(nil), k.locks...)
}

// janitorRecorder collects what the janitor announced.
type janitorRecorder struct {
	mu         sync.Mutex
	reclaimed  []events.Reclaimed
	stats      []events.Stats
	retention  []events.Retention
	partitions []events.Partitions
}

func (r *janitorRecorder) Reclaimed(_ context.Context, ev events.Reclaimed) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reclaimed = append(r.reclaimed, ev)
}

func (r *janitorRecorder) Stats(_ context.Context, ev events.Stats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats = append(r.stats, ev)
}

func (r *janitorRecorder) Retention(_ context.Context, ev events.Retention) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retention = append(r.retention, ev)
}

func (r *janitorRecorder) Partitions(_ context.Context, ev events.Partitions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.partitions = append(r.partitions, ev)
}

func (r *janitorRecorder) counts() (reclaimed, stats, retention, partitions int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.reclaimed), len(r.stats), len(r.retention), len(r.partitions)
}

func janitorConfig() config.Config {
	cfg := testConfig()
	cfg.Janitor = config.JanitorConfig{
		Enabled:           true,
		ReclaimInterval:   time.Millisecond,
		StatsInterval:     time.Millisecond,
		PartitionAhead:    3,
		Retention:         168 * time.Hour,
		RetentionInterval: time.Millisecond,
		RetentionBatch:    5000,
		LockKey:           809021150,
	}

	return cfg
}

func newTestJanitor(t *testing.T, cfg config.Config) (*Janitor, *fakeKeeper, *janitorRecorder) {
	t.Helper()

	keeper := newKeeper()
	rec := &janitorRecorder{}

	return NewJanitor(keeper, rec, cfg, logging.Nop()), keeper, rec
}

// The reclaim limit is derived, not configured: a mass failure must not become
// one enormous statement, and a small batch size must not make recovery crawl.
func TestTheReclaimLimitHasAFloor(t *testing.T) {
	cases := []struct {
		batch int
		want  int
	}{
		{batch: 10, want: 100},   // four times ten is under the floor
		{batch: 25, want: 100},   // exactly the floor
		{batch: 500, want: 2000}, // four times the batch wins
	}

	for _, c := range cases {
		cfg := janitorConfig()
		cfg.Dispatch.BatchSize = c.batch

		j, keeper, _ := newTestJanitor(t, cfg)
		j.ReclaimExpired(t.Context())

		if keeper.reclaimLimit != c.want {
			t.Errorf("batch %d gave a reclaim limit of %d, want %d",
				c.batch, keeper.reclaimLimit, c.want)
		}
	}
}

func TestReclaimAnnouncesEveryLease(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.reclaimed = []store.Reclaimed{
		{ID: "a", Stream: "local", Owner: "dead-1", Overdue: 4 * time.Second},
		{ID: "b", Stream: "local", Owner: "dead-1", Overdue: 5 * time.Second},
	}

	j.ReclaimExpired(t.Context())

	if n, _, _, _ := rec.counts(); n != 2 {
		t.Fatalf("announced %d reclaims, want 2", n)
	}
	if rec.reclaimed[0].Owner != "dead-1" || rec.reclaimed[1].Overdue != 5*time.Second {
		t.Errorf("the event lost the circumstances: %+v", rec.reclaimed)
	}
}

// Nothing to reclaim is the normal state. It must be silent, or the signal that
// a replica died is buried under a message every thirty seconds.
func TestReclaimingNothingSaysNothing(t *testing.T) {
	j, _, rec := newTestJanitor(t, janitorConfig())

	j.ReclaimExpired(t.Context())

	if n, _, _, _ := rec.counts(); n != 0 {
		t.Errorf("announced %d reclaims with nothing reclaimed", n)
	}
}

func TestAFailedReclaimAnnouncesNothing(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.reclaimErr = errors.New("connection refused")
	keeper.reclaimed = []store.Reclaimed{{ID: "a", Stream: "local"}}

	j.ReclaimExpired(t.Context())

	if n, _, _, _ := rec.counts(); n != 0 {
		t.Errorf("announced %d reclaims from a failed query", n)
	}
	if _, _, released, _ := keeper.snapshot(); released != 1 {
		t.Errorf("the lock was released %d times; a failure must still release it", released)
	}
}

func TestSampledStatsCarryEveryGauge(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.stats = store.Stats{
		Pending: 12, Processing: 3, Failed: 1,
		OldestPending: 90 * time.Second, Deferred: 7,
	}

	j.SampleStats(t.Context())

	if _, n, _, _ := rec.counts(); n != 1 {
		t.Fatalf("sampled %d times, want 1", n)
	}

	got := rec.stats[0]
	want := events.Stats{Pending: 12, Processing: 3, Failed: 1, OldestPending: 90 * time.Second, Deferred: 7}
	if got != want {
		t.Errorf("gauges = %+v, want %+v", got, want)
	}
}

// Documented and counterintuitive, so pinned: sampling runs on every replica
// without the advisory lock. Under the lock, every replica but one would export
// its zero value forever and a dashboard would report an empty backlog while
// the queue grew.
func TestSamplingTakesNoLock(t *testing.T) {
	j, keeper, _ := newTestJanitor(t, janitorConfig())
	keeper.lockOK = false

	j.SampleStats(t.Context())

	_, statsCalls, _, locks := keeper.snapshot()
	if len(locks) != 0 {
		t.Errorf("sampling took %d locks, want none", len(locks))
	}
	if statsCalls != 1 {
		t.Errorf("the query ran %d times on a replica that would lose the lock, want 1", statsCalls)
	}
}

func TestAFailedSampleLeavesTheGaugesAlone(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.statsErr = errors.New("timeout")

	j.SampleStats(t.Context())

	if _, n, _, _ := rec.counts(); n != 0 {
		t.Errorf("published %d samples from a failed query; stale gauges beat wrong ones", n)
	}
}

// Retention off means off. If the guard went, the sweep would run with a zero
// window — every delivered row is older than now.
func TestRetentionDisabledSweepsNothingAndTakesNoLock(t *testing.T) {
	for _, retention := range []time.Duration{0, -time.Hour} {
		cfg := janitorConfig()
		cfg.Janitor.Retention = retention

		j, keeper, rec := newTestJanitor(t, cfg)
		j.Sweep(t.Context())

		purges, _, _, locks := keeper.snapshot()
		if purges != 0 {
			t.Errorf("retention %v purged %d times", retention, purges)
		}
		if len(locks) != 0 {
			t.Errorf("retention %v took %d locks", retention, len(locks))
		}
		if _, _, n, _ := rec.counts(); n != 0 {
			t.Errorf("retention %v announced %d sweeps", retention, n)
		}
	}
}

// The sweep is chunked on purpose: it repeats while chunks come back full and
// stops on the first short one. A single unbounded DELETE would hold one
// transaction open over months of rows.
func TestTheSweepRepeatsUntilAChunkComesBackShort(t *testing.T) {
	cfg := janitorConfig()
	cfg.Janitor.RetentionBatch = 100

	j, keeper, rec := newTestJanitor(t, cfg)
	keeper.purgeReturns = []int64{100, 100, 40}

	j.Sweep(t.Context())

	purges, _, released, _ := keeper.snapshot()
	if purges != 3 {
		t.Errorf("ran %d chunks, want 3 — full, full, short", purges)
	}
	if released != 1 {
		t.Errorf("released the lock %d times, want once", released)
	}

	if _, _, n, _ := rec.counts(); n != 1 {
		t.Fatalf("announced %d sweeps, want 1 for the whole run", n)
	}
	if got := rec.retention[0].Deleted; got != 240 {
		t.Errorf("reported %d deleted, want 240 — the total, not the last chunk", got)
	}

	for i, limit := range keeper.purgeLimits {
		if limit != 100 {
			t.Errorf("chunk %d used a limit of %d, want the configured 100", i, limit)
		}
	}
	if keeper.purgeKept[0] != cfg.Janitor.Retention {
		t.Errorf("swept with a window of %v, want %v", keeper.purgeKept[0], cfg.Janitor.Retention)
	}
}

// A sweep that deletes nothing is the steady state, and must not speak.
func TestASweepThatDeletesNothingIsSilent(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.purgeReturns = []int64{0}

	j.Sweep(t.Context())

	if purges, _, _, _ := keeper.snapshot(); purges != 1 {
		t.Errorf("ran %d chunks, want 1", purges)
	}
	if _, _, n, _ := rec.counts(); n != 0 {
		t.Errorf("announced %d sweeps having deleted nothing", n)
	}
}

func TestAFailedChunkStopsTheSweep(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.purgeErr = errors.New("deadlock detected")

	j.Sweep(t.Context())

	if purges, _, released, _ := keeper.snapshot(); purges != 1 || released != 1 {
		t.Errorf("chunks=%d released=%d, want 1 and 1", purges, released)
	}
	if _, _, n, _ := rec.counts(); n != 0 {
		t.Errorf("announced %d sweeps from a failed one", n)
	}
}

// A shutdown mid-sweep leaves the rows for the next run rather than holding the
// process open through however many chunks remain.
func TestAShutdownStopsTheSweepBetweenChunks(t *testing.T) {
	cfg := janitorConfig()
	cfg.Janitor.RetentionBatch = 10

	j, keeper, rec := newTestJanitor(t, cfg)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Every chunk comes back full, so only the cancellation can end this.
	keeper.purgeReturns = []int64{10, 10, 10, 10, 10}
	keeper.onPurge = func() { cancel() }

	done := make(chan struct{})
	go func() {
		defer close(done)
		j.Sweep(ctx)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the sweep ignored the cancellation")
	}

	if purges, _, _, _ := keeper.snapshot(); purges != 1 {
		t.Errorf("ran %d chunks after a cancellation on the first, want 1", purges)
	}
	// It stopped without finishing, so it has no total worth reporting.
	if _, _, n, _ := rec.counts(); n != 0 {
		t.Errorf("announced %d sweeps from an interrupted one", n)
	}
}

// Partitioning is asked about every sweep rather than cached, and it changes
// which tool is used. A chunked DELETE on a partitioned table is exactly the
// work partitioning exists to avoid.
func TestAPartitionedTableIsNeverSweptRowByRow(t *testing.T) {
	j, keeper, _ := newTestJanitor(t, janitorConfig())
	keeper.partitioned = true
	keeper.dropped = []string{"messages_2026_08_01"}

	j.Sweep(t.Context())

	if purges, _, _, _ := keeper.snapshot(); purges != 0 {
		t.Errorf("deleted rows one chunk at a time %d times on a partitioned table", purges)
	}
	if len(keeper.aheadAsked) != 1 || keeper.aheadAsked[0] != 3 {
		t.Errorf("asked for %v days ahead, want one round of 3", keeper.aheadAsked)
	}
}

func TestAnUnansweredPartitionQuestionSweepsNothing(t *testing.T) {
	j, keeper, _ := newTestJanitor(t, janitorConfig())
	keeper.partitionedErr = errors.New("catalogue unreadable")

	j.Sweep(t.Context())

	purges, _, released, _ := keeper.snapshot()
	if purges != 0 {
		t.Errorf("swept %d chunks without knowing the table's shape", purges)
	}
	if released != 1 {
		t.Errorf("released %d times, want once", released)
	}
}

// Creating partitions in advance is reported only when something changed:
// EnsurePartitions names every day it covered, including the ones that already
// existed, so reporting it unconditionally would speak on every sweep forever.
func TestSteadyPartitionMaintenanceIsSilent(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.partitioned = true
	keeper.created = []string{"messages_2026_08_20", "messages_2026_08_21"}

	j.Sweep(t.Context())

	if _, _, _, n := rec.counts(); n != 0 {
		t.Errorf("announced %d rounds with nothing dropped and no orphans", n)
	}
}

func TestDroppedPartitionsAreAnnouncedWithWhatWasCreated(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.partitioned = true
	keeper.created = []string{"messages_2026_08_20", "messages_2026_08_21"}
	keeper.dropped = []string{"messages_2026_08_01"}

	j.Sweep(t.Context())

	if _, _, _, n := rec.counts(); n != 1 {
		t.Fatalf("announced %d rounds, want 1", n)
	}

	want := events.Partitions{Created: 2, Dropped: 1, DefaultRows: 0}
	if got := rec.partitions[0]; got != want {
		t.Errorf("round = %+v, want %+v", got, want)
	}
}

// A row in the default partition means one arrived for a day nobody had created
// a partition for. It is not lost — the producer's transaction committed — but
// it now blocks creating the proper partition for its range, so it must be
// reported even though nothing was dropped.
func TestRowsInTheDefaultPartitionBreakTheSilence(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.partitioned = true
	keeper.orphans = 42

	j.Sweep(t.Context())

	if _, _, _, n := rec.counts(); n != 1 {
		t.Fatalf("announced %d rounds, want 1 — orphans are not a steady state", n)
	}
	if got := rec.partitions[0].DefaultRows; got != 42 {
		t.Errorf("reported %d orphaned rows, want 42", got)
	}
}

// Each step of partition maintenance can fail on its own, and none of them may
// announce a round that did not happen.
func TestPartitionMaintenanceFailuresAnnounceNothing(t *testing.T) {
	boom := errors.New("permission denied")

	cases := []struct {
		name  string
		apply func(*fakeKeeper)
	}{
		{"creating", func(k *fakeKeeper) { k.createErr = boom }},
		{"dropping", func(k *fakeKeeper) { k.dropErr = boom }},
		{"counting orphans", func(k *fakeKeeper) { k.orphanErr = boom }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			j, keeper, rec := newTestJanitor(t, janitorConfig())
			keeper.partitioned = true
			keeper.dropped = []string{"messages_2026_08_01"}
			keeper.orphans = 5
			c.apply(keeper)

			j.Sweep(t.Context())

			if _, _, _, n := rec.counts(); n != 0 {
				t.Errorf("announced %d rounds after %s failed", n, c.name)
			}
			if _, _, released, _ := keeper.snapshot(); released != 1 {
				t.Error("a failure did not release the lock")
			}
		})
	}
}

// Losing the race is the design, not a problem: every replica tries, one wins.
func TestLosingTheLockSkipsTheWorkQuietly(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.lockOK = false
	keeper.reclaimed = []store.Reclaimed{{ID: "a"}}
	keeper.purgeReturns = []int64{100}

	j.ReclaimExpired(t.Context())
	j.Sweep(t.Context())

	purges, _, released, _ := keeper.snapshot()
	if purges != 0 {
		t.Errorf("purged %d times without holding the lock", purges)
	}
	if released != 0 {
		t.Errorf("released a lock it never took, %d times", released)
	}
	if r, _, ret, _ := rec.counts(); r != 0 || ret != 0 {
		t.Errorf("announced reclaims=%d retention=%d without the lock", r, ret)
	}
}

func TestAnUnavailableLockSkipsTheWork(t *testing.T) {
	j, keeper, _ := newTestJanitor(t, janitorConfig())
	keeper.lockErr = errors.New("connection refused")
	keeper.purgeReturns = []int64{100}

	j.Sweep(t.Context())

	if purges, _, released, _ := keeper.snapshot(); purges != 0 || released != 0 {
		t.Errorf("purges=%d released=%d after the lock could not be taken", purges, released)
	}
}

// The two singleton tasks share a lock class and differ in the key, so one does
// not exclude the other: a long sweep must not stop leases being reclaimed.
func TestTheSingletonTasksDoNotExcludeOneAnother(t *testing.T) {
	cfg := janitorConfig()
	cfg.Janitor.LockKey = 4242

	j, keeper, _ := newTestJanitor(t, cfg)

	j.ReclaimExpired(t.Context())
	j.Sweep(t.Context())

	_, _, _, locks := keeper.snapshot()
	if len(locks) != 2 {
		t.Fatalf("took %d locks, want 2", len(locks))
	}
	if locks[0][0] != 4242 || locks[1][0] != 4242 {
		t.Errorf("lock classes = %d and %d, want the configured 4242", locks[0][0], locks[1][0])
	}
	if locks[0][1] == locks[1][1] {
		t.Errorf("reclaim and retention share the key %d, so one blocks the other", locks[0][1])
	}
}

// The retention ticker has to tick at something even when retention is off. It
// ticks rarely, and Sweep returns at once.
func TestTheRetentionTickerIdlesWhenThereIsNothingToSweep(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*config.JanitorConfig)
		want   time.Duration
	}{
		{"retention off", func(c *config.JanitorConfig) { c.Retention = 0 }, time.Hour},
		{"interval off", func(c *config.JanitorConfig) { c.RetentionInterval = 0 }, time.Hour},
		{"both set", func(*config.JanitorConfig) {}, time.Millisecond},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := janitorConfig()
			c.mutate(&cfg.Janitor)

			j, _, _ := newTestJanitor(t, cfg)
			if got := j.retentionInterval(); got != c.want {
				t.Errorf("interval = %v, want %v", got, c.want)
			}
		})
	}
}

// /metrics must be meaningful before the first tick, or a dashboard reports an
// empty backlog for the first half minute of every deployment.
func TestRunSamplesBeforeItsFirstTick(t *testing.T) {
	cfg := janitorConfig()
	cfg.Janitor.StatsInterval = time.Hour
	cfg.Janitor.ReclaimInterval = time.Hour
	cfg.Janitor.RetentionInterval = time.Hour

	j, keeper, rec := newTestJanitor(t, cfg)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- j.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for {
		if _, n, _, _ := rec.counts(); n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("no sample before the first tick, an hour away")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v on a cancellation, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run ignored the cancellation")
	}

	if _, statsCalls, _, _ := keeper.snapshot(); statsCalls < 1 {
		t.Error("the immediate sample never queried")
	}
}

// All three cycles have to fire. With intervals of a millisecond, a short run
// should exercise each one.
func TestRunDrivesAllThreeCycles(t *testing.T) {
	j, keeper, rec := newTestJanitor(t, janitorConfig())
	keeper.reclaimed = []store.Reclaimed{{ID: "a", Stream: "local", Owner: "dead"}}
	keeper.purgeReturns = []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _ = j.Run(ctx) }()

	deadline := time.After(3 * time.Second)
	for {
		reclaimed, stats, retention, _ := rec.counts()
		if reclaimed > 0 && stats > 1 && retention > 0 {
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("cycles after three seconds: reclaimed=%d stats=%d retention=%d",
				reclaimed, stats, retention)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	<-done
}
