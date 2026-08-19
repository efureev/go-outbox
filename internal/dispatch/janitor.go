package dispatch

import (
	"context"
	"log/slog"
	"time"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/store"
)

// Housekeeper is the database side of the janitor.
type Housekeeper interface {
	Reclaim(ctx context.Context, limit int) ([]store.Reclaimed, error)
	Stats(ctx context.Context) (store.Stats, error)
	Purge(ctx context.Context, retention time.Duration, limit int) (int64, error)
	TryLock(ctx context.Context, class, key int32) (release func(), ok bool, err error)

	// The partition side. Only reached on a range-partitioned table, which is
	// something the janitor asks about rather than being told.
	IsPartitioned(ctx context.Context) (bool, error)
	EnsurePartitions(ctx context.Context, ahead int) ([]string, error)
	DropExpiredPartitions(ctx context.Context, retention time.Duration) ([]string, error)
	DefaultPartitionRows(ctx context.Context) (int64, error)
}

// JanitorEmitter publishes what the janitor observes.
type JanitorEmitter interface {
	Reclaimed(ctx context.Context, ev events.Reclaimed)
	Stats(ctx context.Context, ev events.Stats)
	Retention(ctx context.Context, ev events.Retention)
	Partitions(ctx context.Context, ev events.Partitions)
}

// Task identifiers, used as the second half of the advisory lock key so the
// jobs do not exclude one another. Sampling the gauges takes no lock; see
// SampleStats.
const (
	taskReclaim   int32 = 1
	taskRetention int32 = 3
)

// Janitor runs the periodic work that must happen exactly once across the whole
// deployment rather than once per replica: returning expired leases, sampling
// the gauges, and removing delivered rows.
//
// Each cycle takes a PostgreSQL advisory lock and skips itself if another
// replica holds it. Every instance tries, one wins, the rest move on without
// waiting — the right shape for periodic work nobody is blocked on.
type Janitor struct {
	store   Housekeeper
	emitter JanitorEmitter
	cfg     config.JanitorConfig
	limit   int
	log     *slog.Logger
}

// NewJanitor builds a janitor. reclaimLimit bounds how many leases one cycle
// returns, so a mass failure does not become one enormous statement.
func NewJanitor(
	st Housekeeper,
	emitter JanitorEmitter,
	cfg config.Config,
	log *slog.Logger,
) *Janitor {
	return &Janitor{
		store:   st,
		emitter: emitter,
		cfg:     cfg.Janitor,
		limit:   max(cfg.Dispatch.BatchSize*4, 100),
		log:     log,
	}
}

// Run drives the three cycles until ctx is canceled.
func (j *Janitor) Run(ctx context.Context) error {
	reclaim := time.NewTicker(j.cfg.ReclaimInterval)
	defer reclaim.Stop()

	stats := time.NewTicker(j.cfg.StatsInterval)
	defer stats.Stop()

	retention := time.NewTicker(j.retentionInterval())
	defer retention.Stop()

	// One sample immediately, so /metrics is meaningful before the first tick
	// rather than reporting zeros for the first half minute.
	j.SampleStats(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-reclaim.C:
			j.ReclaimExpired(ctx)
		case <-stats.C:
			j.SampleStats(ctx)
		case <-retention.C:
			j.Sweep(ctx)
		}
	}
}

func (j *Janitor) retentionInterval() time.Duration {
	if j.cfg.Retention <= 0 || j.cfg.RetentionInterval <= 0 {
		// Retention is off. The ticker still has to tick at something, so it
		// ticks rarely and its handler returns at once.
		return time.Hour
	}

	return j.cfg.RetentionInterval
}

// ReclaimExpired returns leases whose owner never released them.
func (j *Janitor) ReclaimExpired(ctx context.Context) {
	j.exclusive(ctx, taskReclaim, "reclaim", func(ctx context.Context) error {
		reclaimed, err := j.store.Reclaim(ctx, j.limit)
		if err != nil {
			return err
		}
		if len(reclaimed) == 0 {
			return nil
		}

		for _, r := range reclaimed {
			j.emitter.Reclaimed(ctx, events.Reclaimed{
				Stream:  r.Stream,
				Owner:   r.Owner,
				Overdue: r.Overdue,
			})
		}

		// Worth a log line as well as a metric: a reclaim means a replica died
		// mid-batch, or the lease is shorter than the work.
		j.log.Warn("returned expired leases to the queue",
			slog.Int("messages", len(reclaimed)),
			slog.String("previous_owner", reclaimed[0].Owner),
		)

		return nil
	})
}

// SampleStats refreshes the backlog gauges.
//
// Unlike the other two cycles this runs on every replica, without the advisory
// lock. A gauge that only the lock holder refreshes leaves every other replica
// exporting its zero value forever, so a dashboard reading one series — or
// averaging them — reports an empty backlog while the queue grows. The query is
// four index-only counts on a slow tick; paying for it per replica is cheaper
// than a metric that is wrong on all but one of them.
func (j *Janitor) SampleStats(ctx context.Context) {
	st, err := j.store.Stats(ctx)
	if err != nil {
		if ctx.Err() == nil {
			j.log.Error("housekeeping task failed", slog.String("task", "stats"), slog.Any("error", err))
		}

		return
	}

	j.emitter.Stats(ctx, events.Stats{
		Pending:       st.Pending,
		Processing:    st.Processing,
		Failed:        st.Failed,
		OldestPending: st.OldestPending,
		Deferred:      st.Deferred,
	})
}

// Sweep removes delivered rows past their retention window.
//
// It runs in bounded chunks, repeating until a chunk comes back short. A single
// unbounded DELETE over months of delivered rows holds one transaction open for
// the duration and bloats the table it is trying to shrink.
func (j *Janitor) Sweep(ctx context.Context) {
	if j.cfg.Retention <= 0 {
		return
	}

	j.exclusive(ctx, taskRetention, "retention", func(ctx context.Context) error {
		// Asked every sweep rather than cached. It is one catalogue row every
		// few minutes, on the single replica holding the lock, and it means
		// converting the table does not need the dispatcher restarted to be
		// noticed.
		partitioned, err := j.store.IsPartitioned(ctx)
		if err != nil {
			return err
		}
		if partitioned {
			return j.maintainPartitions(ctx)
		}

		var total int64

		for {
			deleted, err := j.store.Purge(ctx, j.cfg.Retention, j.cfg.RetentionBatch)
			if err != nil {
				return err
			}

			total += deleted

			if deleted < int64(j.cfg.RetentionBatch) {
				break
			}

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(50 * time.Millisecond):
				// A pause between chunks, so a large sweep does not monopolise
				// the database the dispatcher is also using.
			}
		}

		if total > 0 {
			j.emitter.Retention(ctx, events.Retention{Deleted: total})
			j.log.Info("retention sweep finished", slog.Int64("deleted", total))
		}

		return nil
	})
}

// maintainPartitions is retention on a partitioned table: create the days that
// are coming, drop the ones nothing needs any more.
//
// The saving is the whole reason for partitioning. A chunked DELETE has to find
// every row, mark it dead and wait for autovacuum to reclaim the space; past
// roughly ten million rows a day it stops keeping up and the table grows while
// apparently being cleaned. Dropping a partition is a catalogue change and an
// unlink, and costs the same whatever it held.
func (j *Janitor) maintainPartitions(ctx context.Context) error {
	created, err := j.store.EnsurePartitions(ctx, j.cfg.PartitionAhead)
	if err != nil {
		return err
	}

	dropped, err := j.store.DropExpiredPartitions(ctx, j.cfg.Retention)
	if err != nil {
		return err
	}

	orphans, err := j.store.DefaultPartitionRows(ctx)
	if err != nil {
		return err
	}

	// EnsurePartitions reports every day it covered, including the ones that
	// already existed, so a steady state would otherwise say something on every
	// sweep. Only a change or a problem is worth reporting.
	if len(dropped) == 0 && orphans == 0 {
		return nil
	}

	j.emitter.Partitions(ctx, events.Partitions{
		Created: len(created), Dropped: len(dropped), DefaultRows: orphans,
	})

	if len(dropped) > 0 {
		j.log.Info("partitions dropped",
			slog.Int("count", len(dropped)),
			slog.Duration("retention", j.cfg.Retention),
		)
	}
	if orphans > 0 {
		// The default partition did its job — the producer's transaction
		// committed — but those rows now sit in the way of creating the proper
		// partition for their range.
		j.log.Warn("rows landed in the default partition: a daily partition was missing",
			slog.Int64("rows", orphans),
			slog.Int("partition_ahead", j.cfg.PartitionAhead),
		)
	}

	return nil
}

// exclusive runs fn only if this replica wins the advisory lock for the task.
func (j *Janitor) exclusive(ctx context.Context, task int32, name string, fn func(context.Context) error) {
	release, ok, err := j.store.TryLock(ctx, j.lockClass(), task)
	if err != nil {
		j.log.Error("could not take the housekeeping lock",
			slog.String("task", name), slog.Any("error", err))

		return
	}
	if !ok {
		// Another replica is doing it. Nothing to report: this is the design.
		return
	}
	defer release()

	if err := fn(ctx); err != nil && ctx.Err() == nil {
		j.log.Error("housekeeping task failed", slog.String("task", name), slog.Any("error", err))
	}
}

// lockClass is the first half of the advisory-lock key; the task identifier is
// the second, so the three jobs do not exclude one another.
func (j *Janitor) lockClass() int32 { return j.cfg.LockKey }
