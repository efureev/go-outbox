// Package dispatch is the dispatcher's core loop: claim a batch, publish it,
// write the outcome back.
//
// One pipeline runs per stream. Processing every stream in one batch and
// publishing serially would mean a broker that is down does not merely delay
// its own messages: it holds up everything claimed behind them.
package dispatch

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/store"
)

// Store is the database side of a pipeline.
type Store interface {
	Claim(ctx context.Context, stream string, limit int, lease core.Lease) ([]core.Message, error)
	Ack(ctx context.Context, ids []string, token string) (store.AckResult, error)
	Nack(ctx context.Context, outcomes []core.Outcome, token string, maxAttempts int) (store.NackResult, error)
	ReleaseLease(ctx context.Context, ids []string, token string) (int, error)
}

// Router publishes a batch belonging to one stream.
type Router interface {
	Publish(ctx context.Context, stream string, msgs []core.Message) []error
	DriverFor(stream string) (string, bool)
}

// Emitter publishes a domain event. It exists so a pipeline can be tested
// without a bus.
type Emitter interface {
	Iteration(ctx context.Context, ev events.Iteration)
}

// Pipeline dispatches one stream.
type Pipeline struct {
	stream  string
	driver  string
	store   Store
	router  Router
	emitter Emitter
	log     *slog.Logger

	cfg     config.DispatchConfig
	backoff core.BackoffPolicy
	owner   string

	// wake carries "there is work" from the notification listener. Capacity
	// one, because the signal is idempotent: two notifications and one mean the
	// same thing to a loop that claims everything due.
	wake chan struct{}
}

// New builds a pipeline for one stream.
func New(
	stream string,
	st Store,
	router Router,
	emitter Emitter,
	cfg config.Config,
	log *slog.Logger,
) *Pipeline {
	driver, _ := router.DriverFor(stream)

	return &Pipeline{
		stream:  stream,
		driver:  driver,
		store:   st,
		router:  router,
		emitter: emitter,
		log:     log.With(slog.String("stream", stream), slog.String("driver", driver)),
		cfg:     cfg.Dispatch,
		backoff: core.BackoffPolicy{
			Base:   cfg.Dispatch.BackoffBase,
			Max:    cfg.Dispatch.BackoffMax,
			Jitter: cfg.Dispatch.BackoffJitter,
		},
		owner: cfg.App.Instance,
		wake:  make(chan struct{}, 1),
	}
}

// Stream reports which stream this pipeline serves.
func (p *Pipeline) Stream() string { return p.stream }

// Wake asks the pipeline to claim now instead of waiting for the next tick. It
// never blocks: the signal carries no information beyond its own existence.
func (p *Pipeline) Wake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Run drives the pipeline until ctx is canceled.
//
// The loop is adaptive: a full batch means there is a backlog, so the next
// iteration starts at once rather than sleeping out the poll interval. Always
// waiting would cap throughput at batch size divided by poll interval — a fixed
// number of messages per tick, whatever the hardware underneath.
func (p *Pipeline) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		claimed, err := p.RunOnce(ctx)
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nil
		case err != nil:
			p.log.Error("iteration failed", slog.Any("error", err))
		case claimed >= p.cfg.BatchSize:
			// A full batch means more is waiting. Do not sleep on a backlog.
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case <-p.wake:
		case <-ticker.C:
		}
	}
}

// RunOnce performs one claim-publish-write-back cycle and reports how many
// messages it claimed.
func (p *Pipeline) RunOnce(ctx context.Context) (int, error) {
	started := time.Now()

	lease := core.Lease{
		Token: uuid.NewString(),
		Owner: p.owner,
		Until: started.Add(p.cfg.LeaseTTL),
	}

	messages, err := p.store.Claim(ctx, p.stream, p.cfg.BatchSize, lease)
	if err != nil {
		return 0, err
	}
	if len(messages) == 0 {
		return 0, nil
	}

	ev := events.Iteration{
		Stream:  p.stream,
		Driver:  p.driver,
		Claimed: len(messages),
		Retries: countRetries(messages),
	}

	publishes, attempted := p.publish(ctx, messages)
	ev.Publishes = publishes

	// A shutdown that interrupts publishing leaves the untried tail claimed.
	// Handing it back explicitly means another replica picks it up now rather
	// than after the lease expires; simply exiting would leave those rows idle
	// for the whole lease.
	if attempted < len(messages) {
		released, err := p.release(ctx, messages[attempted:], lease.Token)
		if err != nil {
			p.log.Warn("could not hand back unfinished claims", slog.Any("error", err))
		}
		ev.Released = released
	}

	acked, nacked := split(messages[:attempted], publishes, p.backoff)

	if err := p.writeBack(ctx, acked, nacked, lease.Token, &ev); err != nil {
		return len(messages), err
	}

	ev.Duration = time.Since(started)
	p.emitter.Iteration(ctx, ev)

	return len(messages), nil
}

// writeBack records the outcome of the batch.
//
// It uses a context detached from the caller's deadline. Losing this write is
// the one failure that costs something real: the messages stay leased until
// they expire and are then published a second time. Finishing it is worth the
// extra moment even when a shutdown has already begun.
func (p *Pipeline) writeBack(
	ctx context.Context,
	acked []string,
	nacked []core.Outcome,
	token string,
	ev *events.Iteration,
) error {
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.WriteBackTimeout)
	defer cancel()

	var errs []error

	if len(acked) > 0 {
		res, err := p.store.Ack(writeCtx, acked, token)
		if err != nil {
			errs = append(errs, err)
		} else {
			ev.Conflicts += res.Conflicts
			for _, d := range res.Delivered {
				ev.Delivered = append(ev.Delivered, events.Delivery{ID: d.ID, Lag: d.Lag})
			}
		}
	}

	if len(nacked) > 0 {
		res, err := p.store.Nack(writeCtx, nacked, token, p.cfg.MaxAttempts)
		if err != nil {
			errs = append(errs, err)
		} else {
			ev.Conflicts += res.Conflicts
			for _, o := range res.Outcomes {
				if o.Status == core.StatusFailed {
					ev.Failed = append(ev.Failed, events.Terminal{
						ID:        o.ID,
						Attempts:  o.Attempts,
						Permanent: permanentFor(o.ID, nacked),
					})

					continue
				}
				ev.Requeued = append(ev.Requeued, o.ID)
			}
		}
	}

	if ev.Conflicts > 0 {
		// Not an error: the rows belong to whoever holds the lease now. It is
		// worth saying out loud, because it means the lease is shorter than the
		// work and messages are being published twice.
		p.log.Warn("lease was reclaimed while publishing",
			slog.Int("messages", ev.Conflicts),
			slog.Duration("lease_ttl", p.cfg.LeaseTTL),
		)
	}

	return errors.Join(errs...)
}

func (p *Pipeline) release(ctx context.Context, messages []core.Message, token string) (int, error) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.WriteBackTimeout)
	defer cancel()

	pending := make([]string, len(messages))
	for i, m := range messages {
		pending[i] = m.ID
	}

	return p.store.ReleaseLease(releaseCtx, pending, token)
}

func countRetries(messages []core.Message) int {
	n := 0
	for _, m := range messages {
		if m.Attempts > 0 {
			n++
		}
	}

	return n
}

// split turns publish results into the two write-backs they imply.
func split(messages []core.Message, publishes []events.Publish, backoff core.BackoffPolicy) ([]string, []core.Outcome) {
	var (
		acked  []string
		nacked []core.Outcome
	)

	for i, msg := range messages {
		pub := publishes[i]
		if pub.Err == nil {
			acked = append(acked, msg.ID)

			continue
		}

		nacked = append(nacked, core.Outcome{
			ID:        msg.ID,
			Err:       pub.Err,
			Permanent: pub.Permanent,
			// Attempts is the count before this failure, so the delay is for
			// the attempt about to be scheduled.
			Delay: backoff.Next(msg.Attempts + 1),
		})
	}

	return acked, nacked
}

func permanentFor(id string, outcomes []core.Outcome) bool {
	for _, o := range outcomes {
		if o.ID == id {
			return o.Permanent
		}
	}

	return false
}
