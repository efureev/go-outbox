package app

import (
	"context"
	"log/slog"
	"sync"

	"github.com/efureev/appmod/adapters/hubmod"
	"github.com/efureev/appmod/v4"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
	"github.com/efureev/go-outbox/internal/tracing"
)

// dispatchModule runs one pipeline per configured stream.
//
// Separate pipelines are the point. Processing every stream in one batch means
// a broker that is down does not merely delay its own messages: everything
// claimed behind them waits too.
type dispatchModule struct {
	*appmod.BaseAppModule

	cfg config.Config
	log *slog.Logger

	pipelines []*dispatch.Pipeline
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	started   bool
}

func newDispatchModule(cfg config.Config, log *slog.Logger) *dispatchModule {
	m := &dispatchModule{
		BaseAppModule: appmod.New(appmod.WithConfig(appmod.NewConfig(ModuleDispatch, "v1"))),
		cfg:           cfg,
		log:           log.With(slog.String(logging.ModuleKey, ModuleDispatch)),
	}

	m.AfterStart(m.start)
	m.BeforeDestroy(m.stop)

	return m
}

//nolint:contextcheck // the pipelines and the listener outlive the hook that starts them
func (m *dispatchModule) start(_ context.Context, _ appmod.HookModule) error {
	registry := m.AppContext().Registry

	st, err := appmod.Require[*store.Store](registry)
	if err != nil {
		return err
	}

	router, err := appmod.Require[*broker.Router](registry)
	if err != nil {
		return err
	}

	hub, err := hubmod.Require(registry)
	if err != nil {
		return err
	}

	tracer, err := appmod.Require[*tracing.Tracer](registry)
	if err != nil {
		return err
	}

	emitter := events.NewEmitter(hub, m.log)

	if m.cfg.DLQ.Enabled {
		// Asynchronous, unlike the metrics observer: forwarding reads from the
		// database and publishes to a broker, and neither belongs inside the
		// publisher's own loop. Blocking on a full queue rather than dropping,
		// because a failed message is exactly the one worth waiting for.
		forwarder := dispatch.NewDeadLetter(st, router, emitter, m.cfg, m.log)

		if err := hubmod.SubscribeModule(m.BaseAppModule, hub, events.TopicIteration,
			forwarder.Handle); err != nil {
			return err
		}

		m.log.Info("dead-letter forwarding enabled",
			slog.String("stream", m.cfg.DLQ.Stream),
			slog.String("topic", m.cfg.DLQ.Topic),
		)
	}

	for _, stream := range m.cfg.Brokers.StreamNames() {
		m.pipelines = append(m.pipelines,
			dispatch.New(stream, st, router, emitter, m.cfg, m.log, dispatch.WithTracer(tracer)))
	}

	// The pipelines run against a context of their own, canceled by the
	// teardown hook below. Deriving it from the start context would tie the
	// lifetime of the loops to the duration of the startup, which is not what
	// a start context means.
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	for _, p := range m.pipelines {
		m.wg.Add(1)

		go func(p *dispatch.Pipeline) {
			defer m.wg.Done()

			if err := p.Run(ctx); err != nil {
				m.log.Error("pipeline stopped", slog.String("stream", p.Stream()), slog.Any("error", err))
			}
		}(p)
	}

	if m.cfg.Dispatch.NotifyEnabled {
		m.startListener(ctx, st.Pool())
	}

	m.started = true
	m.log.Info("dispatcher running",
		slog.Int("pipelines", len(m.pipelines)),
		slog.Int("batch_size", m.cfg.Dispatch.BatchSize),
		slog.Int("workers", m.cfg.Dispatch.Workers),
		slog.Duration("lease_ttl", m.cfg.Dispatch.LeaseTTL),
	)

	return nil
}

// stop asks the pipelines to finish and waits for them.
//
// A pipeline in flight completes the batch it is publishing and hands back what
// it never started, so a clean shutdown leaves no claim stranded until its lease
// expires.
func (m *dispatchModule) stop(ctx context.Context, _ appmod.HookModule) error {
	if m.cancel == nil {
		return nil
	}

	m.cancel()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.log.Info("dispatcher drained")

		return nil
	case <-ctx.Done():
		// The shutdown budget ran out. Saying so is better than blocking the
		// rest of the teardown; the leases involved expire on their own.
		m.log.Warn("dispatcher did not drain within the shutdown budget")

		return ctx.Err()
	}
}

// startListener turns inserts into immediate wakeups. It reserves one pooled
// connection for as long as it runs, which is why the pool is sized with room
// to spare.
func (m *dispatchModule) startListener(ctx context.Context, pool *pgxpool.Pool) {
	wakers := make([]dispatch.Waker, len(m.pipelines))
	for i, p := range m.pipelines {
		wakers[i] = p
	}

	listener := dispatch.NewListener(pool, m.cfg.Dispatch, wakers, m.log)

	m.wg.Add(1)

	go func() {
		defer m.wg.Done()

		if err := listener.Run(ctx); err != nil {
			m.log.Error("notification listener stopped", slog.Any("error", err))
		}
	}()
}

// Pipelines exposes the running pipelines, so the notification listener can
// wake the right one.
func (m *dispatchModule) Pipelines() []*dispatch.Pipeline { return m.pipelines }

// HealthCheck reports whether the pipelines are running.
func (m *dispatchModule) HealthCheck(context.Context) error {
	if !m.started {
		return errNotStarted
	}

	return nil
}
