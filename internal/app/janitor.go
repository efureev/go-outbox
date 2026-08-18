package app

import (
	"context"
	"log/slog"
	"sync"

	"github.com/efureev/appmod/adapters/hubmod"
	"github.com/efureev/appmod/v4"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/store"
)

// janitorModule runs the periodic work that belongs to the deployment rather
// than to any one replica.
type janitorModule struct {
	*appmod.BaseAppModule

	cfg    config.Config
	log    *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newJanitorModule(cfg config.Config, log *slog.Logger) *janitorModule {
	m := &janitorModule{
		BaseAppModule: appmod.New(appmod.WithConfig(appmod.NewConfig(ModuleJanitor, "v1"))),
		cfg:           cfg,
		log:           log.With(slog.String("component", ModuleJanitor)),
	}

	m.AfterStart(m.start)
	m.BeforeDestroy(m.stop)

	return m
}

//nolint:contextcheck // the housekeeping loop outlives the hook that starts it
func (m *janitorModule) start(_ context.Context, _ appmod.HookModule) error {
	if !m.cfg.Janitor.Enabled {
		m.log.Info("housekeeping disabled")

		return nil
	}

	registry := m.AppContext().Registry

	st, err := appmod.Require[*store.Store](registry)
	if err != nil {
		return err
	}

	hub, err := hubmod.Require(registry)
	if err != nil {
		return err
	}

	janitor := dispatch.NewJanitor(st, events.NewEmitter(hub, m.log), m.cfg, m.log)

	// As in the dispatcher: the loop outlives the hook that starts it, so it
	// cannot borrow the start context's lifetime.
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	m.wg.Add(1)

	go func() {
		defer m.wg.Done()

		if err := janitor.Run(ctx); err != nil {
			m.log.Error("housekeeping stopped", slog.Any("error", err))
		}
	}()

	m.log.Info("housekeeping running",
		slog.Duration("reclaim_interval", m.cfg.Janitor.ReclaimInterval),
		slog.Duration("stats_interval", m.cfg.Janitor.StatsInterval),
		slog.Duration("retention", m.cfg.Janitor.Retention),
	)

	return nil
}

func (m *janitorModule) stop(ctx context.Context, _ appmod.HookModule) error {
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
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
