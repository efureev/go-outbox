package app

import (
	"context"
	"log/slog"

	"github.com/efureev/appmod/v4"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/tracing"
)

// tracingModule owns the trace exporter and hands the dispatcher a tracer.
//
// It is registered whether or not a collector is configured. With none, it
// provides a tracer that records nothing, so no other module has to ask whether
// tracing is on — which is the difference between one branch here and a branch
// on every publish.
type tracingModule struct {
	*appmod.BaseAppModule

	cfg      config.Config
	log      *slog.Logger
	version  versionInfo
	tracer   *tracing.Tracer
	shutdown func(context.Context) error
}

func newTracingModule(cfg config.Config, log *slog.Logger, version versionInfo) *tracingModule {
	m := &tracingModule{
		BaseAppModule: appmod.New(appmod.WithConfig(appmod.NewConfig(ModuleTracing, "v1"))),
		cfg:           cfg,
		log:           log.With(slog.String(logging.ModuleKey, ModuleTracing)),
		version:       version,
	}

	m.BeforeStart(m.build)

	return m
}

func (m *tracingModule) build(ctx context.Context, _ appmod.HookModule) error {
	tracer, shutdown, err := tracing.New(ctx, m.cfg.OTel, tracing.Service{
		Name:     m.cfg.App.Name,
		Version:  m.version.Version,
		Instance: m.cfg.App.Instance,
	})
	if err != nil {
		return err
	}

	m.tracer, m.shutdown = tracer, shutdown

	registry := m.AppContext().Registry
	if err := appmod.Provide[*tracing.Tracer](registry, tracer); err != nil {
		return err
	}

	m.AddCleanup(func(ctx context.Context) error {
		_, err := appmod.Revoke[*tracing.Tracer](registry)

		return err
	})

	// Spans are batched, so whatever the last iterations produced is still in
	// memory when a shutdown begins. Flushing is worth the moment it takes: the
	// traces most worth having are the ones from just before something stopped.
	m.AddCleanup(m.shutdown)

	if tracer.Enabled() {
		m.log.Info("tracing enabled",
			slog.String("endpoint", m.cfg.OTel.Endpoint),
			slog.Float64("sampling", m.cfg.OTel.Sampling),
		)
	}

	return nil
}

// Tracer exposes the tracer for tests.
func (m *tracingModule) Tracer() *tracing.Tracer { return m.tracer }
