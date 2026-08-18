package app

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/efureev/appmod/adapters/hubmod"
	"github.com/efureev/appmod/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/observability"
)

// metricsModule owns the Prometheus registry and the listener that exposes it.
//
// The registry is an explicit value passed to whoever records into it, not the
// package-level default. The previous version declared its metrics as
// promauto globals, which registered into the default registry at init: shared
// mutable state that made tests order-dependent and left every metric
// registered whether or not the feature that fed it was enabled.
type metricsModule struct {
	*appmod.BaseAppModule

	cfg      config.Config
	log      *slog.Logger
	registry *prometheus.Registry
	metrics  *observability.Metrics
	stop     func(context.Context) error
}

func newMetricsModule(cfg config.Config, log *slog.Logger) *metricsModule {
	m := &metricsModule{
		BaseAppModule: appmod.New(appmod.WithConfig(appmod.NewConfig(ModuleMetrics, "v1"))),
		cfg:           cfg,
		log:           log.With(slog.String("component", ModuleMetrics)),
	}

	m.BeforeStart(m.build)
	m.AfterStart(m.publish)

	return m
}

func (m *metricsModule) build(ctx context.Context, _ appmod.HookModule) error {
	m.registry = prometheus.NewRegistry()
	m.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	// Series for the configured streams and drivers are created up front, so
	// /metrics reports a zero rather than nothing at all before the first
	// message — and so the label values stay bounded by the configuration
	// instead of by whatever a producer writes into the stream column.
	m.metrics = observability.New(m.registry, m.cfg.Brokers)

	if !m.cfg.Metrics.Enabled {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle(m.cfg.Metrics.Path, promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		Registry:          m.registry,
		EnableOpenMetrics: true,
	}))

	stop, err := serve(ctx, serveOptions{
		name:            "metrics",
		port:            m.cfg.Metrics.Port,
		handler:         mux,
		readTimeout:     10 * time.Second,
		writeTimeout:    30 * time.Second,
		shutdownTimeout: 5 * time.Second,
		log:             m.log,
	})
	if err != nil {
		return err
	}

	m.stop = stop
	m.AddCleanup(stop)

	return nil
}

func (m *metricsModule) publish(_ context.Context, _ appmod.HookModule) error {
	registry := m.AppContext().Registry

	hub, err := hubmod.Require(registry)
	if err != nil {
		return err
	}

	// Tied to this module's lifecycle, so a restart does not leave a second
	// handler counting every event twice.
	if err := observability.Subscribe(m.BaseAppModule, hub, m.metrics); err != nil {
		return err
	}

	if err := appmod.Provide[*observability.Metrics](registry, m.metrics); err != nil {
		return err
	}
	m.AddCleanup(func(context.Context) error {
		_, err := appmod.Revoke[*observability.Metrics](registry)

		return err
	})

	return nil
}

// Registry exposes the Prometheus registry for tests.
func (m *metricsModule) Registry() *prometheus.Registry { return m.registry }
