// Package app wires the program together: it builds each subsystem as an
// appmod module, declares the order between them and hands the graph to a
// Manager, which starts independent modules concurrently and tears them down
// in reverse.
//
// There is no dependency-injection container. appmod's Registry carries the
// handful of contracts modules share, and the wiring below is the whole
// composition root — readable top to bottom, unlike the previous version's
// list of fx providers whose order and resolvability were implicit (two of
// which, as it turned out, could not be resolved at all).
package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/efureev/appmod/v4"

	"github.com/efureev/go-outbox/internal/config"
)

// Module names, used both for registration and to declare dependencies.
const (
	ModuleHub      = "hub"
	ModuleDB       = "db"
	ModuleBrokers  = "brokers"
	ModuleMetrics  = "metrics"
	ModuleDispatch = "dispatch"
	ModuleJanitor  = "janitor"
	ModuleHTTP     = "http"
)

// Build describes the binary, for the version endpoint.
type Build struct {
	Version string
	Commit  string
	Date    string
}

// App is the assembled program.
type App struct {
	cfg     config.Config
	log     *slog.Logger
	mgr     *appmod.Manager
	version versionInfo
}

// New assembles the module graph. It does not start anything.
func New(cfg config.Config, log *slog.Logger, build Build) (*App, error) {
	mgr := appmod.NewManager(
		appmod.WithLogger(log.With(slog.String("component", "manager"))),
		appmod.WithShutdownTimeout(cfg.App.ShutdownTimeout),
	)

	a := &App{
		cfg: cfg,
		log: log,
		mgr: mgr,
		version: versionInfo{
			Version: build.Version,
			Commit:  build.Commit,
			Built:   build.Date,
		},
	}

	if err := a.register(); err != nil {
		return nil, err
	}

	return a, nil
}

func (a *App) register() error {
	type entry struct {
		name   string
		module appmod.AppModule
		deps   []string
	}

	httpMod := newHTTPModule(a.cfg, a.log)
	httpMod.SetVersion(a.version)
	// The readiness endpoint reports the Manager's own view of module health,
	// which a module inside the graph has no other way to reach.
	httpMod.SetHealthProbe(a.mgr.Health)

	entries := []entry{
		{ModuleHub, newHubModule(a.log), nil},
		{ModuleDB, newDBModule(a.cfg, a.log), nil},
		{ModuleBrokers, newBrokersModule(a.cfg, a.log), nil},
		{ModuleMetrics, newMetricsModule(a.cfg, a.log), []string{ModuleHub}},
		{ModuleDispatch, newDispatchModule(a.cfg, a.log), []string{ModuleDB, ModuleBrokers, ModuleHub, ModuleMetrics}},
		{ModuleJanitor, newJanitorModule(a.cfg, a.log), []string{ModuleDB, ModuleHub, ModuleMetrics}},
		{ModuleHTTP, httpMod, []string{ModuleDB, ModuleMetrics}},
	}

	for _, e := range entries {
		if e.module == nil {
			continue
		}
		if err := a.mgr.Register(e.name, e.module, e.deps...); err != nil {
			return fmt.Errorf("register module %q: %w", e.name, err)
		}
	}

	return nil
}

// Run starts every module, waits for a termination signal or for ctx to be
// canceled, then stops them in reverse dependency order within the configured
// shutdown timeout.
func (a *App) Run(ctx context.Context) error { return a.mgr.Run(ctx) }

// ExitCode reports the code the process should exit with: 128+signum when a
// signal ended the run, so a supervisor can tell a clean stop from a kill.
func (a *App) ExitCode() int { return a.mgr.ExitCode() }

// Health probes every started module that reports its health. It backs the
// readiness endpoint.
func (a *App) Health(ctx context.Context) error { return a.mgr.Health(ctx) }

// Registry exposes the shared contract registry, for tests and for the HTTP
// layer to reach subsystems it does not own.
func (a *App) Registry() *appmod.Registry { return a.mgr.Registry() }
