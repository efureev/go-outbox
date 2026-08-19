package app

import (
	"context"
	"log/slog"

	"github.com/efureev/appmod/v4"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
)

// dbModule owns the connection pool and publishes it for the modules that need
// one.
type dbModule struct {
	*appmod.BaseAppModule

	cfg   config.Config
	log   *slog.Logger
	pool  *pgxpool.Pool
	store *store.Store
}

func newDBModule(cfg config.Config, log *slog.Logger) *dbModule {
	m := &dbModule{
		BaseAppModule: appmod.New(appmod.WithConfig(appmod.NewConfig(ModuleDB, "v1"))),
		cfg:           cfg,
		log:           log.With(slog.String(logging.ModuleKey, ModuleDB)),
	}

	m.BeforeStart(m.open)
	m.AfterStart(m.publish)

	return m
}

func (m *dbModule) open(ctx context.Context, _ appmod.HookModule) error {
	// Migrating before the pool opens keeps the ordering obvious: nothing
	// queries a schema that has not been brought up to date. It is off by
	// default, because in production the schema usually belongs to the
	// producer's deployment rather than to this sidecar.
	if m.cfg.DB.AutoMigrate {
		applied, err := store.Migrate(ctx, m.cfg, m.log)
		if err != nil {
			return err
		}
		m.log.Info("migrations up to date", slog.Int("applied", len(applied)))
	}

	pool, err := store.NewPool(ctx, m.cfg.DB, m.cfg.App.Name)
	if err != nil {
		return err
	}

	// The release is registered next to the acquisition, so a later start hook
	// failing rolls this back without a separate teardown hook having to know
	// the pool was opened.
	m.AddCleanup(func(context.Context) error {
		pool.Close()

		return nil
	})

	m.pool = pool
	m.store = store.New(pool, m.cfg.DB)

	m.log.Info("database pool opened",
		slog.String("schema", m.cfg.DB.Schema),
		slog.String("table", m.cfg.DB.Table),
		slog.Int("max_conns", int(m.cfg.DB.MaxConns)),
	)

	return nil
}

func (m *dbModule) publish(_ context.Context, _ appmod.HookModule) error {
	registry := m.AppContext().Registry

	if err := appmod.Provide[*pgxpool.Pool](registry, m.pool); err != nil {
		return err
	}
	m.AddCleanup(func(context.Context) error {
		_, err := appmod.Revoke[*pgxpool.Pool](registry)

		return err
	})

	if err := appmod.Provide[*store.Store](registry, m.store); err != nil {
		return err
	}
	m.AddCleanup(func(context.Context) error {
		_, err := appmod.Revoke[*store.Store](registry)

		return err
	})

	return nil
}

// HealthCheck backs the readiness probe.
func (m *dbModule) HealthCheck(ctx context.Context) error {
	if m.pool == nil {
		return errNotStarted
	}

	return m.pool.Ping(ctx)
}

// Store exposes the outbox queries for tests.
func (m *dbModule) Store() *store.Store { return m.store }
