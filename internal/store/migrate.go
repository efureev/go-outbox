package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/store/migrate"
)

const migrationLockTimeout = 2 * time.Minute

// Migrate applies the pending migrations and reports the ones it applied.
//
// It opens a connection of its own rather than borrowing one from the pool.
// The pool sets statement_timeout on every connection, which is right for the
// dispatcher's queries and wrong for a CREATE INDEX on a large table; a
// dedicated connection keeps the two settings from having to be one.
func Migrate(ctx context.Context, cfg config.Config, log *slog.Logger) ([]migrate.Record, error) {
	conn, err := pgx.Connect(ctx, dsn(cfg.DB))
	if err != nil {
		return nil, fmt.Errorf("connect for migrations: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	return migrate.Run(ctx, conn, migrationOptions(cfg, log))
}

// MigrationStatus reports what is recorded in the database and what this build
// ships, so the two can be compared.
func MigrationStatus(ctx context.Context, cfg config.Config) (applied, shipped []migrate.Record, err error) {
	conn, err := pgx.Connect(ctx, dsn(cfg.DB))
	if err != nil {
		return nil, nil, fmt.Errorf("connect for migrations: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	if applied, err = migrate.Status(ctx, conn, migrationOptions(cfg, nil)); err != nil {
		return nil, nil, err
	}
	if shipped, err = migrate.Pending(); err != nil {
		return nil, nil, err
	}

	return applied, shipped, nil
}

func migrationOptions(cfg config.Config, log *slog.Logger) migrate.Options {
	return migrate.Options{
		Schema:      cfg.DB.Schema,
		Table:       cfg.DB.Table,
		Channel:     cfg.Dispatch.NotifyChannel,
		LockKey:     cfg.DB.MigrationLockKey,
		LockTimeout: migrationLockTimeout,
		Logger:      log,
	}
}
