package migrate

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const defaultLockTimeout = 30 * time.Second

// lock takes the advisory lock that serialises concurrent runs, so several
// replicas starting at once apply the migrations exactly once between them.
//
// pg_try_advisory_lock in a loop rather than the blocking pg_advisory_lock:
// the blocking form ignores the deadline and would sit behind a statement
// timeout with nothing to report.
func lock(ctx context.Context, conn *pgx.Conn, opts Options) (func(), error) {
	timeout := opts.LockTimeout
	if timeout <= 0 {
		timeout = defaultLockTimeout
	}

	deadline := time.Now().Add(timeout)

	for {
		var acquired bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, opts.LockKey).Scan(&acquired); err != nil {
			return nil, fmt.Errorf("acquire migration lock: %w", err)
		}
		if acquired {
			var once sync.Once

			return func() {
				once.Do(func() {
					// A fresh context: the caller's may already be canceled, and
					// leaving a session-level advisory lock held would block
					// every later run until this connection is closed.
					releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
					defer cancel()

					_, _ = conn.Exec(releaseCtx, `SELECT pg_advisory_unlock($1)`, opts.LockKey)
				})
			}, nil
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("migration lock held by another instance for longer than %s", timeout)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// bootstrap creates the schema and the bookkeeping table. Both are idempotent,
// and neither belongs in a migration file: the runner needs them before it can
// read which migrations have been applied.
func bootstrap(ctx context.Context, conn *pgx.Conn, opts Options) error {
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + quote(opts.Schema),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.schema_migrations (
			version    INTEGER     PRIMARY KEY,
			name       TEXT        NOT NULL,
			checksum   TEXT        NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`, quote(opts.Schema)),
	}

	for _, stmt := range stmts {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("bootstrap migration bookkeeping: %w", err)
		}
	}

	return nil
}

func recorded(ctx context.Context, conn *pgx.Conn, opts Options) (map[int]Record, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT version, name, checksum, applied_at FROM %s.schema_migrations`, quote(opts.Schema)))
	if err != nil {
		return nil, fmt.Errorf("read applied migrations: %w", err)
	}
	defer rows.Close()

	out := map[int]Record{}
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.Version, &r.Name, &r.Checksum, &r.AppliedAt); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		out[r.Version] = r
	}

	return out, rows.Err()
}

// apply runs one migration and records it, in a single transaction: a partly
// applied migration that is nonetheless recorded is the failure mode a runner
// exists to prevent.
func apply(ctx context.Context, conn *pgx.Conn, opts Options, m migration) (Record, error) {
	var rec Record

	tx, err := conn.Begin(ctx)
	if err != nil {
		return rec, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, substitute(m.template, opts)); err != nil {
		return rec, err
	}

	err = tx.QueryRow(ctx, fmt.Sprintf(
		`INSERT INTO %s.schema_migrations (version, name, checksum)
		 VALUES ($1, $2, $3)
		 RETURNING version, name, checksum, applied_at`, quote(opts.Schema)),
		m.version, m.name, m.checksum,
	).Scan(&rec.Version, &rec.Name, &rec.Checksum, &rec.AppliedAt)
	if err != nil {
		return rec, fmt.Errorf("record migration: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return rec, fmt.Errorf("commit: %w", err)
	}

	return rec, nil
}

// substitute fills the schema, table and notification channel into a template.
// A placeholder cannot stand in for an identifier, so these are interpolated —
// which is why config.Validate checks each against a strict identifier pattern
// before anything reaches here.
func substitute(template string, opts Options) string {
	return strings.NewReplacer(
		"@schema@", opts.Schema,
		"@table@", opts.Table,
		"@channel@", opts.Channel,
	).Replace(template)
}

// quote wraps an identifier that has already been validated.
func quote(ident string) string { return `"` + ident + `"` }

func short(sum string) string {
	if len(sum) <= 12 {
		return sum
	}

	return sum[:12]
}
