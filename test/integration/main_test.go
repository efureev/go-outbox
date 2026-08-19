//go:build integration

// Package integration exercises the dispatcher against real infrastructure.
//
// It carries the weight of the test suite on purpose. The product is a set of
// concurrency and ownership rules expressed in SQL, and a mock that matches
// query text can only assert that the SQL has not changed — not that it is
// right. Every claim below runs against PostgreSQL.
//
// Run with:
//
//	docker compose up -d postgres
//	go test -tags integration ./test/integration/...
//
// The database is taken from OUTBOX_TEST_DSN, defaulting to the compose file's
// PostgreSQL. Each test gets a schema of its own, so tests are independent and
// may run in parallel.
package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/store"
	"github.com/efureev/go-outbox/internal/store/migrate"
)

const defaultDSN = "postgres://outbox:outbox@localhost:55432/outbox?sslmode=disable"

var schemaCounter atomic.Int64

func dsn() string {
	if v := os.Getenv("OUTBOX_TEST_DSN"); v != "" {
		return v
	}

	return defaultDSN
}

// TestMain fails loudly rather than skipping when the database is missing.
// A test suite that silently passes because its infrastructure is absent is
// worse than one that fails.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	conn, err := pgx.Connect(ctx, dsn())
	cancel()

	if err != nil {
		fmt.Fprintf(os.Stderr,
			"integration tests need PostgreSQL at %s\n"+
				"start it with: docker compose up -d postgres\n"+
				"or point OUTBOX_TEST_DSN elsewhere\n\n%v\n", dsn(), err)
		os.Exit(1)
	}
	_ = conn.Close(context.Background())

	os.Exit(m.Run())
}

// fixture is one test's isolated database schema.
type fixture struct {
	Pool   *pgxpool.Pool
	Store  *store.Store
	Config config.Config
	Schema string
}

// newFixture creates a schema, migrates it and registers its teardown.
func newFixture(t testing.TB) *fixture {
	t.Helper()

	schema := fmt.Sprintf("outbox_test_%d_%d", os.Getpid(), schemaCounter.Add(1))

	cfg := baseConfig(schema)

	ctx := t.Context()

	pool, err := store.NewPool(ctx, cfg.DB, "outbox-test")
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}

	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()

		if _, err := pool.Exec(dropCtx, fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema)); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
		pool.Close()
	})

	migrateFixture(t, cfg)

	return &fixture{
		Pool:   pool,
		Store:  store.New(pool, cfg.DB),
		Config: cfg,
		Schema: schema,
	}
}

// newBareFixture is newFixture without the migrations, for the one case that
// has to create the table itself: a partitioned outbox, which an operator
// creates before the migration set runs over it.
func newBareFixture(t testing.TB) *fixture {
	t.Helper()

	schema := fmt.Sprintf("outbox_test_%d_%d", os.Getpid(), schemaCounter.Add(1))
	cfg := baseConfig(schema)
	ctx := t.Context()

	pool, err := store.NewPool(ctx, cfg.DB, "outbox-test")
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}

	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()

		if _, err := pool.Exec(dropCtx, fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema)); err != nil {
			t.Logf("drop schema %s: %v", schema, err)
		}
		pool.Close()
	})

	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %q", schema)); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	return &fixture{
		Pool:   pool,
		Store:  store.New(pool, cfg.DB),
		Config: cfg,
		Schema: schema,
	}
}

func baseConfig(schema string) config.Config {
	cfg := config.Config{}
	cfg.DB.DSN = dsn()
	cfg.DB.Schema = schema
	cfg.DB.Table = "messages"
	cfg.DB.MaxConns = 12
	cfg.DB.MinConns = 1
	cfg.DB.ConnectTimeout = 10 * time.Second
	cfg.DB.MaxConnLifetime = time.Hour
	cfg.DB.MaxConnIdleTime = 30 * time.Minute
	cfg.DB.MigrationLockKey = 918273645
	cfg.Dispatch.NotifyChannel = "outbox_test_" + strings.ReplaceAll(schema, "-", "_")

	return cfg
}

func migrateFixture(t testing.TB, cfg config.Config) {
	t.Helper()

	conn := rawConn(t, cfg)

	if _, err := migrate.Run(t.Context(), conn, migrateOptions(cfg)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}

func migrateOptions(cfg config.Config) migrate.Options {
	return migrate.Options{
		Schema:      cfg.DB.Schema,
		Table:       cfg.DB.Table,
		Channel:     cfg.Dispatch.NotifyChannel,
		LockKey:     cfg.DB.MigrationLockKey,
		LockTimeout: 30 * time.Second,
	}
}

// rawConn opens a standalone connection, closed when the test ends.
func rawConn(t testing.TB, cfg config.Config) *pgx.Conn {
	t.Helper()

	conn, err := pgx.Connect(t.Context(), cfg.DB.DSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close(context.WithoutCancel(t.Context()))
	})

	return conn
}
