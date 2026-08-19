//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/efureev/go-outbox/internal/broker/postgres"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/logging"
)

// Use case 8: the inbox lives in another database. Not another schema — another
// database, which is what makes the delivery and the write-back two separate
// transactions and the primary key the only thing absorbing a repeat.
//
// Technically identical to another instance; what a separate host adds is
// network and credentials, not different code.

// remoteInbox creates a second database with an inbox in it and returns the DSN
// that reaches it.
func remoteInbox(t *testing.T, name string) string {
	t.Helper()

	admin, err := pgxpool.New(t.Context(), dsn())
	if err != nil {
		t.Fatalf("connect to create %s: %v", name, err)
	}
	defer admin.Close()

	// CREATE DATABASE cannot run inside a transaction, and dropping needs the
	// pool that used it to be gone first.
	_, _ = admin.Exec(t.Context(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	if _, err := admin.Exec(t.Context(), "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}

	remote := strings.Replace(dsn(), "/outbox?", "/"+name+"?", 1)
	if remote == dsn() {
		t.Fatalf("could not point a DSN at %s from %q", name, dsn())
	}

	pool, err := pgxpool.New(t.Context(), remote)
	if err != nil {
		t.Fatalf("connect to %s: %v", name, err)
	}

	_, err = pool.Exec(t.Context(), `
		CREATE SCHEMA inbox;
		CREATE TABLE inbox.orders (
		    id      UUID PRIMARY KEY,
		    stream  TEXT  NOT NULL,
		    topic   TEXT  NOT NULL,
		    payload BYTEA NOT NULL,
		    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
		    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
		    processed_at TIMESTAMPTZ
		)`)
	if err != nil {
		pool.Close()
		t.Fatalf("create the remote inbox: %v", err)
	}
	pool.Close()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()

		cleanup, err := pgxpool.New(ctx, dsn())
		if err != nil {
			t.Logf("drop %s: %v", name, err)

			return
		}
		defer cleanup.Close()

		if _, err := cleanup.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Logf("drop %s: %v", name, err)
		}
	})

	return remote
}

func remoteDriver(t *testing.T, f *fixture, remote string) *config.PostgresDriver {
	t.Helper()

	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_DSN="+f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA="+f.Schema,
		"OUTBOX_STREAMS=billing",
		"OUTBOX_STREAM_BILLING_DRIVER=inb",
		"OUTBOX_DRIVER_INB_TYPE=postgres",
		"OUTBOX_DRIVER_INB_DSN="+remote,
		"OUTBOX_DRIVER_INB_SCHEMA=inbox",
		"OUTBOX_DRIVER_INB_TABLE=orders",
		"OUTBOX_DRIVER_INB_PREFIX=orders",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	d, ok := cfg.Brokers.Drivers["inb"].(*config.PostgresDriver)
	if !ok {
		t.Fatalf("driver is %T", cfg.Brokers.Drivers["inb"])
	}
	if d.SameDatabase {
		t.Fatal("an explicit DSN was taken for the dispatcher's own database")
	}

	return d
}

func remoteRows(t *testing.T, remote, query string) int {
	t.Helper()

	pool, err := pgxpool.New(t.Context(), remote)
	if err != nil {
		t.Fatalf("connect to the remote inbox: %v", err)
	}
	defer pool.Close()

	var n int
	if err := pool.QueryRow(t.Context(), query).Scan(&n); err != nil {
		t.Fatalf("query the remote inbox: %v", err)
	}

	return n
}

func TestDeliveryIntoAnotherDatabase(t *testing.T) {
	f := newFixture(t)
	remote := remoteInbox(t, "outbox_remote_delivery")
	driver := remoteDriver(t, f, remote)

	pub, err := postgres.New(t.Context(), driver, logging.Nop())
	if err != nil {
		t.Fatalf("open the remote publisher: %v", err)
	}
	defer func() { _ = pub.Close(context.Background()) }()

	msgs := []core.Message{
		{ID: "0198f0a0-0000-7000-8000-000000000101", Stream: "billing", Topic: "order.paid",
			Payload: []byte(`{"n":1}`), CreatedAt: time.Now()},
		{ID: "0198f0a0-0000-7000-8000-000000000102", Stream: "billing", Topic: "order.refunded",
			Payload: []byte(`{"n":2}`), CreatedAt: time.Now()},
	}

	// Delivered twice: across two databases the insert and the write-back are
	// two commits, so a repeat after a replica dies is the expected case.
	for round := 1; round <= 2; round++ {
		for i, err := range pub.Publish(t.Context(), msgs) {
			if err != nil {
				t.Fatalf("round %d, message %d: %v", round, i, err)
			}
		}
	}

	if got := remoteRows(t, remote, "SELECT count(*) FROM inbox.orders"); got != 2 {
		t.Errorf("the remote inbox holds %d rows after two deliveries of two messages, want 2", got)
	}

	// The prefix is part of what a consumer filters on, and it survives the
	// journey to another database like any other naming rule.
	if got := remoteRows(t, remote,
		"SELECT count(*) FROM inbox.orders WHERE topic LIKE 'orders.%'"); got != 2 {
		t.Errorf("%d rows carry the prefixed topic, want 2", got)
	}

	// Nothing was written into the dispatcher's own database by this driver.
	var here int
	if err := f.Pool.QueryRow(t.Context(),
		fmt.Sprintf("SELECT count(*) FROM %q.messages", f.Schema)).Scan(&here); err != nil {
		t.Fatalf("count local rows: %v", err)
	}
	if here != 0 {
		t.Errorf("%d rows appeared in the outbox database; the driver wrote to the wrong one", here)
	}
}

// The grant the recipe recommends is INSERT and SELECT, and nothing wider. If
// the driver needed more, that advice would be wrong and every reader following
// it would meet a permission error in production rather than here.
//
// SELECT is not there because the driver reads anything — it reads nothing. It
// is there because ON CONFLICT with a named target requires it, which is a fact
// about PostgreSQL that only a test like this one keeps the documentation
// honest about.
func TestTheDriverNeedsInsertAndSelectAndNothingWider(t *testing.T) {
	f := newFixture(t)
	remote := remoteInbox(t, "outbox_remote_grant")

	pool, err := pgxpool.New(t.Context(), remote)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	const role = "outbox_insert_only"
	_, _ = pool.Exec(t.Context(), "DROP OWNED BY "+role)
	_, _ = pool.Exec(t.Context(), "DROP ROLE IF EXISTS "+role)

	_, err = pool.Exec(t.Context(), fmt.Sprintf(`
		CREATE ROLE %[1]s LOGIN PASSWORD 'narrow';
		GRANT CONNECT ON DATABASE outbox_remote_grant TO %[1]s;
		GRANT USAGE          ON SCHEMA inbox TO %[1]s;
		GRANT INSERT, SELECT ON inbox.orders TO %[1]s;`, role))
	if err != nil {
		pool.Close()
		t.Fatalf("create the narrow role: %v", err)
	}
	pool.Close()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()

		p, err := pgxpool.New(ctx, remote)
		if err != nil {
			return
		}
		defer p.Close()

		_, _ = p.Exec(ctx, "DROP OWNED BY "+role)
		_, _ = p.Exec(ctx, "DROP ROLE IF EXISTS "+role)
	})

	narrow := strings.Replace(remote, "outbox:outbox@", role+":narrow@", 1)
	if narrow == remote {
		t.Fatalf("could not build a DSN for %s from %q", role, remote)
	}

	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_DSN="+f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA="+f.Schema,
		"OUTBOX_STREAMS=billing",
		"OUTBOX_STREAM_BILLING_DRIVER=inb",
		"OUTBOX_DRIVER_INB_TYPE=postgres",
		"OUTBOX_DRIVER_INB_DSN="+narrow,
		"OUTBOX_DRIVER_INB_SCHEMA=inbox",
		"OUTBOX_DRIVER_INB_TABLE=orders",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	// Startup verifies the table exists, and must manage that on INSERT alone.
	pub, err := postgres.New(t.Context(), cfg.Brokers.Drivers["inb"].(*config.PostgresDriver), logging.Nop())
	if err != nil {
		t.Fatalf("a role with only INSERT could not start the driver: %v", err)
	}
	defer func() { _ = pub.Close(context.Background()) }()

	err = pub.Publish(t.Context(), []core.Message{{
		ID: "0198f0a0-0000-7000-8000-000000000110", Stream: "billing", Topic: "order.paid",
		Payload: []byte("{}"), CreatedAt: time.Now(),
	}})[0]
	if err != nil {
		t.Fatalf("a role with only INSERT could not deliver: %v", err)
	}

	if got := remoteRows(t, remote, "SELECT count(*) FROM inbox.orders"); got != 1 {
		t.Errorf("the remote inbox holds %d rows, want 1", got)
	}

	// And the grant really is narrow: the role can put messages in and cannot
	// change or remove them.
	narrowPool, err := pgxpool.New(t.Context(), narrow)
	if err != nil {
		t.Fatalf("connect as %s: %v", role, err)
	}
	defer narrowPool.Close()

	for name, stmt := range map[string]string{
		"update": "UPDATE inbox.orders SET topic = 'x'",
		"delete": "DELETE FROM inbox.orders",
	} {
		if _, err := narrowPool.Exec(t.Context(), stmt); err == nil {
			t.Errorf("the dispatcher's role can %s the consumer's inbox", name)
		}
	}
}

// The unreachable half of the recipe: a database that is not there must defer
// without spending an attempt, exactly as an unreachable broker does.
func TestAnUnreachableRemoteInboxFailsTheStart(t *testing.T) {
	f := newFixture(t)

	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_DSN="+f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA="+f.Schema,
		"OUTBOX_STREAMS=billing",
		"OUTBOX_STREAM_BILLING_DRIVER=inb",
		"OUTBOX_DRIVER_INB_TYPE=postgres",
		"OUTBOX_DRIVER_INB_DSN=postgres://outbox:outbox@127.0.0.1:1/nothing?sslmode=disable&connect_timeout=2",
		"OUTBOX_DRIVER_INB_SCHEMA=inbox",
		"OUTBOX_DRIVER_INB_TABLE=orders",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if _, err := postgres.New(t.Context(), cfg.Brokers.Drivers["inb"].(*config.PostgresDriver), logging.Nop()); err == nil {
		t.Fatal("an unreachable destination started, so the failure would surface on a real message")
	}
}
