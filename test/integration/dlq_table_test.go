//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/efureev/msghub/v3"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/broker/postgres"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
)

// Use case 9 claims the dead-letter destination can be a table "without a single
// code change", because the forwarder publishes to an ordinary stream through
// the ordinary router. That was a documented claim with nothing behind it.
//
// This runs the whole path: a message the broker refuses permanently, the
// pipeline writing it off, the iteration event on the bus, the forwarder
// reading the row back and publishing it — into PostgreSQL.

// refusingRouter delivers to the dead-letter table and refuses everything else,
// which is the shape of a stream whose broker rejects what it is given.
type refusingRouter struct {
	dlq    broker.Publisher
	stream string
	reason error
}

func (r refusingRouter) Publish(ctx context.Context, stream string, msgs []core.Message) []error {
	if stream == r.stream {
		return r.dlq.Publish(ctx, msgs)
	}

	out := make([]error, len(msgs))
	for i := range out {
		out[i] = core.Permanent("unroutable", r.reason)
	}

	return out
}

func (r refusingRouter) DriverFor(stream string) (string, bool) {
	if stream == r.stream {
		return "dlq_table", true
	}

	return "rmq", true
}

func newDeadLetterTable(t *testing.T, f *fixture) *postgres.Publisher {
	t.Helper()

	_, err := f.Pool.Exec(t.Context(), fmt.Sprintf(`
		CREATE TABLE %s.dead_letter (
		    id      UUID PRIMARY KEY,
		    stream  TEXT  NOT NULL,
		    topic   TEXT  NOT NULL,
		    payload BYTEA NOT NULL,
		    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
		    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`, quoted(f.Schema)))
	if err != nil {
		t.Fatalf("create the dead-letter table: %v", err)
	}

	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_DSN="+f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA="+f.Schema,
		"OUTBOX_STREAMS=local,dead_letter",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN="+amqpDSN(),
		"OUTBOX_STREAM_DEAD_LETTER_DRIVER=dlq_table",
		"OUTBOX_DRIVER_DLQ_TABLE_TYPE=postgres",
		"OUTBOX_DRIVER_DLQ_TABLE_SCHEMA="+f.Schema,
		"OUTBOX_DRIVER_DLQ_TABLE_TABLE=dead_letter",
		"OUTBOX_DLQ_ENABLED=true",
		"OUTBOX_DLQ_STREAM=dead_letter",
		"OUTBOX_DLQ_TOPIC=outbox.dead-letter",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	driver, ok := cfg.Brokers.Drivers["dlq_table"].(*config.PostgresDriver)
	if !ok {
		t.Fatalf("driver is %T", cfg.Brokers.Drivers["dlq_table"])
	}

	pub, err := postgres.New(t.Context(), driver, logging.Nop())
	if err != nil {
		t.Fatalf("open the dead-letter publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close(context.Background()) })

	return pub
}

func TestDeadLettersLandInATable(t *testing.T) {
	f := newFixture(t)
	pub := newDeadLetterTable(t, f)

	cfg := mustConfig(t, f)
	cfg.Dispatch.MaxAttempts = 1
	cfg.DLQ = config.DLQConfig{Enabled: true, Stream: "dead_letter", Topic: "outbox.dead-letter"}

	router := refusingRouter{dlq: pub, stream: "dead_letter", reason: errors.New("no such exchange")}

	hub := msghub.New()
	emitter := events.NewEmitter(hub, logging.Nop())

	// Wired exactly as the application wires it: the forwarder is a subscriber
	// on the bus, not something the pipeline calls.
	forwarder := dispatch.NewDeadLetter(f.Store, router, emitter, cfg, logging.Nop())
	if _, err := msghub.Subscribe(hub, events.TopicIteration,
		forwarder.Handle, msghub.Synchronous()); err != nil {
		t.Fatalf("subscribe the forwarder: %v", err)
	}

	pipeline := dispatch.New("local", f.Store, router, emitter, cfg, logging.Nop())

	const count = 3
	for i := range count {
		f.insert(t, "local", fmt.Sprintf("order.%d", i), fmt.Appendf(nil, `{"n":%d}`, i), nil)
	}

	res, err := pipeline.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if res.Claimed != count {
		t.Fatalf("claimed %d, want %d", res.Claimed, count)
	}

	// Permanently refused, so they are failed rather than retried.
	if failed := f.countByStatus(t, core.StatusFailed); failed != count {
		t.Fatalf("%d messages failed, want %d", failed, count)
	}

	var forwarded int
	if err := f.Pool.QueryRow(t.Context(),
		fmt.Sprintf("SELECT count(*) FROM %s.dead_letter", quoted(f.Schema))).Scan(&forwarded); err != nil {
		t.Fatalf("count dead letters: %v", err)
	}
	if forwarded != count {
		t.Errorf("%d dead letters reached the table, want %d", forwarded, count)
	}
}

// The circumstances of the death are what make the table worth reading: without
// them it holds payloads nobody can place. This is the query the recipe shows.
func TestDeadLettersCarryTheirCircumstances(t *testing.T) {
	f := newFixture(t)
	pub := newDeadLetterTable(t, f)

	cfg := mustConfig(t, f)
	cfg.Dispatch.MaxAttempts = 1
	cfg.DLQ = config.DLQConfig{Enabled: true, Stream: "dead_letter", Topic: "outbox.dead-letter"}

	router := refusingRouter{dlq: pub, stream: "dead_letter", reason: errors.New("no such exchange")}

	hub := msghub.New()
	emitter := events.NewEmitter(hub, logging.Nop())

	forwarder := dispatch.NewDeadLetter(f.Store, router, emitter, cfg, logging.Nop())
	if _, err := msghub.Subscribe(hub, events.TopicIteration,
		forwarder.Handle, msghub.Synchronous()); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	pipeline := dispatch.New("local", f.Store, router, emitter, cfg, logging.Nop())

	f.insert(t, "local", "order.created", []byte(`{"order":"A-1"}`), nil)

	if _, err := pipeline.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	var (
		stream, topic, attempts, permanent, payload string
		storedTopic                                 string
	)
	err := f.Pool.QueryRow(t.Context(), fmt.Sprintf(`
		SELECT headers ->> 'x-outbox-original-stream',
		       headers ->> 'x-outbox-original-topic',
		       headers ->> 'x-outbox-attempts',
		       headers ->> 'x-outbox-permanent',
		       encode(payload, 'escape'),
		       topic
		  FROM %s.dead_letter`, quoted(f.Schema))).
		Scan(&stream, &topic, &attempts, &permanent, &payload, &storedTopic)
	if err != nil {
		t.Fatalf("read the dead letter: %v", err)
	}

	if stream != "local" || topic != "order.created" {
		t.Errorf("origin = %s/%s, want local/order.created", stream, topic)
	}
	if permanent != "true" {
		t.Errorf("x-outbox-permanent = %q, want true", permanent)
	}
	if attempts == "" {
		t.Error("the attempt count did not travel")
	}
	if payload != `{"order":"A-1"}` {
		t.Errorf("payload = %s, want it untouched", payload)
	}
	// Readdressed to the dead-letter topic, not the one that failed.
	if storedTopic != "outbox.dead-letter" {
		t.Errorf("topic = %q, want the dead-letter one", storedTopic)
	}
}

// The row in the outbox is the record; the table is a signal. A dead letter that
// went nowhere must not make the record harder to find, and forwarding must
// never change a status.
func TestForwardingLeavesTheOutboxRowAlone(t *testing.T) {
	f := newFixture(t)
	pub := newDeadLetterTable(t, f)

	cfg := mustConfig(t, f)
	cfg.Dispatch.MaxAttempts = 1
	cfg.DLQ = config.DLQConfig{Enabled: true, Stream: "dead_letter", Topic: "outbox.dead-letter"}

	router := refusingRouter{dlq: pub, stream: "dead_letter", reason: errors.New("gone")}

	hub := msghub.New()
	emitter := events.NewEmitter(hub, logging.Nop())

	forwarder := dispatch.NewDeadLetter(f.Store, router, emitter, cfg, logging.Nop())
	if _, err := msghub.Subscribe(hub, events.TopicIteration,
		forwarder.Handle, msghub.Synchronous()); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	pipeline := dispatch.New("local", f.Store, router, emitter, cfg, logging.Nop())

	id := f.insert(t, "local", "order.created", []byte("{}"), nil)

	if _, err := pipeline.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	row := f.row(t, id)
	if row.Status != core.StatusFailed {
		t.Errorf("the outbox row is %s; forwarding must not change a status", row.Status)
	}
	if row.LastError == nil || *row.LastError == "" {
		t.Error("the outbox row lost the reason it failed")
	}
}
