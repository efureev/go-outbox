//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/efureev/msghub/v3"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/broker/rabbitmq"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
)

// harness is a fixture plus a live RabbitMQ pipeline for one stream.
type harness struct {
	*fixture

	Pipeline *dispatch.Pipeline
	Hub      *msghub.Hub
	Prefix   string

	mu         sync.Mutex
	iterations []events.Iteration
}

// newHarness wires the real store, the real broker driver and the real bus.
// Only the message contents are invented.
func newHarness(t *testing.T, tune ...string) *harness {
	t.Helper()

	f := newFixture(t)

	prefix := uniqueName("e2e")

	pairs := append([]string{
		"OUTBOX_DB_DSN=" + f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA=" + f.Schema,
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN=" + amqpDSN(),
		"OUTBOX_DRIVER_RMQ_DECLARE=true",
		"OUTBOX_DRIVER_RMQ_PREFIX=" + prefix,
		"OUTBOX_DISPATCH_BATCH_SIZE=50",
		"OUTBOX_DISPATCH_WORKERS=4",
		"OUTBOX_DISPATCH_POLL_INTERVAL=50ms",
	}, tune...)

	cfg, err := config.LoadFrom(env(t, pairs...))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	driver, ok := cfg.Brokers.Drivers["rmq"].(*config.RabbitMQDriver)
	if !ok {
		t.Fatalf("driver is %T", cfg.Brokers.Drivers["rmq"])
	}

	publisher, err := rabbitmq.New(t.Context(), driver, logging.Nop())
	if err != nil {
		t.Fatalf("connect to rabbitmq: %v", err)
	}
	t.Cleanup(func() { _ = publisher.Close(context.WithoutCancel(t.Context())) })

	router, err := broker.NewRouter(cfg.Brokers, map[string]broker.Publisher{"rmq": publisher})
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	hub := msghub.New(msghub.WithLogger(nil))
	t.Cleanup(func() { _ = hub.Close(context.WithoutCancel(t.Context())) })

	h := &harness{fixture: f, Hub: hub, Prefix: prefix}

	// Synchronous, so an assertion right after an iteration sees its event.
	// This is how the metrics observer is registered in production too: an
	// inline handler cannot drop a counter increment under backpressure.
	if _, err := msghub.Subscribe(hub, events.TopicIteration,
		func(_ context.Context, ev events.Iteration) error {
			h.mu.Lock()
			defer h.mu.Unlock()

			h.iterations = append(h.iterations, ev)

			return nil
		}, msghub.Synchronous()); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	h.Pipeline = dispatch.New("local", f.Store, router, events.NewEmitter(hub, logging.Nop()), cfg, logging.Nop())

	return h
}

func (h *harness) events() []events.Iteration {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]events.Iteration(nil), h.iterations...)
}

// queueFor is the effective broker name for a topic, which is what a consumer
// must subscribe to.
func (h *harness) queueFor(topic string) string { return h.Prefix + "_" + topic }

func TestEndToEndInsertReachesTheBroker(t *testing.T) {
	h := newHarness(t)

	const topic = "orders.placed"

	// A routing envelope, so the naming and key path is exercised too.
	id := h.insert(t, "local", topic, []byte(`{"order":"A-1"}`), &core.Target{Key: "customer-1"})

	claimed, err := h.Pipeline.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if claimed != 1 {
		t.Fatalf("claimed %d messages, want 1", claimed)
	}

	got := consumeOne(t, h.queueFor(topic))
	if string(got.Body) != `{"order":"A-1"}` {
		t.Errorf("body = %q, want the payload unchanged", got.Body)
	}
	if got.MessageId != id {
		t.Errorf("MessageId = %q, want the row id %q", got.MessageId, id)
	}

	if r := h.row(t, id); r.Status != core.StatusSent {
		t.Errorf("row is %s, want sent", r.Status)
	}

	evs := h.events()
	if len(evs) != 1 {
		t.Fatalf("%d iteration events, want 1", len(evs))
	}
	if len(evs[0].Delivered) != 1 || evs[0].Delivered[0].ID != id {
		t.Errorf("event delivered = %+v, want the one message", evs[0].Delivered)
	}
	if evs[0].Conflicts != 0 {
		t.Errorf("Conflicts = %d, want 0", evs[0].Conflicts)
	}
}

func TestEndToEndDrainsABacklogAcrossIterations(t *testing.T) {
	h := newHarness(t)

	const (
		topic = "bulk.events"
		total = 120 // more than the batch size of 50
	)

	for range total {
		h.insert(t, "local", topic, []byte(`{}`), nil)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- h.Pipeline.Run(ctx) }()

	waitFor(t, 30*time.Second, func() bool {
		return h.countByStatus(t, core.StatusSent) == total
	}, "all messages delivered")

	cancel()
	<-done

	if got := h.countByStatus(t, core.StatusPending); got != 0 {
		t.Errorf("%d messages left pending", got)
	}

	// A backlog larger than one batch must be drained by consecutive
	// iterations, not by waiting a poll interval between each.
	if len(h.events()) < 3 {
		t.Errorf("%d iterations for %d messages in batches of 50, want at least 3", len(h.events()), total)
	}
}

// A message whose stream has no driver must fail once and stop, rather than
// spending the whole retry budget rediscovering that the configuration has not
// changed.
func TestEndToEndUnknownStreamFailsImmediately(t *testing.T) {
	h := newHarness(t)

	// The pipeline serves "local"; claim from it but hand the router a stream
	// it does not know by writing the row into a differently named stream and
	// pointing the pipeline at that stream instead.
	id := h.insert(t, "ghost", "topic", []byte(`{}`), nil)

	ghost := dispatch.New("ghost", h.Store, ghostRouter{}, events.NewEmitter(h.Hub, logging.Nop()),
		mustConfig(t, h.fixture), logging.Nop())

	if _, err := ghost.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	r := h.row(t, id)
	if r.Status != core.StatusFailed {
		t.Errorf("row is %s, want failed on the first attempt", r.Status)
	}
	if r.Attempts != 1 {
		t.Errorf("attempts = %d, want 1: the rest of the budget must be untouched", r.Attempts)
	}
	if r.LastError == nil || *r.LastError == "" {
		t.Error("last_error is empty; the reason a message stopped must be recorded")
	}
}

// ghostRouter knows no streams at all.
type ghostRouter struct{}

func (ghostRouter) Publish(_ context.Context, _ string, msgs []core.Message) []error {
	out := make([]error, len(msgs))
	for i := range out {
		out[i] = core.Permanent("unknown stream", core.ErrUnknownStream)
	}

	return out
}

func (ghostRouter) DriverFor(string) (string, bool) { return "", false }

func mustConfig(t *testing.T, f *fixture) config.Config {
	t.Helper()

	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_DSN="+f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA="+f.Schema,
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN="+amqpDSN(),
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	return cfg
}

// A retryable failure must leave the message pending with its backoff applied,
// not consume it.
func TestEndToEndRetryableFailureSchedulesARetry(t *testing.T) {
	h := newHarness(t, "OUTBOX_DISPATCH_BACKOFF_BASE=30s", "OUTBOX_DISPATCH_BACKOFF_JITTER=0")

	id := h.insert(t, "local", "retry.me", []byte(`{}`), nil)

	failing := dispatch.New("local", h.Store, flakyRouter{err: errors.New("broker unavailable")},
		events.NewEmitter(h.Hub, logging.Nop()), mustConfig(t, h.fixture), logging.Nop())

	if _, err := failing.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	r := h.row(t, id)
	if r.Status != core.StatusPending {
		t.Errorf("row is %s, want pending for another attempt", r.Status)
	}
	if r.Attempts != 1 {
		t.Errorf("attempts = %d, want 1", r.Attempts)
	}
	if !r.AvailableAt.After(time.Now().Add(10 * time.Second)) {
		t.Errorf("available_at = %s, want a backoff of about a minute from now", r.AvailableAt)
	}
}

type flakyRouter struct{ err error }

func (f flakyRouter) Publish(_ context.Context, _ string, msgs []core.Message) []error {
	out := make([]error, len(msgs))
	for i := range out {
		out[i] = f.err
	}

	return out
}

func (flakyRouter) DriverFor(string) (string, bool) { return "rmq", true }

// waitFor polls a condition until it holds or the deadline passes.
func waitFor(t *testing.T, within time.Duration, cond func() bool, what string) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	t.Fatalf("timed out after %s waiting for: %s", within, what)
}
