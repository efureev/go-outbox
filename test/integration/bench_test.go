//go:build integration

package integration

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/broker/rabbitmq"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
)

// Benchmarks for the numbers quoted in CHANGELOG.md.
//
// They carry the integration build tag, so `go test ./...` never sees them and
// CI pays nothing for them. Run them deliberately:
//
//	make up
//	make bench
//
// b.N is the message count, and only the drain is timed — the schema, the
// migrations, the broker connection and the seeding all happen with the timer
// stopped. Reported alongside ns/op:
//
//	msg/s   throughput, which is the figure worth quoting
//	claims  claim round trips, showing how well batching is working
//
// Fix the message count rather than letting the harness pick one, or the ramp
// spends most of its time rebuilding fixtures for tiny values of b.N:
//
//	go test -tags integration -run '^$' -bench BenchmarkDrain -benchtime 5000x ./test/integration/
//
// A drain benchmark is not a microbenchmark: it measures this process, the
// database and the broker together, on whatever hardware is to hand. Compare
// runs on one machine; do not read the absolute figures as a capacity plan.

// benchSink is the destination a drain benchmark publishes to.
type benchSink struct {
	name string
	// router is built once per benchmark, after the fixture exists.
	router func(tb testing.TB, f *fixture, tune ...string) dispatch.Router
}

var benchSinks = []benchSink{
	// The dispatcher and PostgreSQL alone: claim, write back, and the work in
	// between, with the broker taken out of the picture. This is the part the
	// project actually controls, so it is the one to watch for regressions.
	{"postgres", func(testing.TB, *fixture, ...string) dispatch.Router { return discardRouter{} }},

	// End to end, publisher confirms and all. Dominated by the broker.
	{"rabbitmq", benchRabbitRouter},
}

var benchWorkers = []int{1, 4, 8}

// BenchmarkDrain measures sustained delivery: how fast a backlog is cleared.
func BenchmarkDrain(b *testing.B) {
	for _, sink := range benchSinks {
		for _, workers := range benchWorkers {
			b.Run(fmt.Sprintf("%s/workers=%d", sink.name, workers), func(b *testing.B) {
				benchmarkDrain(b, sink, workers)
			})
		}
	}
}

// BenchmarkDrainBatchSize shows what the claim batch buys, holding the worker
// count fixed. Larger batches amortise the claim round trip over more messages,
// up to the point where a batch stops fitting inside the lease.
func BenchmarkDrainBatchSize(b *testing.B) {
	for _, size := range []int{25, 100, 200, 500} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			benchmarkDrain(b, benchSinks[0], 4, "OUTBOX_DISPATCH_BATCH_SIZE="+itoa(size))
		})
	}
}

// BenchmarkDrainChannels answers the tuning question the worker sweep raises:
// throughput stops climbing once the workers outnumber the driver's publish
// channels, so does widening the pool move it again? Workers are held at eight
// and the pool is swept underneath them.
func BenchmarkDrainChannels(b *testing.B) {
	for _, channels := range []int{1, 4, 8, 16} {
		b.Run(fmt.Sprintf("channels=%d", channels), func(b *testing.B) {
			benchmarkDrain(b, benchSinks[1], 8, "OUTBOX_DRIVER_RMQ_CHANNELS="+itoa(channels))
		})
	}
}

func benchmarkDrain(b *testing.B, sink benchSink, workers int, tune ...string) {
	b.Helper()

	f := newFixture(b)
	router := sink.router(b, f, tune...)

	cfg := benchConfig(b, f, append([]string{
		"OUTBOX_DISPATCH_WORKERS=" + itoa(workers),
		"OUTBOX_DISPATCH_BATCH_SIZE=200",
	}, tune...)...)

	counter := newDeliveryCounter()
	pipeline := dispatch.New("local", f.Store, router, counter, cfg, logging.Nop())

	seedBulk(b, f, b.N)

	ctx, cancel := context.WithTimeout(b.Context(), 10*time.Minute)
	defer cancel()

	// Everything above is setup, however long it took.
	b.ResetTimer()

	started := time.Now()

	done := make(chan error, 1)
	go func() { done <- pipeline.Run(ctx) }()

	counter.await(b, int64(b.N), 10*time.Minute)
	elapsed := time.Since(started)

	b.StopTimer()

	cancel()
	<-done

	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
	b.ReportMetric(float64(counter.claims.Load()), "claims")
}

// BenchmarkNotifyLatency measures the other half of the story: how long one
// message takes to reach the broker when the pipeline is otherwise idle.
//
// Each iteration inserts exactly one message and waits for that message before
// inserting the next, so ns/op is the latency a producer actually sees.
// Inserting the whole run first and waiting for the total would report
// amortised throughput wearing a latency label.
//
// Two configurations, because the answer is mostly a configuration choice:
// "defaults" is what a deployment gets out of the box, and "tuned" is the floor
// once the debounce window and the replica jitter are taken out.
//
//	go test -tags integration -run '^$' -bench BenchmarkNotifyLatency -benchtime 200x ./test/integration/
func BenchmarkNotifyLatency(b *testing.B) {
	configurations := []struct {
		name string
		tune []string
	}{
		{"defaults", nil},
		{"tuned", []string{
			"OUTBOX_DISPATCH_NOTIFY_DEBOUNCE=1ms",
			"OUTBOX_DISPATCH_NOTIFY_JITTER=0",
		}},
	}

	for _, c := range configurations {
		b.Run(c.name, func(b *testing.B) {
			benchmarkLatency(b, c.tune...)
		})
	}
}

func benchmarkLatency(b *testing.B, tune ...string) {
	b.Helper()

	f := newFixture(b)

	cfg := benchConfig(b, f, append([]string{
		"OUTBOX_DISPATCH_WORKERS=1",
		"OUTBOX_DISPATCH_BATCH_SIZE=10",
		// An hour, so nothing can arrive by way of the poll loop: a result here
		// can only have come through the notification path.
		"OUTBOX_DISPATCH_POLL_INTERVAL=1h",
		"OUTBOX_DISPATCH_NOTIFY_ENABLED=true",
	}, tune...)...)
	cfg.Dispatch.NotifyChannel = f.Config.Dispatch.NotifyChannel

	counter := newDeliveryCounter()
	pipeline := dispatch.New("local", f.Store, discardRouter{}, counter, cfg, logging.Nop())
	listener := dispatch.NewListener(f.Pool, cfg.Dispatch, []dispatch.Waker{pipeline}, logging.Nop())

	ctx, cancel := context.WithTimeout(b.Context(), 10*time.Minute)
	defer cancel()

	go func() { _ = pipeline.Run(ctx) }()
	go func() { _ = listener.Run(ctx) }()

	// Let the listener take its connection and issue LISTEN before anything is
	// measured; until it has, the only path is the hour-long tick.
	time.Sleep(500 * time.Millisecond)

	b.ResetTimer()

	for i := range b.N {
		f.insert(b, "local", "bench.latency", []byte(`{}`), nil)
		counter.await(b, int64(i+1), 30*time.Second)
	}

	b.StopTimer()
}

// deliveryCounter watches the pipeline's own events instead of polling the
// database for a count. Polling would fold its own interval into every
// measurement; the events are exact and cost nothing.
type deliveryCounter struct {
	delivered atomic.Int64
	claims    atomic.Int64

	// signal is nudged after every iteration. Buffered and sent to without
	// blocking, so a waiter that is behind never slows the pipeline down.
	signal chan struct{}
}

func newDeliveryCounter() *deliveryCounter {
	return &deliveryCounter{signal: make(chan struct{}, 1)}
}

func (c *deliveryCounter) Iteration(_ context.Context, ev events.Iteration) {
	c.claims.Add(1)
	c.delivered.Add(int64(len(ev.Delivered)))

	select {
	case c.signal <- struct{}{}:
	default:
	}
}

// await blocks until at least target messages have been delivered.
func (c *deliveryCounter) await(tb testing.TB, target int64, within time.Duration) {
	tb.Helper()

	deadline := time.After(within)

	for c.delivered.Load() < target {
		select {
		case <-c.signal:
		case <-deadline:
			tb.Fatalf("only %d of %d messages were delivered within %s",
				c.delivered.Load(), target, within)
		}
	}
}

// discardRouter accepts everything and does nothing, so a benchmark can measure
// the dispatcher without the broker in the way.
type discardRouter struct{}

func (discardRouter) Publish(_ context.Context, _ string, msgs []core.Message) []error {
	return make([]error, len(msgs))
}

func (discardRouter) DriverFor(string) (string, bool) { return "discard", true }

func benchRabbitRouter(tb testing.TB, f *fixture, tune ...string) dispatch.Router {
	tb.Helper()

	cfg := benchConfig(tb, f, tune...)

	driver, ok := cfg.Brokers.Drivers["rmq"].(*config.RabbitMQDriver)
	if !ok {
		tb.Fatalf("driver is %T, want *config.RabbitMQDriver", cfg.Brokers.Drivers["rmq"])
	}

	publisher, err := rabbitmq.New(tb.Context(), driver, logging.Nop())
	if err != nil {
		tb.Fatalf("connect to rabbitmq: %v", err)
	}
	tb.Cleanup(func() { _ = publisher.Close(context.WithoutCancel(tb.Context())) })

	router, err := broker.NewRouter(cfg.Brokers, map[string]broker.Publisher{"rmq": publisher})
	if err != nil {
		tb.Fatalf("router: %v", err)
	}

	return router
}

func benchConfig(tb testing.TB, f *fixture, tune ...string) config.Config {
	tb.Helper()

	pairs := append([]string{
		"OUTBOX_DB_DSN=" + f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA=" + f.Schema,
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN=" + amqpDSN(),
		"OUTBOX_DRIVER_RMQ_DECLARE=true",
		"OUTBOX_DRIVER_RMQ_PREFIX=" + uniqueName("bench"),
		"OUTBOX_DISPATCH_POLL_INTERVAL=5ms",
		"OUTBOX_DISPATCH_NOTIFY_ENABLED=false",
	}, tune...)

	cfg, err := config.LoadFrom(env(tb, pairs...))
	if err != nil {
		tb.Fatalf("load config: %v", err)
	}

	return cfg
}

// seedBulk writes n messages in one statement. Inserting them one at a time
// would take longer than the drain being measured.
func seedBulk(tb testing.TB, f *fixture, n int) {
	tb.Helper()

	_, err := f.Pool.Exec(tb.Context(), fmt.Sprintf(`
		INSERT INTO %q.messages (id, stream, topic, payload, target)
		SELECT gen_random_uuid(), 'local', 'bench', $1::bytea, '{}'::jsonb
		  FROM generate_series(1, $2)`, f.Schema),
		[]byte(`{"benchmark":true}`), n)
	if err != nil {
		tb.Fatalf("seed %d messages: %v", n, err)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
