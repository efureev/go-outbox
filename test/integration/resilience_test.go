//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/broker/rabbitmq"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/store"
)

// What these establish is what happens after the process is running and a
// dependency goes away: which failures are contained, which stall, and what
// comes back on its own. Startup is a separate contract — every configured
// broker must be reachable or the process refuses to start — and is covered by
// TestUnreachableBrokerFailsTheStart below.

// hostOf extracts host:port from a postgres URL, for pointing a proxy at it.
func hostOf(tb testing.TB, pgURL string) string {
	tb.Helper()

	rest := pgURL[strings.Index(pgURL, "//")+2:]
	if at := strings.Index(rest, "@"); at >= 0 {
		rest = rest[at+1:]
	}
	if slash := strings.Index(rest, "/"); slash >= 0 {
		rest = rest[:slash]
	}

	return rest
}

// amqpHost extracts host:port from an amqp URL.
func amqpHost(tb testing.TB, url string) string {
	tb.Helper()

	return hostOf(tb, url)
}

// resilient is a dispatcher wired through proxies that a test can break.
type resilient struct {
	*fixture

	Store     *store.Store
	Pipelines map[string]*dispatch.Pipeline
	Brokers   map[string]*proxy
	DB        *proxy

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// newResilient builds one pipeline per stream, each publishing through its own
// proxy, and the dispatcher's database access through another.
func newResilient(t *testing.T, streams []string, tune ...string) *resilient {
	t.Helper()

	f := newFixture(t)

	r := &resilient{
		fixture:   f,
		Pipelines: map[string]*dispatch.Pipeline{},
		Brokers:   map[string]*proxy{},
	}

	// The dispatcher reaches PostgreSQL through a proxy; the fixture keeps its
	// direct pool, so the test can still observe the table during an outage.
	r.DB = newProxy(t, hostOf(t, f.Config.DB.DSN))
	proxiedDSN := strings.Replace(f.Config.DB.DSN, hostOf(t, f.Config.DB.DSN), r.DB.Addr(), 1)

	pairs := []string{
		"OUTBOX_DB_DSN=" + proxiedDSN,
		"OUTBOX_DB_SCHEMA=" + f.Schema,
		"OUTBOX_STREAMS=" + strings.Join(streams, ","),
		"OUTBOX_DISPATCH_POLL_INTERVAL=100ms",
		"OUTBOX_DISPATCH_NOTIFY_ENABLED=false",
		"OUTBOX_DISPATCH_BATCH_SIZE=50",
		"OUTBOX_DISPATCH_WORKERS=2",
		"OUTBOX_DISPATCH_PUBLISH_TIMEOUT=2s",
		"OUTBOX_DISPATCH_LEASE_TTL=10s",
		"OUTBOX_DISPATCH_BACKOFF_BASE=200ms",
		"OUTBOX_DISPATCH_BACKOFF_MAX=400ms",
		"OUTBOX_DISPATCH_BACKOFF_JITTER=0",
		// Finding a broker gone pauses claiming for that stream. These tests pass
		// at the shipped ceiling too; it is lowered so their recovery assertions
		// wait on the proxy being healed rather than on a half-minute timer that
		// has nothing to do with what they are testing.
		"OUTBOX_DISPATCH_PAUSE_MAX=200ms",
		"OUTBOX_DISPATCH_WRITE_BACK_TIMEOUT=5s",
	}

	prefix := uniqueName("res")

	for _, stream := range streams {
		p := newProxy(t, amqpHost(t, amqpDSN()))
		r.Brokers[stream] = p

		driver := "rmq_" + stream
		pairs = append(pairs,
			fmt.Sprintf("OUTBOX_STREAM_%s_DRIVER=%s", strings.ToUpper(stream), driver),
			fmt.Sprintf("OUTBOX_DRIVER_%s_TYPE=rabbitmq", strings.ToUpper(driver)),
			fmt.Sprintf("OUTBOX_DRIVER_%s_DSN=amqp://outbox:outbox@%s/", strings.ToUpper(driver), p.Addr()),
			fmt.Sprintf("OUTBOX_DRIVER_%s_DECLARE=true", strings.ToUpper(driver)),
			fmt.Sprintf("OUTBOX_DRIVER_%s_PREFIX=%s%s", strings.ToUpper(driver), prefix, stream),
			// Reconnect fast, so a test does not spend its budget waiting out
			// the production backoff.
			fmt.Sprintf("OUTBOX_DRIVER_%s_RECONNECT_DELAY=100ms", strings.ToUpper(driver)),
		)
	}

	cfg, err := config.LoadFrom(env(t, append(pairs, tune...)...))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	pool, err := store.NewPool(t.Context(), cfg.DB, "resilience-test")
	if err != nil {
		t.Fatalf("open the proxied pool: %v", err)
	}
	t.Cleanup(pool.Close)

	r.Store = store.New(pool, cfg.DB)

	publishers := map[string]broker.Publisher{}
	for name, driver := range cfg.Brokers.Drivers {
		d, ok := driver.(*config.RabbitMQDriver)
		if !ok {
			t.Fatalf("driver %q is %T", name, driver)
		}

		pub, err := rabbitmq.New(t.Context(), d, logging.Nop())
		if err != nil {
			t.Fatalf("connect %q: %v", name, err)
		}
		t.Cleanup(func() { _ = pub.Close(context.WithoutCancel(t.Context())) })

		publishers[name] = pub
	}

	router, err := broker.NewRouter(cfg.Brokers, publishers)
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	emitter := events.NewEmitter(nil, logging.Nop())
	for _, stream := range streams {
		r.Pipelines[stream] = dispatch.New(stream, r.Store, router, emitter, cfg, logging.Nop())
	}

	return r
}

// Start runs every pipeline until the test ends.
func (r *resilient) Start(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	for _, p := range r.Pipelines {
		r.wg.Add(1)

		go func(p *dispatch.Pipeline) {
			defer r.wg.Done()

			_ = p.Run(ctx)
		}(p)
	}

	t.Cleanup(func() {
		cancel()
		r.wg.Wait()
	})
}

// sentIn counts delivered messages for one stream, read through the fixture's
// direct pool so it works during a database outage of the dispatcher's own.
func (r *resilient) sentIn(t *testing.T, stream string) int {
	t.Helper()

	var n int
	err := r.Pool.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT count(*) FROM %q.messages WHERE stream = $1 AND status = 2`, r.Schema), stream).Scan(&n)
	if err != nil {
		t.Fatalf("count sent in %s: %v", stream, err)
	}

	return n
}

// feltAFailure counts messages in a stream that have a recorded failure, which
// is how a test tells a real outage from a lucky race.
//
// Two counters can move, and which one does is the point: a broker that rejects
// a message advances attempts, while one that could not be reached advances
// nothing and only stamps deferred_since. Asking about attempts alone would
// mean an outage — the very thing these tests arrange — no longer registers as
// having happened.
func (r *resilient) feltAFailure(t *testing.T, stream string) int {
	t.Helper()

	var n int
	err := r.Pool.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT count(*) FROM %q.messages
		  WHERE stream = $1 AND last_error IS NOT NULL
		    AND (attempts > 0 OR deferred_since IS NOT NULL)`, r.Schema), stream).Scan(&n)
	if err != nil {
		t.Fatalf("count failures in %s: %v", stream, err)
	}

	return n
}

func (r *resilient) statusCount(t *testing.T, status core.Status) int {
	t.Helper()

	return r.countByStatus(t, status)
}

// Two of three brokers go away while the dispatcher is running. The third must
// keep delivering, the other two must lose nothing, and both must resume on
// their own once the brokers come back — no restart, no intervention.
func TestTwoOfThreeBrokersFailAndRecover(t *testing.T) {
	streams := []string{"alpha", "beta", "gamma"}
	r := newResilient(t, streams)
	r.Start(t)

	const perStream = 10

	seed := func(round string) {
		for _, s := range streams {
			for i := range perStream {
				r.insert(t, s, "res."+round, fmt.Appendf(nil, `{"n":%d}`, i), nil)
			}
		}
	}

	seed("first")

	waitFor(t, 30*time.Second, func() bool {
		return r.statusCount(t, core.StatusSent) == perStream*len(streams)
	}, "the healthy baseline is delivered")

	// alpha and beta go down; gamma stays up.
	r.Brokers["alpha"].Break()
	r.Brokers["beta"].Break()

	seed("outage")

	// Containment: the surviving stream drains its round while the others cannot.
	waitFor(t, 30*time.Second, func() bool {
		return r.sentIn(t, "gamma") == perStream*2
	}, "gamma keeps delivering through the outage")

	if got := r.sentIn(t, "alpha"); got != perStream {
		t.Errorf("alpha delivered %d messages while its broker was down, want the %d from before",
			got, perStream)
	}

	// The outage has to have actually been felt, or the assertions above pass
	// for the wrong reason: a test that races past the fault proves nothing.
	waitFor(t, 30*time.Second, func() bool {
		return r.feltAFailure(t, "alpha") > 0
	}, "alpha records a failed publish attempt")

	if r.feltAFailure(t, "gamma") != 0 {
		t.Error("gamma recorded a publish failure; its broker never went down")
	}

	// And it was felt as an outage rather than as a rejection: alpha's broker
	// never saw these messages, so none of them may be charged an attempt.
	if attempts := r.maxAttempts(t); attempts != 0 {
		t.Errorf("max attempts = %d, want 0: an unreachable broker must not spend the budget",
			attempts)
	}
	if deferred := r.deferredCount(t); deferred == 0 {
		t.Error("nothing is marked as waiting on a broker, so the outage was recorded as a rejection")
	}

	// Nothing is lost: the undelivered messages are pending or leased, never gone.
	if total := r.statusCount(t, core.StatusPending) + r.statusCount(t, core.StatusProcessing) +
		r.statusCount(t, core.StatusSent) + r.statusCount(t, core.StatusFailed); total != perStream*6 {
		t.Errorf("%d rows accounted for, want %d — messages went missing", total, perStream*6)
	}

	// Recovery, unattended.
	r.Brokers["alpha"].Heal()
	r.Brokers["beta"].Heal()

	waitFor(t, 60*time.Second, func() bool {
		return r.statusCount(t, core.StatusSent) == perStream*6
	}, "the recovered streams drain without intervention")

	if failed := r.statusCount(t, core.StatusFailed); failed != 0 {
		t.Errorf("%d messages ended up failed; an outage inside the retry budget must not consume it",
			failed)
	}
}

// PostgreSQL goes away. The dispatcher must survive it — no crash, no hot loop —
// and resume once the database is back.
func TestDatabaseOutageStallsAndRecovers(t *testing.T) {
	r := newResilient(t, []string{"alpha"})
	r.Start(t)

	const count = 10

	for i := range count {
		r.insert(t, "alpha", "db.outage", fmt.Appendf(nil, `{"n":%d}`, i), nil)
	}

	waitFor(t, 30*time.Second, func() bool {
		return r.sentIn(t, "alpha") == count
	}, "the baseline is delivered")

	r.DB.Break()

	// More work arrives while the dispatcher cannot see the database.
	for i := range count {
		r.insert(t, "alpha", "db.outage", fmt.Appendf(nil, `{"n":%d}`, count+i), nil)
	}

	// The dispatcher cannot deliver, and must not have died trying.
	time.Sleep(2 * time.Second)

	if got := r.sentIn(t, "alpha"); got != count {
		t.Errorf("delivered %d with the database unreachable, want the %d from before", got, count)
	}

	r.DB.Heal()

	waitFor(t, 60*time.Second, func() bool {
		return r.sentIn(t, "alpha") == count*2
	}, "delivery resumes once the database is back")
}

// Everything goes at once, then comes back. The dispatcher is expected to be
// running and caught up afterwards, with no message lost along the way.
func TestEverythingFailsAndComesBack(t *testing.T) {
	r := newResilient(t, []string{"alpha", "beta"})
	r.Start(t)

	const perStream = 8

	for _, s := range []string{"alpha", "beta"} {
		for i := range perStream {
			r.insert(t, s, "total.outage", fmt.Appendf(nil, `{"n":%d}`, i), nil)
		}
	}

	waitFor(t, 30*time.Second, func() bool {
		return r.statusCount(t, core.StatusSent) == perStream*2
	}, "the baseline is delivered")

	r.DB.Break()
	for _, p := range r.Brokers {
		p.Break()
	}

	for _, s := range []string{"alpha", "beta"} {
		for i := range perStream {
			r.insert(t, s, "total.outage", fmt.Appendf(nil, `{"n":%d}`, perStream+i), nil)
		}
	}

	time.Sleep(2 * time.Second)

	if got := r.statusCount(t, core.StatusSent); got != perStream*2 {
		t.Errorf("delivered %d with everything down, want the %d from before", got, perStream*2)
	}

	// Back in the order an operator would see: the database first, then the
	// brokers.
	r.DB.Heal()
	time.Sleep(time.Second)

	for _, p := range r.Brokers {
		p.Heal()
	}

	waitFor(t, 90*time.Second, func() bool {
		return r.statusCount(t, core.StatusSent) == perStream*4
	}, "everything drains once the dependencies return")

	if failed := r.statusCount(t, core.StatusFailed); failed != 0 {
		t.Errorf("%d messages failed permanently after a recoverable outage", failed)
	}
}

// An outage longer than the retry budget is a different story, and the one an
// operator needs to know about: the messages stop being retried and wait for a
// deliberate requeue.
// deferredCount is how many rows are waiting on a broker that could not be
// reached. It reads the marker rather than a metric, so the assertion is about
// what was stored.
func (r *resilient) deferredCount(t *testing.T) int {
	t.Helper()

	var n int
	if err := r.Pool.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT count(*) FROM %q.messages WHERE deferred_since IS NOT NULL`, r.Schema),
	).Scan(&n); err != nil {
		t.Fatalf("count deferred: %v", err)
	}

	return n
}

// maxAttempts is the highest attempt counter in the table.
func (r *resilient) maxAttempts(t *testing.T) int {
	t.Helper()

	var n int
	if err := r.Pool.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT coalesce(max(attempts), 0) FROM %q.messages`, r.Schema),
	).Scan(&n); err != nil {
		t.Fatalf("max attempts: %v", err)
	}

	return n
}

// The behaviour the deferral path exists for.
//
// Before it, every failure advanced the attempt counter, so an outage longer
// than the retry budget moved messages to failed and required an operator to
// requeue them by hand — although the broker never saw one of them. The budget
// counts rejections now, and being unreachable is not a rejection.
//
// MAX_ATTEMPTS is 2 against a 200ms backoff, so the old behaviour would have
// exhausted it within a second. The test then waits an order of magnitude
// longer than that and insists nothing failed.
func TestOutageBeyondTheRetryBudgetStillDelivers(t *testing.T) {
	r := newResilient(t, []string{"alpha"}, "OUTBOX_DISPATCH_MAX_ATTEMPTS=2")
	r.Start(t)

	const count = 5

	r.Brokers["alpha"].Break()

	for i := range count {
		r.insert(t, "alpha", "long.outage", fmt.Appendf(nil, `{"n":%d}`, i), nil)
	}

	waitFor(t, 60*time.Second, func() bool {
		return r.deferredCount(t) == count
	}, "the dispatcher recognises the broker is gone and holds the messages back")

	// Long enough for a dozen retry cycles, which under the old rule was six
	// times the whole budget.
	time.Sleep(2 * time.Second)

	if failed := r.statusCount(t, core.StatusFailed); failed != 0 {
		t.Fatalf("%d messages failed during an outage the broker never even saw", failed)
	}
	if attempts := r.maxAttempts(t); attempts != 0 {
		t.Errorf("attempts = %d, want 0: an unreachable broker must not spend the budget", attempts)
	}
	if pending := r.statusCount(t, core.StatusPending); pending != count {
		t.Errorf("pending = %d, want %d", pending, count)
	}

	// No operator involved: the broker comes back and the backlog drains.
	r.Brokers["alpha"].Heal()

	waitFor(t, 60*time.Second, func() bool {
		return r.statusCount(t, core.StatusSent) == count
	}, "the backlog drains on its own once the broker returns")

	if n := r.deferredCount(t); n != 0 {
		t.Errorf("%d delivered messages still carry a deferral marker; the next outage would "+
			"inherit a window that has already elapsed", n)
	}
}

// The escape hatch. Unbounded deferral is the default because a late message
// beats a failed one, but a stream with a deadline of its own can ask for the
// opposite, and then an outage does end in failed.
func TestOutageBeyondMaxDeferEndsInFailed(t *testing.T) {
	r := newResilient(t, []string{"alpha"},
		"OUTBOX_DISPATCH_MAX_ATTEMPTS=2",
		"OUTBOX_DISPATCH_MAX_DEFER=1s",
	)
	r.Start(t)

	const count = 5

	r.Brokers["alpha"].Break()

	for i := range count {
		r.insert(t, "alpha", "deadline.bound", fmt.Appendf(nil, `{"n":%d}`, i), nil)
	}

	waitFor(t, 60*time.Second, func() bool {
		return r.statusCount(t, core.StatusFailed) == count
	}, "the deferral window runs out and the messages stop waiting")

	// Still nothing lost: they are recorded, with the reason.
	var lastError string
	if err := r.Pool.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT coalesce(last_error, '') FROM %q.messages WHERE status = 3 LIMIT 1`, r.Schema),
	).Scan(&lastError); err != nil {
		t.Fatalf("read last_error: %v", err)
	}
	if lastError == "" {
		t.Error("a failed message carries no reason; the outage must be recorded")
	}
	// A message failed this way was never rejected, so its counter reads zero.
	// That is why the reason is reported as unreachable rather than exhausted:
	// an attempts figure of zero next to "attempts exhausted" sends whoever
	// reads it looking for a rejection that never happened.
	if attempts := r.maxAttempts(t); attempts != 0 {
		t.Errorf("attempts = %d, want 0", attempts)
	}
	if n := r.deferredCount(t); n != 0 {
		t.Errorf("%d failed messages still carry a deferral marker", n)
	}

	// The broker returns, and the operator puts them back.
	r.Brokers["alpha"].Heal()

	ids := make([]string, 0, count)
	rows, err := r.Pool.Query(t.Context(), fmt.Sprintf(
		`SELECT id FROM %q.messages WHERE status = 3`, r.Schema))
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()

	if _, err := r.Store.Requeue(t.Context(), ids); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	waitFor(t, 60*time.Second, func() bool {
		return r.statusCount(t, core.StatusSent) == count
	}, "requeued messages are delivered once the broker is back")
}

func TestUnreachableBrokerFailsTheStart(t *testing.T) {
	f := newFixture(t)

	dead := newProxy(t, amqpHost(t, amqpDSN()))
	dead.Break()

	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_DSN="+f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA="+f.Schema,
		"OUTBOX_STREAMS=alpha",
		"OUTBOX_STREAM_ALPHA_DRIVER=rmq_alpha",
		"OUTBOX_DRIVER_RMQ_ALPHA_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_ALPHA_DSN=amqp://outbox:outbox@"+dead.Addr()+"/",
		"OUTBOX_DRIVER_RMQ_ALPHA_PUBLISH_TIMEOUT=2s",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	d, ok := cfg.Brokers.Drivers["rmq_alpha"].(*config.RabbitMQDriver)
	if !ok {
		t.Fatalf("driver is %T", cfg.Brokers.Drivers["rmq_alpha"])
	}

	if _, err := rabbitmq.New(t.Context(), d, logging.Nop()); err == nil {
		t.Fatal("connecting to an unreachable broker succeeded; startup must fail instead")
	}
}
