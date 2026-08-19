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

// attemptedWithError counts messages in a stream that have a recorded failure,
// which is how a test tells a real outage from a lucky race.
func (r *resilient) attemptedWithError(t *testing.T, stream string) int {
	t.Helper()

	var n int
	err := r.Pool.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT count(*) FROM %q.messages
		  WHERE stream = $1 AND attempts > 0 AND last_error IS NOT NULL`, r.Schema), stream).Scan(&n)
	if err != nil {
		t.Fatalf("count attempted in %s: %v", stream, err)
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
		return r.attemptedWithError(t, "alpha") > 0
	}, "alpha records a failed publish attempt")

	if r.attemptedWithError(t, "gamma") != 0 {
		t.Error("gamma recorded a publish failure; its broker never went down")
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
func TestOutageBeyondTheRetryBudgetEndsInFailed(t *testing.T) {
	r := newResilient(t, []string{"alpha"}, "OUTBOX_DISPATCH_MAX_ATTEMPTS=2")
	r.Start(t)

	const count = 5

	r.Brokers["alpha"].Break()

	for i := range count {
		r.insert(t, "alpha", "long.outage", fmt.Appendf(nil, `{"n":%d}`, i), nil)
	}

	waitFor(t, 60*time.Second, func() bool {
		return r.statusCount(t, core.StatusFailed) == count
	}, "the attempts are spent and the messages stop being retried")

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

// The startup contract, stated as a test because it is the asymmetry that
// surprises people: a broker that is unreachable while running is contained,
// but one that is unreachable at startup stops the process.
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
