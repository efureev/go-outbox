//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/broker/postgres"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
)

// inboxFixture gives an outbox and an inbox in one database — the modular
// monolith of use case 1, and the shape where no broker exists at all.
type inboxFixture struct {
	*fixture

	Driver *config.PostgresDriver
	Pub    *postgres.Publisher
	Table  string
}

func newInboxFixture(t *testing.T, ddl ...string) *inboxFixture {
	t.Helper()

	f := newFixture(t)

	create := fmt.Sprintf(`
		CREATE TABLE %[1]s.inbox (
		    id          UUID PRIMARY KEY,
		    stream      TEXT        NOT NULL,
		    topic       TEXT        NOT NULL,
		    payload     BYTEA       NOT NULL,
		    headers     JSONB       NOT NULL DEFAULT '{}'::jsonb,
		    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		    processed_at TIMESTAMPTZ
		)`, quoted(f.Schema))
	if len(ddl) > 0 {
		create = fmt.Sprintf(ddl[0], quoted(f.Schema))
	}

	if _, err := f.Pool.Exec(t.Context(), create); err != nil {
		t.Fatalf("create the inbox: %v", err)
	}

	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_DSN="+f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA="+f.Schema,
		"OUTBOX_STREAMS=billing",
		"OUTBOX_STREAM_BILLING_DRIVER=inb",
		"OUTBOX_DRIVER_INB_TYPE=postgres",
		"OUTBOX_DRIVER_INB_SCHEMA="+f.Schema,
		"OUTBOX_DRIVER_INB_TABLE=inbox",
		"OUTBOX_DRIVER_INB_PREFIX=app",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	driver, ok := cfg.Brokers.Drivers["inb"].(*config.PostgresDriver)
	if !ok {
		t.Fatalf("driver is %T", cfg.Brokers.Drivers["inb"])
	}

	pub, err := postgres.New(t.Context(), driver, logging.Nop())
	if err != nil {
		t.Fatalf("open the inbox publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close(context.Background()) })

	return &inboxFixture{
		fixture: f,
		Driver:  driver,
		Pub:     pub,
		Table:   quoted(f.Schema) + ".inbox",
	}
}

// countStatements arms the inbox with a statement-level trigger. It fires once
// per statement whatever the row count, which is the only way to tell a batch
// insert from a loop of them without changing the driver to report on itself.
//
// It counts statements that committed. A statement the server refused is rolled
// back and takes its trigger with it, so a failed batch contributes nothing —
// which is what makes the count on the failure path meaningful rather than
// merely large.
func (f *inboxFixture) countStatements(t *testing.T) {
	t.Helper()

	ddl := fmt.Sprintf(`
		CREATE TABLE %[1]s.statements (n INTEGER NOT NULL);
		INSERT INTO %[1]s.statements VALUES (0);

		CREATE FUNCTION %[1]s.count_statement() RETURNS TRIGGER LANGUAGE plpgsql AS $fn$
		BEGIN
		    UPDATE %[1]s.statements SET n = n + 1;
		    RETURN NULL;
		END;
		$fn$;

		CREATE TRIGGER inbox_statement_trg
		    AFTER INSERT ON %[1]s.inbox
		    FOR EACH STATEMENT
		EXECUTE FUNCTION %[1]s.count_statement();`, quoted(f.Schema))

	if _, err := f.Pool.Exec(t.Context(), ddl); err != nil {
		t.Fatalf("arm the statement counter: %v", err)
	}
}

func (f *inboxFixture) statements(t *testing.T) int {
	t.Helper()

	var n int
	if err := f.Pool.QueryRow(t.Context(),
		fmt.Sprintf("SELECT n FROM %s.statements", quoted(f.Schema))).Scan(&n); err != nil {
		t.Fatalf("read the statement counter: %v", err)
	}

	return n
}

func (f *inboxFixture) rows(t *testing.T) int {
	t.Helper()

	var n int
	if err := f.Pool.QueryRow(t.Context(), "SELECT count(*) FROM "+f.Table).Scan(&n); err != nil {
		t.Fatalf("count inbox rows: %v", err)
	}

	return n
}

func inboxMessage(id, topic string, payload []byte) core.Message {
	return core.Message{
		ID: id, Stream: "billing", Topic: topic, Payload: payload,
		CreatedAt: time.Now(),
	}
}

// The reason this driver exists. At-least-once still applies — the insert and
// the write-back are two commits — but the repeat is absorbed by the inbox's
// primary key instead of by the consumer's code.
func TestInboxAbsorbsARepeatedDelivery(t *testing.T) {
	f := newInboxFixture(t)

	msgs := []core.Message{
		inboxMessage("0198f0a0-0000-7000-8000-000000000001", "order.created", []byte(`{"n":1}`)),
		inboxMessage("0198f0a0-0000-7000-8000-000000000002", "order.paid", []byte(`{"n":2}`)),
	}

	for round := 1; round <= 3; round++ {
		for i, err := range f.Pub.Publish(t.Context(), msgs) {
			if err != nil {
				t.Fatalf("round %d, message %d: %v", round, i, err)
			}
		}
	}

	// Three deliveries, two rows: a repeat is reported as success, because the
	// message is in the inbox either way.
	if got := f.rows(t); got != 2 {
		t.Errorf("inbox holds %d rows after three deliveries of two messages, want 2", got)
	}
}

// The inbox stores the effective name — prefix and version applied — which is
// the same string a consumer would have subscribed to at a broker. The naming
// rules do not change just because the destination is a table.
func TestInboxStoresTheEffectiveTopic(t *testing.T) {
	f := newInboxFixture(t)

	msg := inboxMessage("0198f0a0-0000-7000-8000-000000000003", "order.created", []byte("{}"))
	msg.Target = core.Target{Version: 2}

	if err := f.Pub.Publish(t.Context(), []core.Message{msg})[0]; err != nil {
		t.Fatalf("publish: %v", err)
	}

	var topic string
	if err := f.Pool.QueryRow(t.Context(),
		"SELECT topic FROM "+f.Table).Scan(&topic); err != nil {
		t.Fatalf("read topic: %v", err)
	}

	if want := "app.order.created.v2"; topic != want {
		t.Errorf("topic = %q, want %q", topic, want)
	}
}

// A payload is protobuf or msgpack as often as it is JSON, and headers are the
// only thing carrying a traceparent onward.
func TestInboxPreservesHeadersAndBinaryPayloads(t *testing.T) {
	f := newInboxFixture(t)

	binary := []byte{0x00, 0x01, 0xff, 0xfe, 0x7b, 0x80, 0x0a, 0x00}
	msg := inboxMessage("0198f0a0-0000-7000-8000-000000000004", "order.created", binary)
	msg.Headers = map[string]string{"traceparent": "00-abc-def-01"}

	if err := f.Pub.Publish(t.Context(), []core.Message{msg})[0]; err != nil {
		t.Fatalf("publish: %v", err)
	}

	var (
		payload []byte
		trace   string
	)
	if err := f.Pool.QueryRow(t.Context(),
		"SELECT payload, headers->>'traceparent' FROM "+f.Table).Scan(&payload, &trace); err != nil {
		t.Fatalf("read row: %v", err)
	}

	if string(payload) != string(binary) {
		t.Errorf("payload = %v, want %v", payload, binary)
	}
	if trace != "00-abc-def-01" {
		t.Errorf("traceparent = %q", trace)
	}
}

// A destination that is not there is a configuration mistake, and configuration
// mistakes belong at the boot rather than on a real message at three in the
// morning.
func TestInboxThatDoesNotExistFailsTheStart(t *testing.T) {
	f := newFixture(t)

	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_DSN="+f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA="+f.Schema,
		"OUTBOX_STREAMS=billing",
		"OUTBOX_STREAM_BILLING_DRIVER=inb",
		"OUTBOX_DRIVER_INB_TYPE=postgres",
		"OUTBOX_DRIVER_INB_SCHEMA="+f.Schema,
		"OUTBOX_DRIVER_INB_TABLE=nowhere",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	_, err = postgres.New(t.Context(), cfg.Brokers.Drivers["inb"].(*config.PostgresDriver), logging.Nop())
	if err == nil {
		t.Fatal("a missing inbox was accepted at startup")
	}
}

// One malformed message must not condemn the rest of the batch to a pointless
// retry. The fast path is a single statement; when the database refuses, the
// batch is replayed one at a time to find out who is guilty.
func TestInboxIsolatesTheOffendingMessage(t *testing.T) {
	f := newInboxFixture(t, `
		CREATE TABLE %[1]s.inbox (
		    id      UUID PRIMARY KEY,
		    stream  TEXT  NOT NULL,
		    topic   TEXT  NOT NULL CHECK (topic <> 'app.forbidden'),
		    payload BYTEA NOT NULL,
		    headers JSONB NOT NULL DEFAULT '{}'::jsonb
		)`)

	msgs := []core.Message{
		inboxMessage("0198f0a0-0000-7000-8000-000000000005", "fine.one", []byte("{}")),
		inboxMessage("0198f0a0-0000-7000-8000-000000000006", "forbidden", []byte("{}")),
		inboxMessage("0198f0a0-0000-7000-8000-000000000007", "fine.two", []byte("{}")),
	}
	// The CHECK is on the stored topic, which is the effective name — prefix
	// applied — not the bare one the producer wrote.

	errs := f.Pub.Publish(t.Context(), msgs)

	if errs[0] != nil || errs[2] != nil {
		t.Errorf("a bad message condemned its neighbours: %v", errs)
	}
	if errs[1] == nil {
		t.Fatal("the offending message was reported as delivered")
	}
	if !core.IsPermanent(errs[1]) {
		t.Errorf("a CHECK violation is not permanent: %v", errs[1])
	}
	if core.IsUnavailable(errs[1]) {
		t.Error("a refusal was classified as an outage, so it would retry forever")
	}

	if got := f.rows(t); got != 2 {
		t.Errorf("inbox holds %d rows, want the two good ones", got)
	}
}

// End to end: the dispatcher claims from the outbox and delivers into the
// inbox, with no broker anywhere in the picture.
func TestTheDispatcherDeliversIntoAnInbox(t *testing.T) {
	f := newInboxFixture(t)

	router, err := broker.NewRouter(
		config.BrokerConfig{
			Streams: map[string]config.StreamConfig{"billing": {Driver: "inb"}},
			Drivers: map[string]config.DriverConfig{"inb": f.Driver},
		},
		map[string]broker.Publisher{"inb": f.Pub},
	)
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	cfg := mustConfig(t, f.fixture)
	cfg.Brokers = config.BrokerConfig{
		Streams: map[string]config.StreamConfig{"billing": {Driver: "inb"}},
		Drivers: map[string]config.DriverConfig{"inb": f.Driver},
	}

	pipeline := dispatch.New("billing", f.Store, router,
		events.NewEmitter(nil, logging.Nop()), cfg, logging.Nop())

	for i := range 3 {
		f.insert(t, "billing", fmt.Sprintf("order.%d", i), fmt.Appendf(nil, `{"n":%d}`, i), nil)
	}

	res, err := pipeline.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if res.Delivered != 3 {
		t.Fatalf("delivered %d, want 3", res.Delivered)
	}

	if got := f.rows(t); got != 3 {
		t.Errorf("inbox holds %d rows, want 3", got)
	}
	if pending := f.countByStatus(t, core.StatusPending); pending != 0 {
		t.Errorf("%d rows are still pending in the outbox", pending)
	}
}

// A batch is one statement. Not a detail: the dispatcher claims two hundred
// messages at a time by default, and a loop of two hundred inserts would make
// the destination database the bottleneck the broker never was.
//
// Counted by a statement-level trigger, which fires once per statement whatever
// the row count — the driver is not asked to report on itself.
func TestInboxWritesABatchInOneStatement(t *testing.T) {
	f := newInboxFixture(t)
	f.countStatements(t)

	const count = 50

	msgs := make([]core.Message, count)
	for i := range msgs {
		msgs[i] = inboxMessage(
			fmt.Sprintf("0198f0a0-0000-7000-8000-%012d", i),
			fmt.Sprintf("order.%d", i),
			fmt.Appendf(nil, `{"n":%d}`, i))
	}

	for i, err := range f.Pub.Publish(t.Context(), msgs) {
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
	}

	if got := f.rows(t); got != count {
		t.Fatalf("inbox holds %d rows, want %d", got, count)
	}
	if got := f.statements(t); got != 1 {
		t.Errorf("%d messages took %d statements, want 1", count, got)
	}
}

// The other half of the contract: when the database refuses, the batch is
// replayed one at a time so the error lands on the message that caused it.
// Errors in the first and last positions are included deliberately — an
// off-by-one in the isolating pass would show up exactly there.
func TestInboxReportsFailuresPositionally(t *testing.T) {
	f := newInboxFixture(t, `
		CREATE TABLE %[1]s.inbox (
		    id      UUID PRIMARY KEY,
		    stream  TEXT  NOT NULL,
		    topic   TEXT  NOT NULL CHECK (topic NOT LIKE '%%.bad.%%'),
		    payload BYTEA NOT NULL,
		    headers JSONB NOT NULL DEFAULT '{}'::jsonb
		)`)
	f.countStatements(t)

	topics := []string{"bad.first", "fine.one", "bad.middle", "fine.two", "bad.last"}

	msgs := make([]core.Message, len(topics))
	for i, topic := range topics {
		msgs[i] = inboxMessage(
			fmt.Sprintf("0198f0a0-0000-7000-8000-%012d", 100+i), topic, []byte("{}"))
	}

	errs := f.Pub.Publish(t.Context(), msgs)

	for i, topic := range topics {
		bad := strings.HasPrefix(topic, "bad.")
		switch {
		case bad && errs[i] == nil:
			t.Errorf("position %d (%s) was reported as delivered", i, topic)
		case bad && !core.IsPermanent(errs[i]):
			t.Errorf("position %d (%s): %v is not permanent", i, topic, errs[i])
		case !bad && errs[i] != nil:
			t.Errorf("position %d (%s) was condemned by its neighbours: %v", i, topic, errs[i])
		}
	}

	if got := f.rows(t); got != 2 {
		t.Errorf("inbox holds %d rows, want the two good ones", got)
	}

	// The trigger counts what committed. The batch attempt failed and left
	// nothing, so these two are the good messages inserted one at a time — and
	// that is the isolating pass, observed rather than assumed. Without it the
	// count would be zero and the whole batch lost to one bad topic.
	if got := f.statements(t); got != 2 {
		t.Errorf("%d statements committed, want the two good messages inserted individually", got)
	}
}

// What a broker refuses for size, a table takes.
//
// Every broker driver has a permanent failure class for a payload above the
// limit — RabbitMQ's frame size, Kafka's message.max.bytes. This one does not,
// and the point of the test is that the absence was measured rather than
// assumed: a payload two orders of magnitude past what a broker would take goes
// in and comes back byte for byte.
func TestInboxTakesPayloadsNoBrokerWould(t *testing.T) {
	f := newInboxFixture(t)

	for _, size := range []int{1 << 20, 8 << 20, 32 << 20} {
		t.Run(fmt.Sprintf("%dMiB", size>>20), func(t *testing.T) {
			payload := make([]byte, size)
			for i := range payload {
				// Not zeroes: a compressible payload would prove nothing about
				// what TOAST actually carries.
				payload[i] = byte(i * 7)
			}

			id := fmt.Sprintf("0198f0a0-0000-7000-8000-%012d", 900+(size>>20))
			msg := inboxMessage(id, "large.payload", payload)

			if err := f.Pub.Publish(t.Context(), []core.Message{msg})[0]; err != nil {
				t.Fatalf("%d MiB was refused: %v", size>>20, err)
			}

			var stored []byte
			if err := f.Pool.QueryRow(t.Context(),
				"SELECT payload FROM "+f.Table+" WHERE id = $1", id).Scan(&stored); err != nil {
				t.Fatalf("read back: %v", err)
			}

			if len(stored) != size {
				t.Fatalf("stored %d bytes, wrote %d", len(stored), size)
			}
			if !bytes.Equal(stored, payload) {
				t.Error("the payload came back changed")
			}
		})
	}
}

// A batch of large payloads is where a single statement could plausibly hit a
// protocol limit that one message never would.
func TestInboxTakesABatchOfLargePayloads(t *testing.T) {
	f := newInboxFixture(t)

	const (
		count = 16
		each  = 4 << 20 // 64 MiB in one statement
	)

	msgs := make([]core.Message, count)
	for i := range msgs {
		payload := make([]byte, each)
		for j := range payload {
			payload[j] = byte(i + j)
		}
		msgs[i] = inboxMessage(
			fmt.Sprintf("0198f0a0-0000-7000-8000-%012d", 800+i), "large.batch", payload)
	}

	for i, err := range f.Pub.Publish(t.Context(), msgs) {
		if err != nil {
			t.Fatalf("message %d of a %d MiB batch: %v", i, (count*each)>>20, err)
		}
	}

	if got := f.rows(t); got != count {
		t.Errorf("inbox holds %d rows, want %d", got, count)
	}
}
