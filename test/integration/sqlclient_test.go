//go:build integration

package integration

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"

	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/pkg/outboxsql"
)

// Both drivers a team on database/sql is realistically holding. The client
// imports neither — that is the point of it — so the only way to know it works
// through them is to write through them.
var sqlDrivers = []string{"pgx", "postgres"}

func openSQL(t *testing.T, driver string) *sql.DB {
	t.Helper()

	db, err := sql.Open(driver, dsn())
	if err != nil {
		t.Fatalf("open %s: %v", driver, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("ping through %s: %v", driver, err)
	}

	return db
}

func newSQLClient(t *testing.T, f *fixture) *outboxsql.Client {
	t.Helper()

	c, err := outboxsql.New(f.Schema, "messages")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	return c
}

// The property the whole pattern rests on, through database/sql this time: the
// message is written in the same transaction as the business change, so a
// rollback takes both.
func TestSQLEnqueueRollsBackWithTheBusinessTransaction(t *testing.T) {
	for _, driver := range sqlDrivers {
		t.Run(driver, func(t *testing.T) {
			f := newFixture(t)
			client := newSQLClient(t, f)
			db := openSQL(t, driver)

			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}

			if err := client.Enqueue(t.Context(), tx, outboxsql.Message{
				Stream:  "local",
				Topic:   "account.debited",
				Payload: []byte(`{"amount":100}`),
			}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			if err := tx.Rollback(); err != nil {
				t.Fatalf("rollback: %v", err)
			}

			if n := f.countByStatus(t, core.StatusPending); n != 0 {
				t.Errorf("%d messages survived a rolled-back transaction", n)
			}
		})
	}
}

// And the other half: what commits is claimable by the dispatcher, which is the
// only thing that makes a foreign client useful rather than merely present.
func TestSQLEnqueueIsClaimedByTheDispatcher(t *testing.T) {
	for _, driver := range sqlDrivers {
		t.Run(driver, func(t *testing.T) {
			f := newFixture(t)
			client := newSQLClient(t, f)
			db := openSQL(t, driver)

			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}

			if err := client.EnqueueBatch(t.Context(), tx, []outboxsql.Message{
				{Stream: "local", Topic: "a", Payload: []byte(`{"n":1}`)},
				{Stream: "local", Topic: "b", Payload: []byte(`{"n":2}`)},
			}); err != nil {
				t.Fatalf("enqueue batch: %v", err)
			}

			if err := tx.Commit(); err != nil {
				t.Fatalf("commit: %v", err)
			}

			claimed, err := f.Store.Claim(t.Context(), "local", 10, lease("a", time.Minute))
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			if len(claimed) != 2 {
				t.Fatalf("claimed %d messages, want 2", len(claimed))
			}
		})
	}
}

// The encoding this client had to get right and could not verify without a
// database: jsonb columns and a bytea one, through a driver it does not import.
//
// A payload is protobuf or msgpack as often as it is JSON, so the bytes are
// deliberately not text.
func TestSQLEnqueuePreservesHeadersAndBinaryPayloads(t *testing.T) {
	binary := []byte{0x00, 0x01, 0xff, 0xfe, 0x7b, 0x80, 0x0a, 0x00}

	for _, driver := range sqlDrivers {
		t.Run(driver, func(t *testing.T) {
			f := newFixture(t)
			client := newSQLClient(t, f)
			db := openSQL(t, driver)

			if err := client.Enqueue(t.Context(), db, outboxsql.Message{
				Stream:  "local",
				Topic:   "order.created",
				Payload: binary,
				Headers: map[string]string{"traceparent": "00-abc-def-01"},
				Target:  outboxsql.Target{Key: "customer-1", Version: 2},
			}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			claimed, err := f.Store.Claim(t.Context(), "local", 10, lease("a", time.Minute))
			if err != nil {
				t.Fatalf("claim: %v", err)
			}
			if len(claimed) != 1 {
				t.Fatalf("claimed %d messages, want 1", len(claimed))
			}

			got := claimed[0]
			if string(got.Payload) != string(binary) {
				t.Errorf("payload = %v, want %v", got.Payload, binary)
			}
			if got.Headers["traceparent"] != "00-abc-def-01" {
				t.Errorf("headers = %v, want the traceparent preserved", got.Headers)
			}
			if got.Target.Key != "customer-1" || got.Target.Version != 2 {
				t.Errorf("target = %+v, want the routing envelope preserved", got.Target)
			}
		})
	}
}

// End to end: written through database/sql, delivered to a real broker.
func TestSQLEnqueueReachesTheBroker(t *testing.T) {
	h := newHarness(t)
	client := newSQLClient(t, h.fixture)
	db := openSQL(t, "postgres")

	const topic = "sqlclient.orders"

	if err := client.Enqueue(t.Context(), db, outboxsql.Message{
		Stream:  "local",
		Topic:   topic,
		Payload: []byte(`{"order":"A-1"}`),
		Headers: map[string]string{"traceparent": "00-abc-def-01"},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	res, err := h.Pipeline.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if res.Delivered != 1 {
		t.Fatalf("delivered %d, want 1", res.Delivered)
	}

	got := consumeOne(t, h.queueFor(topic))
	if string(got.Body) != `{"order":"A-1"}` {
		t.Errorf("body = %q", got.Body)
	}
	if tp, _ := got.Headers["traceparent"].(string); tp != "00-abc-def-01" {
		t.Errorf("traceparent = %q, want it carried through", tp)
	}
}

// AvailableAt has to reach the database as a real time, and a zero one has to
// fall through to now(). Sending the zero time would schedule the first attempt
// for the year one, and the message would never be claimed.
func TestSQLAvailableAtDelaysTheFirstAttempt(t *testing.T) {
	f := newFixture(t)
	client := newSQLClient(t, f)
	db := openSQL(t, "postgres")

	if err := client.Enqueue(t.Context(), db, outboxsql.Message{
		Stream: "local", Topic: "later", Payload: []byte("{}"),
		AvailableAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("enqueue delayed: %v", err)
	}
	if err := client.Enqueue(t.Context(), db, outboxsql.Message{
		Stream: "local", Topic: "now", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("enqueue immediate: %v", err)
	}

	claimed, err := f.Store.Claim(t.Context(), "local", 10, lease("a", time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d messages, want only the one that is due", len(claimed))
	}
	if claimed[0].Topic != "now" {
		t.Errorf("claimed %q, want the immediate one", claimed[0].Topic)
	}
}
