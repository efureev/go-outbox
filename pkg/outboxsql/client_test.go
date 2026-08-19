package outboxsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// recorder stands in for a database, so the arguments a statement would carry
// can be inspected without one.
type recorder struct {
	query string
	args  []any
	err   error
}

func (r *recorder) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	r.query, r.args = query, args

	return nil, r.err
}

func TestNewRejectsIdentifiersItCannotQuote(t *testing.T) {
	for name, in := range map[string][2]string{
		"injection in the schema": {`public"; DROP TABLE x --`, "messages"},
		"injection in the table":  {"outbox", `messages"; DROP TABLE x --`},
		"upper case":              {"Outbox", "messages"},
		"empty":                   {"", "messages"},
		"leading digit":           {"1outbox", "messages"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := New(in[0], in[1]); err == nil {
				t.Errorf("New(%q, %q) was accepted", in[0], in[1])
			}
		})
	}
}

func TestEnqueueRejectsAnIncompleteMessage(t *testing.T) {
	c := Default()

	for name, msg := range map[string]Message{
		"no stream": {Topic: "t", Payload: []byte("{}")},
		"no topic":  {Stream: "local", Payload: []byte("{}")},
	} {
		t.Run(name, func(t *testing.T) {
			if err := c.Enqueue(t.Context(), &recorder{}, msg); err == nil {
				t.Error("an incomplete message was accepted")
			}
		})
	}
}

// The JSON columns go as strings. Both forms reach a jsonb column intact
// through lib/pq and pgx/v5/stdlib, but a []byte is the shape a driver is
// entitled to encode as bytea, and this package cannot know which driver it is
// talking to.
func TestJSONColumnsAreSentAsStrings(t *testing.T) {
	rec := &recorder{}

	err := Default().Enqueue(t.Context(), rec, Message{
		Stream:  "local",
		Topic:   "account.debited",
		Payload: []byte{0x00, 0xff},
		Headers: map[string]string{"traceparent": "00-ab-cd-01"},
		Target:  Target{Key: "customer-1"},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	headers, ok := rec.args[4].(string)
	if !ok {
		t.Fatalf("headers were sent as %T, not a string", rec.args[4])
	}
	if !strings.Contains(headers, "traceparent") {
		t.Errorf("headers = %q", headers)
	}

	target, ok := rec.args[5].(string)
	if !ok {
		t.Fatalf("target was sent as %T, not a string", rec.args[5])
	}
	if !strings.Contains(target, "customer-1") {
		t.Errorf("target = %q", target)
	}

	// The payload is the opposite case: bytea, and arbitrary bytes.
	if payload, ok := rec.args[3].([]byte); !ok || len(payload) != 2 {
		t.Errorf("payload was sent as %T", rec.args[3])
	}
}

func TestHeadersDefaultToAnEmptyObject(t *testing.T) {
	rec := &recorder{}

	if err := Default().Enqueue(t.Context(), rec, Message{
		Stream: "local", Topic: "t", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if got := rec.args[4]; got != "{}" {
		t.Errorf("headers = %v, want an empty JSON object", got)
	}
}

// A zero AvailableAt must reach the statement as nil, so its coalesce falls
// through to the database's now(). Sending the zero time would schedule the
// first attempt for the year one, and the message would never be claimed.
func TestZeroAvailableAtIsSentAsNil(t *testing.T) {
	rec := &recorder{}

	if err := Default().Enqueue(t.Context(), rec, Message{
		Stream: "local", Topic: "t", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if rec.args[6] != nil {
		t.Errorf("available_at = %v, want nil", rec.args[6])
	}

	at := time.Now().Add(time.Hour)
	if err := Default().Enqueue(t.Context(), rec, Message{
		Stream: "local", Topic: "t", Payload: []byte("{}"), AvailableAt: at,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if rec.args[6] != at {
		t.Errorf("available_at = %v, want %v", rec.args[6], at)
	}
}

func TestGeneratedAndSuppliedIDs(t *testing.T) {
	rec := &recorder{}
	c := Default()

	if err := c.Enqueue(t.Context(), rec, Message{
		Stream: "local", Topic: "t", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	generated, _ := rec.args[0].(string)
	if len(generated) != 36 {
		t.Errorf("generated id = %q, want a UUID", generated)
	}

	if err := c.Enqueue(t.Context(), rec, Message{
		ID:     "0198f0a0-0000-7000-8000-000000000001",
		Stream: "local", Topic: "t", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if rec.args[0] != "0198f0a0-0000-7000-8000-000000000001" {
		t.Errorf("id = %v, want the supplied one", rec.args[0])
	}
}

// The statement must name only the producer's columns. Naming a
// dispatcher-owned one is how a producer ends up writing a lease or a status it
// has no business setting.
func TestTheStatementTouchesOnlyProducerColumns(t *testing.T) {
	rec := &recorder{}

	if err := Default().Enqueue(t.Context(), rec, Message{
		Stream: "local", Topic: "t", Payload: []byte("{}"),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	for _, forbidden := range []string{"status", "attempts", "lease_token", "owner", "dispatched_at"} {
		if strings.Contains(rec.query, forbidden) {
			t.Errorf("the insert names the dispatcher-owned column %q:\n%s", forbidden, rec.query)
		}
	}
}

func TestEnqueueBatchStopsAtTheFirstFailure(t *testing.T) {
	rec := &recorder{err: sql.ErrConnDone}

	err := Default().EnqueueBatch(t.Context(), rec, []Message{
		{Stream: "local", Topic: "a", Payload: []byte("{}")},
		{Stream: "local", Topic: "b", Payload: []byte("{}")},
	})
	if err == nil {
		t.Fatal("a failed write was reported as success")
	}
	if !strings.Contains(err.Error(), "1 of 2") {
		t.Errorf("the error does not say which message failed: %v", err)
	}
}

func TestEnqueueBatchOfNothingDoesNothing(t *testing.T) {
	rec := &recorder{}

	if err := Default().EnqueueBatch(t.Context(), rec, nil); err != nil {
		t.Fatalf("EnqueueBatch(nil): %v", err)
	}
	if rec.query != "" {
		t.Error("an empty batch still sent a statement")
	}
}

// Target has to serialise to the shape the dispatcher reads back out of the
// target column, whatever the client that wrote it.
func TestTargetSerialisesToTheDocumentedShape(t *testing.T) {
	raw, err := json.Marshal(Target{Key: "k", Version: 2, Exchange: "e", RoutingKey: "r"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	want := `{"key":"k","version":2,"exchange":"e","routing_key":"r"}`
	if string(raw) != want {
		t.Errorf("Target = %s, want %s", raw, want)
	}

	// Empty fields are omitted, so an unrouted message stores {} rather than a
	// row of nulls the dispatcher then has to ignore.
	if raw, _ = json.Marshal(Target{}); string(raw) != "{}" {
		t.Errorf("empty Target = %s, want {}", raw)
	}
}
