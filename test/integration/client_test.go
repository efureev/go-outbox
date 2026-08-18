//go:build integration

package integration

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/pkg/outboxclient"
)

func newClient(t *testing.T, f *fixture) *outboxclient.Client {
	t.Helper()

	c, err := outboxclient.New(f.Schema, "messages")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	return c
}

// The property the whole pattern rests on: the message is written in the same
// transaction as the business change, so a rollback takes both.
func TestEnqueueRollsBackWithTheBusinessTransaction(t *testing.T) {
	f := newFixture(t)
	client := newClient(t, f)

	tx, err := f.Pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	if err := client.Enqueue(t.Context(), tx, outboxclient.Message{
		Stream:  "local",
		Topic:   "account.debited",
		Payload: []byte(`{"amount":100}`),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	if got := f.countByStatus(t, core.StatusPending); got != 0 {
		t.Errorf("%d messages survived a rolled-back transaction", got)
	}
}

func TestEnqueueCommitsWithTheBusinessTransaction(t *testing.T) {
	f := newFixture(t)
	client := newClient(t, f)

	tx, err := f.Pool.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	if err := client.Enqueue(t.Context(), tx, outboxclient.Message{
		Stream:  "local",
		Topic:   "account.debited",
		Payload: []byte(`{"amount":100}`),
		Headers: map[string]string{"traceparent": "00-trace-span-01"},
		Target:  outboxclient.Target{Key: "customer-7", Version: 2},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatalf("commit: %v", err)
	}

	claimed, err := f.Store.Claim(t.Context(), "local", 10, lease("a", time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d messages, want 1", len(claimed))
	}

	m := claimed[0]
	if m.Topic != "account.debited" {
		t.Errorf("topic = %q", m.Topic)
	}
	if string(m.Payload) != `{"amount":100}` {
		t.Errorf("payload = %q, want the bytes unchanged", m.Payload)
	}
	if m.Headers["traceparent"] != "00-trace-span-01" {
		t.Errorf("headers = %v, want the traceparent preserved", m.Headers)
	}
	if m.Target.Key != "customer-7" || m.Target.Version != 2 {
		t.Errorf("target = %+v, want key customer-7 and version 2", m.Target)
	}
}

// A generated id must sort by creation time, so the primary key index stays
// append-ordered rather than scattering writes across the tree.
func TestGeneratedIDsSortByCreationTime(t *testing.T) {
	f := newFixture(t)
	client := newClient(t, f)

	for range 20 {
		if err := client.Enqueue(t.Context(), f.Pool, outboxclient.Message{
			Stream: "local", Topic: "t", Payload: []byte(`{}`),
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	rows, err := f.Pool.Query(t.Context(),
		`SELECT id FROM `+quoted(f.Schema)+`.messages ORDER BY created_at, id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var byTime []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		byTime = append(byTime, id)
	}

	for i := 1; i < len(byTime); i++ {
		if byTime[i-1] >= byTime[i] {
			t.Fatalf("id %s does not sort after %s; the ids are not time-ordered",
				byTime[i], byTime[i-1])
		}
	}
}

func TestEnqueueBatchWritesEverythingOrNothing(t *testing.T) {
	f := newFixture(t)
	client := newClient(t, f)

	msgs := make([]outboxclient.Message, 10)
	for i := range msgs {
		msgs[i] = outboxclient.Message{Stream: "local", Topic: "batch", Payload: []byte(`{}`)}
	}

	err := pgx.BeginFunc(t.Context(), f.Pool, func(tx pgx.Tx) error {
		return client.EnqueueBatch(t.Context(), tx, msgs)
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}

	if got := f.countByStatus(t, core.StatusPending); got != 10 {
		t.Errorf("%d messages written, want 10", got)
	}
}

func TestAvailableAtDelaysTheFirstAttempt(t *testing.T) {
	f := newFixture(t)
	client := newClient(t, f)

	if err := client.Enqueue(t.Context(), f.Pool, outboxclient.Message{
		Stream:      "local",
		Topic:       "later",
		Payload:     []byte(`{}`),
		AvailableAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claimed, err := f.Store.Claim(t.Context(), "local", 10, lease("a", time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 0 {
		t.Errorf("claimed %d messages that are scheduled for later", len(claimed))
	}
}

func TestEnqueueRejectsAnIncompleteMessage(t *testing.T) {
	f := newFixture(t)
	client := newClient(t, f)

	for _, msg := range []outboxclient.Message{
		{Topic: "t", Payload: []byte(`{}`)},
		{Stream: "local", Payload: []byte(`{}`)},
	} {
		if err := client.Enqueue(t.Context(), f.Pool, msg); err == nil {
			t.Errorf("enqueue accepted %+v; stream and topic are both required", msg)
		}
	}
}
