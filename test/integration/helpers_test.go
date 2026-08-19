//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	envi "github.com/efureev/envi/v2"

	"github.com/google/uuid"

	"github.com/efureev/go-outbox/internal/core"
)

// seed writes messages exactly as a producer would: the public columns only,
// with every dispatcher-owned column left to its default.
func (f *fixture) seed(t testing.TB, stream string, n int) []string {
	t.Helper()

	ids := make([]string, n)
	for i := range n {
		ids[i] = f.insert(t, stream, fmt.Sprintf("topic.%d", i), []byte(fmt.Sprintf(`{"n":%d}`, i)), nil)
	}

	return ids
}

func (f *fixture) insert(t testing.TB, stream, topic string, payload []byte, target *core.Target) string {
	t.Helper()

	id := uuid.NewString()

	tgt := []byte(`{}`)
	if target != nil {
		var err error
		if tgt, err = json.Marshal(target); err != nil {
			t.Fatalf("marshal target: %v", err)
		}
	}

	_, err := f.Pool.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %q.messages (id, stream, topic, payload, target) VALUES ($1, $2, $3, $4, $5)`, f.Schema),
		id, stream, topic, payload, tgt)
	if err != nil {
		t.Fatalf("insert message: %v", err)
	}

	return id
}

// insertWithHeaders writes a message carrying producer headers, which is how a
// traceparent reaches the dispatcher in the first place.
func (f *fixture) insertWithHeaders(
	t testing.TB, stream, topic string, payload []byte, headers map[string]string,
) {
	t.Helper()

	id := uuid.NewString()

	raw, err := json.Marshal(headers)
	if err != nil {
		t.Fatalf("marshal headers: %v", err)
	}

	_, err = f.Pool.Exec(t.Context(), fmt.Sprintf(
		`INSERT INTO %q.messages (id, stream, topic, payload, headers, target)
		 VALUES ($1, $2, $3, $4, $5, '{}'::jsonb)`, f.Schema),
		id, stream, topic, payload, raw)
	if err != nil {
		t.Fatalf("insert message: %v", err)
	}
}

// lease builds a fresh lease for the given owner.
func lease(owner string, ttl time.Duration) core.Lease {
	return core.Lease{
		Token: uuid.NewString(),
		Owner: owner,
		Until: time.Now().Add(ttl),
	}
}

// row is the dispatcher-owned state of one message, for assertions.
type row struct {
	Status      core.Status
	Attempts    int
	LeaseToken  *string
	Owner       *string
	LastError   *string
	AvailableAt time.Time
	Dispatched  *time.Time
	// DeferredSince is nil unless the row is waiting on a broker that could not
	// be reached.
	DeferredSince *time.Time
}

func (f *fixture) row(t testing.TB, id string) row {
	t.Helper()

	var r row
	err := f.Pool.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT status, attempts, lease_token, owner, last_error, available_at, dispatched_at,
		        deferred_since
		   FROM %q.messages WHERE id = $1`, f.Schema), id).
		Scan(&r.Status, &r.Attempts, &r.LeaseToken, &r.Owner, &r.LastError, &r.AvailableAt,
			&r.Dispatched, &r.DeferredSince)
	if err != nil {
		t.Fatalf("read row %s: %v", id, err)
	}

	return r
}

// expire forces a lease to look overdue, standing in for a worker that died
// after claiming.
func (f *fixture) expire(t testing.TB, ids ...string) {
	t.Helper()

	_, err := f.Pool.Exec(t.Context(), fmt.Sprintf(
		`UPDATE %q.messages SET lease_until = now() - interval '1 second' WHERE id = ANY($1)`, f.Schema), ids)
	if err != nil {
		t.Fatalf("expire leases: %v", err)
	}
}

// countByStatus is used where a test cares about totals rather than individual
// rows.
func (f *fixture) countByStatus(t testing.TB, status core.Status) int {
	t.Helper()

	var n int
	err := f.Pool.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT count(*) FROM %q.messages WHERE status = $1`, f.Schema), status).Scan(&n)
	if err != nil {
		t.Fatalf("count status %s: %v", status, err)
	}

	return n
}

func ids(messages []core.Message) []string {
	out := make([]string, len(messages))
	for i, m := range messages {
		out[i] = m.ID
	}

	return out
}

// env builds a configuration source from key=value pairs, so a test never
// depends on the process environment.
func env(t testing.TB, kv ...string) *envi.Env {
	t.Helper()

	e := envi.New()
	for _, pair := range kv {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			t.Fatalf("malformed pair %q", pair)
		}
		e.Set(k, v)
	}

	return e
}

// uniqueName keeps queues and topics from colliding across runs, since neither
// broker is torn down between them.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, os.Getpid(), schemaCounter.Add(1))
}

func newID() string { return uuid.NewString() }

// notifyWait bounds how long a test waits for a NOTIFY to arrive.
const notifyWait = 5 * time.Second

func mustContext(t testing.TB, d time.Duration) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)

	return ctx
}
