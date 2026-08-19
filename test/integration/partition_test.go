//go:build integration

package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/core"
)

// partitionedFixture builds the schema an operator gets from
// migrations/partitioned/messages.sql, then runs the ordinary migrations over
// it — which is the whole design: no fork of the migration set, only a table
// created differently before it runs.
func partitionedFixture(t *testing.T) *fixture {
	t.Helper()

	f := newBareFixture(t)

	ddl := fmt.Sprintf(`
		CREATE TABLE %[1]s.messages (
		    id      UUID  NOT NULL,
		    stream  TEXT  NOT NULL,
		    topic   TEXT  NOT NULL,
		    payload BYTEA NOT NULL,
		    headers JSONB NOT NULL DEFAULT '{}'::jsonb,
		    target  JSONB NOT NULL DEFAULT '{}'::jsonb,
		    status        SMALLINT    NOT NULL DEFAULT 0,
		    attempts      INTEGER     NOT NULL DEFAULT 0,
		    available_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
		    lease_token   UUID,
		    lease_until   TIMESTAMPTZ,
		    owner         TEXT,
		    last_error    TEXT,
		    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
		    dispatched_at TIMESTAMPTZ,
		    PRIMARY KEY (id, created_at),
		    CONSTRAINT messages_status_check CHECK (status BETWEEN 0 AND 3),
		    CONSTRAINT messages_attempts_check CHECK (attempts >= 0),
		    CONSTRAINT messages_lease_check CHECK (
		        ((status = 1) = (lease_token IS NOT NULL))
		        AND ((lease_token IS NULL) = (lease_until IS NULL))
		    )
		) PARTITION BY RANGE (created_at);
		CREATE TABLE %[1]s.messages_default PARTITION OF %[1]s.messages DEFAULT;`,
		quoted(f.Schema))

	if _, err := f.Pool.Exec(t.Context(), ddl); err != nil {
		t.Fatalf("create the partitioned table: %v", err)
	}

	migrateFixture(t, f.Config)

	return f
}

// The claim of the design, checked rather than asserted: the released
// migrations apply unchanged over a table somebody created partitioned, and
// every index lands on the parent.
func TestMigrationsRunOverAPartitionedTable(t *testing.T) {
	f := partitionedFixture(t)

	partitioned, err := f.Store.IsPartitioned(t.Context())
	if err != nil {
		t.Fatalf("IsPartitioned: %v", err)
	}
	if !partitioned {
		t.Fatal("the table is not partitioned, so this test proves nothing about partitioning")
	}

	var indexes int
	if err := f.Pool.QueryRow(t.Context(), `
		SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relkind = 'I'`, f.Schema).Scan(&indexes); err != nil {
		t.Fatalf("count partitioned indexes: %v", err)
	}
	// One per index in 0001 and 0004, created on the parent and propagated.
	if indexes < 6 {
		t.Errorf("%d partitioned indexes, want the ones the migrations create", indexes)
	}
}

// Partitioning is transparent to DML, which is the reason none of the
// dispatcher's queries know about it. This is the assertion that keeps that
// true.
func TestTheDispatcherWorksAgainstAPartitionedTable(t *testing.T) {
	f := partitionedFixture(t)
	f.seed(t, "local", 3)

	l := lease("a", time.Minute)
	claimed, err := f.Store.Claim(t.Context(), "local", 10, l)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d, want 3", len(claimed))
	}

	res, err := f.Store.Ack(t.Context(), ids(claimed), l.Token)
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if len(res.Delivered) != 3 {
		t.Fatalf("delivered %d, want 3", len(res.Delivered))
	}

	st, err := f.Store.Stats(t.Context())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Pending != 0 {
		t.Errorf("pending = %d, want 0", st.Pending)
	}
}

func TestEnsurePartitionsCreatesDaysAhead(t *testing.T) {
	f := partitionedFixture(t)

	if _, err := f.Store.EnsurePartitions(t.Context(), 3); err != nil {
		t.Fatalf("EnsurePartitions: %v", err)
	}

	parts, err := f.Store.Partitions(t.Context())
	if err != nil {
		t.Fatalf("Partitions: %v", err)
	}

	// Today plus three, plus the default.
	if len(parts) != 5 {
		t.Fatalf("%d partitions, want 5: today, three ahead and the default", len(parts))
	}

	// Running again must not fail, because every replica reaches this and the
	// janitor's lock covers the sweep rather than the startup path.
	if _, err := f.Store.EnsurePartitions(t.Context(), 3); err != nil {
		t.Fatalf("second EnsurePartitions: %v", err)
	}
}

// The rule that makes dropping safe. Partition bounds are on created_at and
// retention is on dispatched_at, so a partition full of week-old messages may
// still hold one that failed and is waiting for somebody. Dropping by age alone
// would delete it.
func TestDropExpiredPartitionsSpareAnythingNotDelivered(t *testing.T) {
	f := partitionedFixture(t)
	ctx := t.Context()

	old := time.Now().AddDate(0, 0, -10)
	makeDay := func(day time.Time) string {
		t.Helper()

		name := fmt.Sprintf("%s.%q", quoted(f.Schema), "messages_"+day.Format("20060102"))
		_, err := f.Pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE %s PARTITION OF %s.messages FOR VALUES FROM ('%s') TO ('%s')`,
			name, quoted(f.Schema),
			day.Format(time.DateOnly), day.AddDate(0, 0, 1).Format(time.DateOnly)))
		if err != nil {
			t.Fatalf("create partition: %v", err)
		}

		return name
	}

	makeDay(old)
	held := makeDay(old.AddDate(0, 0, 1))

	insert := func(day time.Time, status core.Status, dispatched bool) {
		t.Helper()

		var at any
		if dispatched {
			at = day
		}
		_, err := f.Pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s.messages (id, stream, topic, payload, status, created_at, dispatched_at)
			 VALUES (gen_random_uuid(), 'local', 't', '\x00'::bytea, $1, $2, $3)`, quoted(f.Schema)),
			status, day, at)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	insert(old.Add(time.Hour), core.StatusSent, true)
	// One message nobody has dealt with, in a partition just as old.
	insert(old.AddDate(0, 0, 1).Add(time.Hour), core.StatusFailed, false)

	dropped, err := f.Store.DropExpiredPartitions(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("DropExpiredPartitions: %v", err)
	}

	// The name is compared by what it ends with rather than in full: the
	// catalogue renders identifiers without quotes where none are needed, and
	// which of them need quoting is PostgreSQL's business, not this test's.
	if len(dropped) != 1 {
		t.Fatalf("dropped %v, want exactly the spent partition", dropped)
	}
	if want := "messages_" + old.Format("20060102"); !strings.HasSuffix(dropped[0], want) {
		t.Fatalf("dropped %q, want the partition ending %q", dropped[0], want)
	}

	// The one holding a failed message is still there, and so is the message.
	var failed int
	if err := f.Pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s WHERE status = 3`, held)).Scan(&failed); err != nil {
		t.Fatalf("count the held message: %v", err)
	}
	if failed != 1 {
		t.Errorf("the partition holding a failed message lost it (found %d)", failed)
	}
}

// The default partition is the reason a missing daily partition costs a warning
// rather than a producer's transaction. It is never dropped, and what lands in
// it is counted so somebody notices.
func TestTheDefaultPartitionCatchesRowsAndIsCounted(t *testing.T) {
	f := partitionedFixture(t)
	ctx := t.Context()

	// A day nobody created a partition for.
	_, err := f.Pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.messages (id, stream, topic, payload, created_at)
		 VALUES (gen_random_uuid(), 'local', 't', '\x00'::bytea, now() - interval '400 days')`,
		quoted(f.Schema)))
	if err != nil {
		t.Fatalf("the insert failed, so a missing partition would break a producer: %v", err)
	}

	n, err := f.Store.DefaultPartitionRows(ctx)
	if err != nil {
		t.Fatalf("DefaultPartitionRows: %v", err)
	}
	if n != 1 {
		t.Errorf("default partition rows = %d, want 1", n)
	}

	dropped, err := f.Store.DropExpiredPartitions(ctx, time.Hour)
	if err != nil {
		t.Fatalf("DropExpiredPartitions: %v", err)
	}
	for _, name := range dropped {
		if strings.HasSuffix(name, "messages_default") {
			t.Error("the default partition was dropped; a producer's next insert would fail")
		}
	}
}

// An ordinary table must not be treated as partitioned, or the retention path
// would silently do nothing on every deployment that did not opt in.
func TestAnOrdinaryTableIsNotPartitioned(t *testing.T) {
	f := newFixture(t)

	partitioned, err := f.Store.IsPartitioned(t.Context())
	if err != nil {
		t.Fatalf("IsPartitioned: %v", err)
	}
	if partitioned {
		t.Error("the ordinary table reports itself as partitioned")
	}

	parts, err := f.Store.Partitions(t.Context())
	if err != nil {
		t.Fatalf("Partitions: %v", err)
	}
	if len(parts) != 0 {
		t.Errorf("an ordinary table reported %d partitions", len(parts))
	}
}
