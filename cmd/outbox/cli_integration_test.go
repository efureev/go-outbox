//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// These run the commands the way an operator does: through the flag set, through
// config.LoadAdmin reading the environment, through a real pool, against a real
// table. Everything below the flag parsing is otherwise only reachable by
// standing up the process.
//
// The environment is the point rather than an inconvenience. withStore and
// adminStore exist to turn OUTBOX_ variables into a working store, and a test
// that handed them a store would be testing nothing they do.

const defaultTestDSN = "postgres://outbox:outbox@localhost:55432/outbox?sslmode=disable"

var cliSchemas atomic.Int64

func testDSN() string {
	if v := os.Getenv("OUTBOX_TEST_DSN"); v != "" {
		return v
	}

	return defaultTestDSN
}

// cliEnv puts the command line in front of its own schema, migrated and empty.
func cliEnv(t *testing.T) (schema string, conn *pgx.Conn) {
	t.Helper()

	schema = fmt.Sprintf("cli_%d_%d", os.Getpid(), cliSchemas.Add(1))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, testDSN())
	if err != nil {
		t.Fatalf("integration tests need PostgreSQL at %s: %v", testDSN(), err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		_, _ = conn.Exec(dropCtx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		_ = conn.Close(dropCtx)
	})

	// LoadAdmin reads the process environment, which is exactly the surface
	// under test.
	t.Setenv("OUTBOX_DB_DSN", testDSN())
	t.Setenv("OUTBOX_DB_SCHEMA", schema)
	t.Setenv("OUTBOX_DB_TABLE", "messages")
	t.Setenv("OUTBOX_LOG_FORMAT", "json")

	return schema, conn
}

func cli(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()

	var out, errs bytes.Buffer
	code = run(args, &out, &errs)

	return code, out.String(), errs.String()
}

func seedFailed(t *testing.T, conn *pgx.Conn, schema string, n int) []string {
	t.Helper()

	ids := make([]string, 0, n)
	for i := range n {
		var id string
		err := conn.QueryRow(t.Context(), fmt.Sprintf(`
			INSERT INTO %s.messages
			       (id, stream, topic, payload, status, attempts, last_error, created_at)
			VALUES (gen_random_uuid(), $1, $2, $3, 3, 5, $4, now() - make_interval(days => $5))
			RETURNING id`, schema),
			"local", fmt.Sprintf("order.%d", i), []byte(`{}`),
			"rabbitmq: publish failed\n\tconnection reset", i+1).Scan(&id)
		if err != nil {
			t.Fatalf("seed: %v", err)
		}
		ids = append(ids, id)
	}

	return ids
}

// Migration is the first command anybody runs, and status is how they check it
// worked. Both go through the same path the dispatcher's own startup uses.
func TestMigrateUpThenStatus(t *testing.T) {
	schema, conn := cliEnv(t)

	code, out, errs := cli(t, "migrate", "up")
	if code != 0 {
		t.Fatalf("migrate up exited %d: %s", code, errs)
	}
	if !strings.Contains(out, "applied 0001") {
		t.Errorf("migrate up printed:\n%s", out)
	}

	// Applied twice, it has nothing to do and says so rather than repeating the
	// list — the difference between a converged deployment and a stuck one.
	code, out, _ = cli(t, "migrate", "up")
	if code != 0 {
		t.Fatalf("the second migrate up exited %d", code)
	}
	if !strings.Contains(out, "schema is up to date") {
		t.Errorf("a second migrate up printed:\n%s", out)
	}

	code, out, errs = cli(t, "migrate", "status")
	if code != 0 {
		t.Fatalf("migrate status exited %d: %s", code, errs)
	}
	if !strings.Contains(out, "VER") || !strings.Contains(out, "APPLIED") {
		t.Errorf("migrate status printed:\n%s", out)
	}
	if strings.Contains(out, "pending") {
		t.Errorf("something is still pending after migrate up:\n%s", out)
	}

	var exists bool
	if err := conn.QueryRow(t.Context(),
		`SELECT to_regclass($1) IS NOT NULL`, schema+".messages").Scan(&exists); err != nil {
		t.Fatalf("check the table: %v", err)
	}
	if !exists {
		t.Error("migrate up reported success without creating the table")
	}
}

// Status before anything is applied has to work: it is how an operator finds out
// what is missing, and refusing to run until the migrations are applied would
// make it useless for the one job it has.
func TestMigrateStatusBeforeAnythingIsApplied(t *testing.T) {
	cliEnv(t)

	code, out, errs := cli(t, "migrate", "status")
	if code != 0 {
		t.Fatalf("migrate status on an empty schema exited %d: %s", code, errs)
	}
	if !strings.Contains(out, "pending") {
		t.Errorf("nothing is reported as pending on an empty schema:\n%s", out)
	}
}

func TestStatsAgainstARealTable(t *testing.T) {
	schema, conn := cliEnv(t)

	if code, _, errs := cli(t, "migrate", "up"); code != 0 {
		t.Fatalf("migrate: %s", errs)
	}
	seedFailed(t, conn, schema, 3)

	code, out, errs := cli(t, "stats")
	if code != 0 {
		t.Fatalf("stats exited %d: %s", code, errs)
	}
	if !strings.Contains(out, "failed         3") {
		t.Errorf("stats did not count the failed messages:\n%s", out)
	}

	code, out, errs = cli(t, "stats", "-json")
	if code != 0 {
		t.Fatalf("stats -json exited %d: %s", code, errs)
	}

	var doc struct {
		Messages struct {
			Failed  int64 `json:"failed"`
			Pending int64 `json:"pending"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("stats -json does not parse: %v\n%s", err, out)
	}
	if doc.Messages.Failed != 3 || doc.Messages.Pending != 0 {
		t.Errorf("stats -json = %+v", doc.Messages)
	}
}

func TestFailedAgainstARealTable(t *testing.T) {
	schema, conn := cliEnv(t)

	if code, _, errs := cli(t, "migrate", "up"); code != 0 {
		t.Fatalf("migrate: %s", errs)
	}
	ids := seedFailed(t, conn, schema, 3)

	code, out, errs := cli(t, "failed")
	if code != 0 {
		t.Fatalf("failed exited %d: %s", code, errs)
	}
	for _, id := range ids {
		if !strings.Contains(out, id) {
			t.Errorf("message %s is missing from the listing:\n%s", id, out)
		}
	}
	// One header and one line per message, whatever newlines the broker error
	// arrived with.
	if lines := strings.Count(strings.TrimRight(out, "\n"), "\n") + 1; lines != 4 {
		t.Errorf("the listing is %d lines, want 4:\n%s", lines, out)
	}

	code, out, _ = cli(t, "failed", "-limit", "1")
	if code != 0 {
		t.Fatalf("failed -limit exited %d", code)
	}
	if lines := strings.Count(strings.TrimRight(out, "\n"), "\n") + 1; lines != 2 {
		t.Errorf("-limit 1 printed %d lines:\n%s", lines, out)
	}

	// A stream nobody wrote to is empty, not an error.
	code, out, _ = cli(t, "failed", "-stream", "nonexistent")
	if code != 0 {
		t.Fatalf("failed -stream exited %d", code)
	}
	if !strings.Contains(out, "nothing has failed") {
		t.Errorf("an unknown stream printed:\n%s", out)
	}
}

// The whole point of the command: a message stops being retried, somebody looks
// at it, and it goes back into the queue. Requeue has to reset the attempt
// counter and the availability time along with the status, or the row is never
// selected again and the command silently does nothing.
func TestRequeueReturnsMessagesToTheQueue(t *testing.T) {
	schema, conn := cliEnv(t)

	if code, _, errs := cli(t, "migrate", "up"); code != 0 {
		t.Fatalf("migrate: %s", errs)
	}
	ids := seedFailed(t, conn, schema, 3)

	code, out, errs := cli(t, "requeue", ids[0], ids[1])
	if code != 0 {
		t.Fatalf("requeue exited %d: %s", code, errs)
	}
	if !strings.Contains(out, "requeued 2 message(s)") {
		t.Errorf("requeue printed:\n%s", out)
	}

	var status int16
	var attempts int
	var available time.Time
	err := conn.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT status, attempts, available_at FROM %s.messages WHERE id = $1`, schema),
		ids[0]).Scan(&status, &attempts, &available)
	if err != nil {
		t.Fatalf("read the requeued row: %v", err)
	}

	if status != 0 {
		t.Errorf("status = %d, want 0 — the row is not pending", status)
	}
	if attempts != 0 {
		t.Errorf("attempts = %d, want 0 — the budget was not returned", attempts)
	}
	if available.After(time.Now().Add(time.Minute)) {
		t.Errorf("available_at = %v, so the row is not due", available)
	}

	// The third was not asked for and must not have moved.
	if err := conn.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT status FROM %s.messages WHERE id = $1`, schema), ids[2]).Scan(&status); err != nil {
		t.Fatalf("read the untouched row: %v", err)
	}
	if status != 3 {
		t.Errorf("a message nobody asked about moved to status %d", status)
	}
}

// Requeueing something that is not failed is not an error, and the count is how
// the operator learns it. A command that reported success for all five would
// send somebody looking for messages that never moved.
func TestRequeueReportsWhatItCouldNotMove(t *testing.T) {
	schema, conn := cliEnv(t)

	if code, _, errs := cli(t, "migrate", "up"); code != 0 {
		t.Fatalf("migrate: %s", errs)
	}
	ids := seedFailed(t, conn, schema, 1)

	var pending string
	err := conn.QueryRow(t.Context(), fmt.Sprintf(`
		INSERT INTO %s.messages (id, stream, topic, payload, status)
		VALUES (gen_random_uuid(), 'local', 'order.pending', '{}', 0)
		RETURNING id`, schema)).Scan(&pending)
	if err != nil {
		t.Fatalf("seed a pending message: %v", err)
	}

	code, out, errs := cli(t, "requeue", ids[0], pending)
	if code != 0 {
		t.Fatalf("requeue exited %d: %s", code, errs)
	}
	if !strings.Contains(out, "1 of 2") {
		t.Errorf("requeue printed %q, want it to name both numbers", out)
	}
	if !strings.Contains(out, "not in the failed state") {
		t.Errorf("requeue printed %q, want it to say why", out)
	}
}

// -before is the bulk form: after an outage there are thousands of them and
// nobody is pasting ids.
func TestRequeueBeforeMovesTheOldOnesOnly(t *testing.T) {
	schema, conn := cliEnv(t)

	if code, _, errs := cli(t, "migrate", "up"); code != 0 {
		t.Fatalf("migrate: %s", errs)
	}
	// Seeded one, two and three days old.
	seedFailed(t, conn, schema, 3)

	cutoff := time.Now().Add(-36 * time.Hour).UTC().Format(time.RFC3339)

	code, out, errs := cli(t, "requeue", "-before", cutoff, "-json")
	if code != 0 {
		t.Fatalf("requeue -before exited %d: %s", code, errs)
	}

	var doc struct {
		Requeued int      `json:"requeued"`
		IDs      []string `json:"ids"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("requeue -json does not parse: %v\n%s", err, out)
	}
	if doc.Requeued != 2 || len(doc.IDs) != 2 {
		t.Errorf("requeued %d messages older than 36 hours, want the two of three that are",
			doc.Requeued)
	}

	var stillFailed int
	if err := conn.QueryRow(t.Context(), fmt.Sprintf(
		`SELECT count(*) FROM %s.messages WHERE status = 3`, schema)).Scan(&stillFailed); err != nil {
		t.Fatalf("count: %v", err)
	}
	if stillFailed != 1 {
		t.Errorf("%d messages are still failed, want the one inside the cutoff", stillFailed)
	}
}

// The bulk form is bounded, so an outage's worth of messages does not become one
// enormous statement.
func TestRequeueBeforeRespectsItsLimit(t *testing.T) {
	schema, conn := cliEnv(t)

	if code, _, errs := cli(t, "migrate", "up"); code != 0 {
		t.Fatalf("migrate: %s", errs)
	}
	seedFailed(t, conn, schema, 5)

	cutoff := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)

	code, out, errs := cli(t, "requeue", "-before", cutoff, "-limit", "2")
	if code != 0 {
		t.Fatalf("requeue exited %d: %s", code, errs)
	}
	if !strings.Contains(out, "requeued 2 message(s)") {
		t.Errorf("requeue printed %q with -limit 2", out)
	}
}

// A command pointed at a database that is not there has to fail, not hang and
// not report success. The exit code is what a shell script reads.
func TestACommandAgainstAnAbsentDatabaseFails(t *testing.T) {
	cliEnv(t)
	t.Setenv("OUTBOX_DB_DSN", "postgres://nobody:nobody@127.0.0.1:1/none?sslmode=disable")
	t.Setenv("OUTBOX_DB_CONNECT_TIMEOUT", "2s")

	for _, args := range [][]string{{"stats"}, {"failed"}, {"requeue", "some-id"}} {
		code, out, errs := cli(t, args...)

		if code != 1 {
			t.Errorf("%v exited %d, want 1", args, code)
		}
		if out != "" {
			t.Errorf("%v printed %q to stdout while failing", args, out)
		}
		if errs == "" {
			t.Errorf("%v failed without saying why", args)
		}
	}
}

// A misconfigured environment is caught before a connection is attempted, and
// the message names every variable that is wrong rather than the first.
func TestAMisconfiguredEnvironmentIsRefusedBeforeConnecting(t *testing.T) {
	cliEnv(t)
	os.Unsetenv("OUTBOX_DB_DSN")
	t.Setenv("OUTBOX_DB_USER", "")
	t.Setenv("OUTBOX_DB_NAME", "")

	code, _, errs := cli(t, "stats")

	if code != 1 {
		t.Errorf("exit code %d, want 1", code)
	}
	if !strings.Contains(errs, "OUTBOX_DB_USER") || !strings.Contains(errs, "OUTBOX_DB_NAME") {
		t.Errorf("the error names only some of what is missing: %q", errs)
	}
}
