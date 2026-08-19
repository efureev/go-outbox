package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	envi "github.com/efureev/envi/v2"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/store"
)

// A broker error arrives with whatever newlines the driver put in it. Printed
// as-is into a column, one of them turns the table into something no reader —
// and no awk — can follow.
func TestOneLineFlattensBrokerErrors(t *testing.T) {
	got := oneLine("rabbitmq: publish failed\n\tconnection reset\n")

	if want := "rabbitmq: publish failed connection reset"; got != want {
		t.Errorf("oneLine = %q, want %q", got, want)
	}
	if oneLine("") != "" {
		t.Errorf("oneLine on an empty error = %q", oneLine(""))
	}
}

func TestTruncate(t *testing.T) {
	cases := map[string]struct{ in, want string }{
		"short enough is untouched": {"local", "local"},
		"exactly the limit":         {"1234567890", "1234567890"},
		"longer is cut":             {"12345678901", "123456789…"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := truncate(tc.in, 10); got != tc.want {
				t.Errorf("truncate(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// fakeAdmin is the store the administrative commands see. Their job is turning
// what it returns into something an operator can read at three in the morning,
// and that is what these tests are about.
type fakeAdmin struct {
	failed    []store.FailedMessage
	failedErr error
	askedFor  struct {
		limit  int
		stream string
	}

	stats    store.Stats
	statsErr error

	requeued   []string
	requeueErr error
	gotIDs     []string
	gotBefore  time.Time
	gotLimit   int
}

func (f *fakeAdmin) ListFailed(
	_ context.Context, _ store.Cursor, limit int, stream string,
) ([]store.FailedMessage, error) {
	f.askedFor.limit, f.askedFor.stream = limit, stream

	return f.failed, f.failedErr
}

func (f *fakeAdmin) Stats(context.Context) (store.Stats, error) { return f.stats, f.statsErr }

func (f *fakeAdmin) Requeue(_ context.Context, ids []string) ([]string, error) {
	f.gotIDs = ids

	return f.requeued, f.requeueErr
}

func (f *fakeAdmin) RequeueFailedBefore(
	_ context.Context, before time.Time, limit int,
) ([]string, error) {
	f.gotBefore, f.gotLimit = before, limit

	return f.requeued, f.requeueErr
}

func failedRow(id, stream, topic, lastError string) store.FailedMessage {
	return store.FailedMessage{
		ID: id, Stream: stream, Topic: topic, Attempts: 5,
		LastError: lastError,
		CreatedAt: time.Date(2026, 8, 19, 10, 30, 0, 0, time.UTC),
	}
}

func run3(t *testing.T, fn func(io.Writer) error) string {
	t.Helper()

	var buf bytes.Buffer
	if err := fn(&buf); err != nil {
		t.Fatalf("command: %v", err)
	}

	return buf.String()
}

// An empty result is the good news, and it has to say so. A bare header over no
// rows reads as a truncated command.
func TestNothingFailedSaysSo(t *testing.T) {
	out := run3(t, func(w io.Writer) error {
		return listFailed(t.Context(), w, &fakeAdmin{}, failedOpts{limit: 20})
	})

	if !strings.Contains(out, "nothing has failed") {
		t.Errorf("output was %q", out)
	}
	if strings.Contains(out, "STREAM") {
		t.Error("a header was printed over an empty list")
	}
}

// The table is read by eye and by awk. A long topic or a multi-line broker error
// must not push the columns out of line.
func TestTheFailedTableSurvivesAwkwardValues(t *testing.T) {
	f := &fakeAdmin{failed: []store.FailedMessage{
		failedRow("0f8f-1", "a-very-long-stream-name", "orders.created.v2.with.a.long.name",
			"rabbitmq: publish failed\n\tconnection reset by peer\n"),
	}}

	out := run3(t, func(w io.Writer) error {
		return listFailed(t.Context(), w, f, failedOpts{limit: 20})
	})

	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("a single message printed %d lines:\n%s", len(lines), out)
	}
	if !strings.Contains(lines[1], "…") {
		t.Error("the long values were not truncated, so the columns are out of line")
	}
	if strings.Contains(lines[1], "\t") {
		t.Error("a tab from the broker error reached the table")
	}
	if !strings.Contains(lines[1], "2026-08-19T10:30:00Z") {
		t.Errorf("the timestamp is not RFC3339 in UTC: %q", lines[1])
	}
}

// The flags are the operator's half of the contract: what they ask for has to
// be what the store is asked for.
func TestTheFailedFlagsReachTheQuery(t *testing.T) {
	f := &fakeAdmin{}

	_ = run3(t, func(w io.Writer) error {
		return listFailed(t.Context(), w, f, failedOpts{limit: 5, stream: "orders"})
	})

	if f.askedFor.limit != 5 || f.askedFor.stream != "orders" {
		t.Errorf("queried limit=%d stream=%q, want 5 and orders",
			f.askedFor.limit, f.askedFor.stream)
	}
}

// -json exists so the output can be piped. It has to be a document, not a table
// with brackets.
func TestFailedInJSONIsAValidDocument(t *testing.T) {
	f := &fakeAdmin{failed: []store.FailedMessage{failedRow("id-1", "local", "order.created", "boom")}}

	out := run3(t, func(w io.Writer) error {
		return listFailed(t.Context(), w, f, failedOpts{limit: 20, asJSON: true})
	})

	var doc struct {
		Messages []struct {
			ID    string `json:"id"`
			Error string `json:"last_error"`
		} `json:"messages"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("the -json output does not parse: %v\n%s", err, out)
	}
	if len(doc.Messages) != 1 || doc.Messages[0].ID != "id-1" {
		t.Errorf("document = %+v", doc)
	}
}

func TestAFailedQueryIsReportedNotPrinted(t *testing.T) {
	var buf bytes.Buffer
	err := listFailed(t.Context(), &buf,
		&fakeAdmin{failedErr: errors.New("connection refused")}, failedOpts{limit: 20})

	if err == nil {
		t.Fatal("a failed query reported success")
	}
	if buf.Len() != 0 {
		t.Errorf("a failed query still printed %q", buf.String())
	}
}

// Requeue moves only rows that were actually failed. Asking for ten and moving
// three is how an operator learns the other seven were somewhere else, and a
// bare count would hide it.
func TestRequeueSaysWhenItMovedFewerThanAsked(t *testing.T) {
	f := &fakeAdmin{requeued: []string{"a", "b", "c"}}

	out := run3(t, func(w io.Writer) error {
		return requeue(t.Context(), w, f, requeueOpts{ids: []string{"a", "b", "c", "d", "e"}})
	})

	if !strings.Contains(out, "3 of 5") {
		t.Errorf("output was %q, want it to name both numbers", out)
	}
	if !strings.Contains(out, "not in the failed state") {
		t.Errorf("output was %q, want it to say why", out)
	}
}

func TestRequeueMovingEverythingAskedIsPlain(t *testing.T) {
	f := &fakeAdmin{requeued: []string{"a", "b"}}

	out := run3(t, func(w io.Writer) error {
		return requeue(t.Context(), w, f, requeueOpts{ids: []string{"a", "b"}})
	})

	if strings.Contains(out, "of 2") {
		t.Errorf("output %q hedges about a complete requeue", out)
	}
	if !strings.Contains(out, "requeued 2 message(s)") {
		t.Errorf("output was %q", out)
	}
}

func TestRequeueByIDPassesTheIDsThrough(t *testing.T) {
	f := &fakeAdmin{requeued: []string{"a"}}

	_ = run3(t, func(w io.Writer) error {
		return requeue(t.Context(), w, f, requeueOpts{ids: []string{"a"}})
	})

	if len(f.gotIDs) != 1 || f.gotIDs[0] != "a" {
		t.Errorf("the store was asked for %v", f.gotIDs)
	}
	if !f.gotBefore.IsZero() {
		t.Error("requeueing by id also went down the -before path")
	}
}

func TestRequeueBeforeParsesTheCutoff(t *testing.T) {
	f := &fakeAdmin{requeued: []string{"a", "b"}}

	_ = run3(t, func(w io.Writer) error {
		return requeue(t.Context(), w, f,
			requeueOpts{before: "2026-01-31T23:00:00Z", limit: 250})
	})

	want := time.Date(2026, 1, 31, 23, 0, 0, 0, time.UTC)
	if !f.gotBefore.Equal(want) {
		t.Errorf("cutoff = %v, want %v", f.gotBefore, want)
	}
	if f.gotLimit != 250 {
		t.Errorf("limit = %d, want 250", f.gotLimit)
	}
	if f.gotIDs != nil {
		t.Error("the -before path also requeued by id")
	}
}

// time.Parse's own message names the layout it failed against and quotes
// non-ASCII byte by byte, which tells an operator holding a mistyped timestamp
// nothing they can act on. The replacement has to show the shape wanted.
func TestAMistypedCutoffIsExplained(t *testing.T) {
	err := requeue(t.Context(), io.Discard, &fakeAdmin{}, requeueOpts{before: "yesterday"})
	if err == nil {
		t.Fatal("a nonsensical cutoff was accepted")
	}

	msg := err.Error()
	if !strings.Contains(msg, "RFC3339") {
		t.Errorf("the error does not name the format: %q", msg)
	}
	if !strings.Contains(msg, "2026-01-31T23:00:00Z") {
		t.Errorf("the error does not show an example: %q", msg)
	}
	if !strings.Contains(msg, `"yesterday"`) {
		t.Errorf("the error does not quote what was given: %q", msg)
	}
	if strings.Contains(msg, "cannot parse") {
		t.Errorf("time.Parse's own message leaked through: %q", msg)
	}
}

func TestAFailedRequeueIsReported(t *testing.T) {
	var buf bytes.Buffer
	err := requeue(t.Context(), &buf,
		&fakeAdmin{requeueErr: errors.New("deadlock")}, requeueOpts{ids: []string{"a"}})

	if err == nil {
		t.Fatal("a failed requeue reported success")
	}
	if buf.Len() != 0 {
		t.Errorf("a failed requeue still printed %q", buf.String())
	}
}

func TestRequeueInJSONReportsBothCountAndIDs(t *testing.T) {
	f := &fakeAdmin{requeued: []string{"a", "b"}}

	out := run3(t, func(w io.Writer) error {
		return requeue(t.Context(), w, f, requeueOpts{ids: []string{"a", "b"}, asJSON: true})
	})

	var doc struct {
		Requeued int      `json:"requeued"`
		IDs      []string `json:"ids"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("the -json output does not parse: %v\n%s", err, out)
	}
	if doc.Requeued != 2 || len(doc.IDs) != 2 {
		t.Errorf("document = %+v", doc)
	}
}

func statsConfig(t *testing.T) config.Config {
	t.Helper()

	e := envi.New()
	for k, v := range map[string]string{
		"OUTBOX_DB_USER":             "outbox",
		"OUTBOX_DB_NAME":             "app",
		"OUTBOX_STREAMS":             "local,audit",
		"OUTBOX_STREAM_LOCAL_DRIVER": "rmq",
		"OUTBOX_STREAM_AUDIT_DRIVER": "rmq",
		"OUTBOX_DRIVER_RMQ_TYPE":     "rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN":      "amqp://guest:guest@localhost:5672/",
	} {
		e.Set(k, v)
	}

	cfg, err := config.LoadFrom(e)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	return cfg
}

// Deferred is a subset of pending and processing, not a fourth status. A reader
// who adds it to the others gets a number that is wrong, so the line has to say
// so where it is read.
func TestStatsWarnsThatDeferredIsCountedTwice(t *testing.T) {
	f := &fakeAdmin{stats: store.Stats{
		Pending: 100, Processing: 5, Failed: 2, Deferred: 30,
		OldestPending: 90*time.Second + 400*time.Millisecond,
	}}

	out := run3(t, func(w io.Writer) error {
		return stats(t.Context(), w, f, statsConfig(t), false)
	})

	var deferred string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "deferred") {
			deferred = line
		}
	}
	if deferred == "" {
		t.Fatalf("no deferred line:\n%s", out)
	}
	if !strings.Contains(deferred, "counted above too") {
		t.Errorf("the deferred line does not warn it overlaps: %q", deferred)
	}
	if !strings.Contains(deferred, "unreachable broker") {
		t.Errorf("the deferred line does not say what it means: %q", deferred)
	}
	// A sub-second tail in an age nobody reads to the millisecond.
	if !strings.Contains(out, "1m30s") {
		t.Errorf("the age was not truncated to the second:\n%s", out)
	}
}

func TestStatsListsTheConfiguredStreams(t *testing.T) {
	out := run3(t, func(w io.Writer) error {
		return stats(t.Context(), w, &fakeAdmin{}, statsConfig(t), false)
	})

	if !strings.Contains(out, "STREAM") || !strings.Contains(out, "DRIVER") {
		t.Errorf("no routing table:\n%s", out)
	}
	for _, name := range []string{"local", "audit"} {
		if !strings.Contains(out, name) {
			t.Errorf("stream %q is missing:\n%s", name, out)
		}
	}
}

// The counts are the point of the command. A configuration with no routing
// table must still produce them rather than an empty header.
func TestStatsWithoutARoutingTableStillCounts(t *testing.T) {
	out := run3(t, func(w io.Writer) error {
		return stats(t.Context(), w, &fakeAdmin{stats: store.Stats{Pending: 7}}, config.Config{}, false)
	})

	if !strings.Contains(out, "pending        7") {
		t.Errorf("the counts are missing:\n%s", out)
	}
	if strings.Contains(out, "STREAM") {
		t.Errorf("an empty routing table printed a header:\n%s", out)
	}
}

func TestStatsInJSONCarriesTheGaugesAndTheRouting(t *testing.T) {
	f := &fakeAdmin{stats: store.Stats{
		Pending: 100, Processing: 5, Failed: 2, Deferred: 30, OldestPending: 90 * time.Second,
	}}

	out := run3(t, func(w io.Writer) error {
		return stats(t.Context(), w, f, statsConfig(t), true)
	})

	var doc struct {
		Messages struct {
			Pending  int64   `json:"pending"`
			Deferred int64   `json:"deferred"`
			Oldest   float64 `json:"oldest_pending_seconds"`
		} `json:"messages"`
		Streams map[string]string `json:"streams"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("the -json output does not parse: %v\n%s", err, out)
	}
	if doc.Messages.Pending != 100 || doc.Messages.Deferred != 30 {
		t.Errorf("gauges = %+v", doc.Messages)
	}
	if doc.Messages.Oldest != 90 {
		t.Errorf("oldest = %v seconds, want 90", doc.Messages.Oldest)
	}
	if doc.Streams["local"] != "rmq" || doc.Streams["audit"] != "rmq" {
		t.Errorf("streams = %v", doc.Streams)
	}
}

func TestAFailedStatsQueryIsReported(t *testing.T) {
	var buf bytes.Buffer
	if err := stats(t.Context(), &buf, &fakeAdmin{statsErr: errors.New("timeout")},
		config.Config{}, false); err == nil {
		t.Fatal("a failed query reported success")
	}
	if buf.Len() != 0 {
		t.Errorf("a failed query still printed %q", buf.String())
	}
}

func TestStreamMapOnAnEmptyRoutingTable(t *testing.T) {
	if got := streamMap(config.Config{}); len(got) != 0 {
		t.Errorf("streamMap = %v, want empty", got)
	}
}

// Indented output is the difference between a document a human can read in a
// terminal and one line of several kilobytes.
func TestPrintJSONIndents(t *testing.T) {
	var buf bytes.Buffer
	if err := printJSON(&buf, map[string]any{"a": 1}); err != nil {
		t.Fatalf("printJSON: %v", err)
	}

	if !strings.Contains(buf.String(), "\n  ") {
		t.Errorf("output is not indented: %q", buf.String())
	}
}

func TestPrintJSONReportsAValueItCannotEncode(t *testing.T) {
	if err := printJSON(io.Discard, map[string]any{"f": func() {}}); err == nil {
		t.Error("a function was encoded as JSON")
	}
}
