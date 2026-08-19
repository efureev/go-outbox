package logging

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/config"
)

func TestNewWritesJSONWithSlogKeys(t *testing.T) {
	var buf bytes.Buffer

	log, err := New(config.LogConfig{Level: "info", Format: "json"}, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.Info("claimed", slog.String("stream", "local"), slog.Int("count", 7))

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("output is not one JSON object per line: %v\n%s", err, buf.String())
	}

	if rec[slog.MessageKey] != "claimed" {
		t.Errorf("%s = %v, want claimed", slog.MessageKey, rec[slog.MessageKey])
	}
	if rec["stream"] != "local" {
		t.Errorf("stream = %v, want local", rec["stream"])
	}
	if rec["count"] != float64(7) {
		t.Errorf("count = %v, want 7", rec["count"])
	}
}

func TestNewHonoursLevel(t *testing.T) {
	var buf bytes.Buffer

	log, err := New(config.LogConfig{Level: "warn", Format: "json"}, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.Info("suppressed")
	if buf.Len() != 0 {
		t.Errorf("info should be below the warn threshold, got %q", buf.String())
	}

	log.Warn("emitted")
	if !strings.Contains(buf.String(), "emitted") {
		t.Errorf("warn should pass the threshold, got %q", buf.String())
	}
}

// A debug level in the configuration must actually produce debug records. The
// global threshold gates every event alongside the logger's own, so lowering
// only one of the two silently drops them.
func TestDebugLevelReachesTheOutput(t *testing.T) {
	var buf bytes.Buffer

	log, err := New(config.LogConfig{Level: "debug", Format: "json"}, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.Debug("visible")
	if !strings.Contains(buf.String(), "visible") {
		t.Errorf("debug record was dropped, got %q", buf.String())
	}
}

func TestNewRejectsUnknownLevel(t *testing.T) {
	if _, err := New(config.LogConfig{Level: "chatty", Format: "json"}, &bytes.Buffer{}); err == nil {
		t.Fatal("want an error for an unknown level")
	}
}

// Two events inside one second must be orderable. reggol's console default is
// time.Kitchen — "3:19AM" — which cannot even order two events inside a minute.
func TestConsoleTimestampCarriesSecondsAndMilliseconds(t *testing.T) {
	var buf bytes.Buffer

	log, err := New(config.LogConfig{Level: "info", Format: "console"}, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.Info("first")
	log.Info("second")

	stamp := regexp.MustCompile(`^\d{2}:\d{2}:\d{2}\.\d{3} `)
	for i, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if !stamp.MatchString(stripANSI(line)) {
			t.Errorf("line %d does not start with HH:MM:SS.mmm: %q", i, stripANSI(line))
		}
	}
}

// The subsystem is rendered ahead of the message, where it is read rather than
// hunted for among the data fields — and only there: repeating it as a field
// would put the same name on the line twice, which is the noise this set out to
// remove. In the console the prefix is the token to grep for.
func TestConsolePrefixesTheMessageWithTheModule(t *testing.T) {
	var buf bytes.Buffer

	log, err := New(config.LogConfig{Level: "info", Format: "console"}, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.With(slog.String(ModuleKey, "db")).Info("database pool opened", slog.Int("conns", 10))

	out := stripANSI(buf.String())
	if !strings.Contains(out, "[db] database pool opened") {
		t.Errorf("the module is not rendered before the message: %q", out)
	}
	if strings.Contains(out, ModuleKey+"=db") {
		t.Errorf("the module appears twice on the line: %q", out)
	}
	if !strings.Contains(out, "conns=10") {
		t.Errorf("the other fields were dropped along with it: %q", out)
	}
}

// A framework whose logging belongs to no single module still gets a name.
func TestConsoleFallsBackToTheComponent(t *testing.T) {
	var buf bytes.Buffer

	log, err := New(config.LogConfig{Level: "info", Format: "console"}, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.With(slog.String(ComponentKey, "lifecycle")).Info("shutdown started")

	out := stripANSI(buf.String())
	if !strings.Contains(out, "[lifecycle] shutdown started") {
		t.Errorf("the component was not used as the prefix: %q", out)
	}
	if strings.Contains(out, ComponentKey+"=") {
		t.Errorf("the component appears twice on the line: %q", out)
	}
}

// A logger with nothing bound must not produce an empty pair of brackets.
func TestConsoleWithoutAModuleAddsNoPrefix(t *testing.T) {
	var buf bytes.Buffer

	log, err := New(config.LogConfig{Level: "info", Format: "console"}, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.Info("no owner")

	if out := stripANSI(buf.String()); strings.Contains(out, "[]") {
		t.Errorf("an unbound logger produced an empty prefix: %q", out)
	}
}

// The prefix is a convenience for a human reading a terminal. A collector wants
// the message unadorned and the name in its own field.
func TestJSONKeepsTheMessageUnadorned(t *testing.T) {
	var buf bytes.Buffer

	log, err := New(config.LogConfig{Level: "info", Format: "json"}, &buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.With(
		slog.String(ModuleKey, "db"),
		slog.String(InstanceKey, "outbox-7d9f"),
	).Info("database pool opened")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("not a JSON object: %v\n%s", err, buf.String())
	}

	if got := rec[slog.MessageKey]; got != "database pool opened" {
		t.Errorf("%s = %v, want the message without a prefix", slog.MessageKey, got)
	}
	if rec[ModuleKey] != "db" {
		t.Errorf("%s = %v, want db", ModuleKey, rec[ModuleKey])
	}
	if rec[InstanceKey] != "outbox-7d9f" {
		t.Errorf("%s = %v, want the replica name", InstanceKey, rec[InstanceKey])
	}
	if _, err := time.Parse(time.RFC3339Nano, rec[slog.TimeKey].(string)); err != nil {
		t.Errorf("timestamp %v is not RFC3339Nano: %v", rec[slog.TimeKey], err)
	}
}

// The regression: the bridge records a call site on every event, so the setting
// meant to control it did nothing at all and every line carried one — for
// dependencies, a path inside the Go module cache.
func TestCallerFollowsTheSetting(t *testing.T) {
	for _, tc := range []struct {
		name string
		on   bool
		want bool
	}{
		{"off by default", false, false},
		{"on when asked", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			log, err := New(config.LogConfig{Level: "info", Format: "console", Caller: tc.on}, &buf)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			log.Info("hello")

			out := stripANSI(buf.String())
			if got := strings.Contains(out, ".go:"); got != tc.want {
				t.Errorf("caller present = %v, want %v: %q", got, tc.want, out)
			}
		})
	}
}

// stripANSI removes the colour escapes the console encoder adds, so assertions
// match on the text rather than on the styling.
func stripANSI(s string) string {
	return regexp.MustCompile(`\x1b\[[0-9;]*m`).ReplaceAllString(s, "")
}

// Binding a field must cost once, at construction, not on every record.
//
// The bridge folds bound attributes into a child reggol logger, which encodes
// them once and copies the bytes per line. Were that not so, the instance name
// now on every line would buy attribution at the price of an allocation per
// record — and the property is not visible from the call site, so it is pinned
// here.
func TestBoundFieldsCostNothingPerRecord(t *testing.T) {
	perRecord := func(bind bool) float64 {
		log, err := New(config.LogConfig{Level: "info", Format: "json"}, io.Discard)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		if bind {
			log = log.With(
				slog.String(InstanceKey, "outbox-7d9f-x4k2"),
				slog.String(ModuleKey, "dispatch"),
				slog.String("driver", "rmq_local"),
			)
		}

		return testing.AllocsPerRun(200, func() { log.Info("published") })
	}

	// Equality rather than an absolute count: the exact number moves with the
	// Go release, the property does not.
	if bound, bare := perRecord(true), perRecord(false); bound != bare {
		t.Errorf("a record costs %v allocations with bound fields and %v without; "+
			"bound fields are supposed to be pre-encoded once", bound, bare)
	}
}
