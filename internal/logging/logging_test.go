package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

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
