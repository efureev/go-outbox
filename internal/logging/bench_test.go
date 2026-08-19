package logging

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/efureev/go-outbox/internal/config"
)

// What a log line costs, measured rather than reasoned about.
//
// The figure matters less than where it applies: the dispatcher's hot path —
// claim, publish, write back — emits no log lines at all, so these numbers
// describe the error and lifecycle paths. TestHotPathDoesNotLogPerMessage in
// test/integration is what keeps that true.
func BenchmarkLog(b *testing.B) {
	cases := []struct {
		name   string
		format string
		attrs  []any
	}{
		{"json/no-attrs", "json", nil},
		{"json/three-attrs", "json", []any{
			slog.String("stream", "local"),
			slog.Int("messages", 42),
			slog.Any("error", errBench),
		}},
		{"console/no-attrs", "console", nil},
		{"console/three-attrs", "console", []any{
			slog.String("stream", "local"),
			slog.Int("messages", 42),
			slog.Any("error", errBench),
		}},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			// Bound the way the application binds them: an instance on the root
			// and a module per subsystem.
			log := benchLogger(b, c.format).With(
				slog.String(InstanceKey, "outbox-7d9f-x4k2"),
				slog.String(ModuleKey, "dispatch"),
			)

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				log.Warn("lease was reclaimed while publishing", c.attrs...)
			}
		})
	}
}

// A call below the threshold still evaluates and boxes its arguments, so it is
// worth knowing what an unwanted Debug in a loop would cost even switched off.
func BenchmarkLogBelowThreshold(b *testing.B) {
	log := benchLogger(b, "json").With(slog.String(ModuleKey, "dispatch"))

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		log.Debug("published", slog.String("id", "0199a1b2-c3d4-7000-8000-000000000001"))
	}
}

var errBench = errors.New("broker unavailable")

func benchLogger(b *testing.B, format string) *slog.Logger {
	b.Helper()

	log, err := New(config.LogConfig{Level: "info", Format: format}, io.Discard)
	if err != nil {
		b.Fatalf("New: %v", err)
	}

	return log
}
