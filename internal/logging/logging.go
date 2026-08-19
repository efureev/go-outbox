// Package logging builds the application logger.
//
// reggol is confined to this package: everything else in the program takes a
// *slog.Logger. That keeps the logging library out of the domain packages,
// which would otherwise carry a vendor-specific logger interface through every
// constructor — and *slog.Logger is what appmod and msghub accept anyway.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/efureev/reggol"
	"github.com/efureev/reggol/slogr"

	"github.com/efureev/go-outbox/internal/config"
)

// New returns a logger writing to w in the configured format.
//
// The writer is synchronized: several goroutines publish concurrently, and
// interleaved bytes from two events are worse than either event alone.
func New(cfg config.LogConfig, w io.Writer) (*slog.Logger, error) {
	level, err := reggol.ParseLevel(strings.ToLower(cfg.Level))
	if err != nil {
		return nil, fmt.Errorf("log level %q: %w", cfg.Level, err)
	}

	format := strings.ToLower(cfg.Format)

	// reggol.WithCaller is deliberately absent: the slog bridge records a call
	// site on every event regardless of it, so the setting is honoured in the
	// handler below instead — where it can actually take effect.
	base := reggol.New(reggol.SyncWriter(w),
		reggol.WithLevel(level),
		reggol.WithEncoder(encoder(format)),
	)

	// The global threshold gates every event on top of the logger's own, so it
	// has to be lowered too or a debug level in the configuration has no
	// effect.
	reggol.SetGlobalLevel(level)

	return slog.New(&handler{
		next: slogr.NewHandler(base),
		// Only the human-facing format gets the name ahead of the message; a
		// collector reading JSON wants the message unadorned.
		prefix: format == formatConsole,
		caller: cfg.Caller,
	}), nil
}

// Supported output formats.
const (
	formatConsole = "console"
	formatText    = "text"
	formatJSON    = "json"
)

// Timestamp layouts.
//
// The console default in reggol is time.Kitchen — "3:19AM" — which cannot order
// two events inside the same minute, let alone the same second. Delivery
// latency here is measured in milliseconds, so the console carries them too.
const (
	consoleTimeFormat = "15:04:05.000"
	textTimeFormat    = "2006-01-02T15:04:05.000Z07:00"
)

func encoder(format string) reggol.Encoder {
	switch format {
	case formatConsole:
		return reggol.NewConsoleEncoder(
			reggol.WithConsoleOptions(reggol.WithTimeFormat(consoleTimeFormat)),
		)
	case formatText:
		return reggol.NewTextEncoder(reggol.WithTimeFormat(textTimeFormat))
	default:
		// slog's own key names, so a collector configured for the standard
		// library's JSON output reads these records unchanged. The default
		// layout is already RFC3339Nano.
		return reggol.NewJSONEncoder(
			reggol.WithKeyNames(slog.TimeKey, slog.LevelKey, slog.MessageKey),
		)
	}
}

// Nop returns a logger that discards everything, for tests.
func Nop() *slog.Logger { return slogr.New(reggol.Nop()) }
