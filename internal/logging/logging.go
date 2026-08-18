// Package logging builds the application logger.
//
// reggol is confined to this package: everything else in the program takes a
// *slog.Logger. That keeps the logging library out of the domain packages —
// the previous version threaded a vendor-specific logger interface through
// every constructor, which is most of why it could not be lifted out of its
// original repository — and it is what appmod and msghub accept anyway.
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

	opts := []reggol.Option{
		reggol.WithLevel(level),
		reggol.WithEncoder(encoder(cfg.Format)),
	}
	if cfg.Caller {
		opts = append(opts, reggol.WithCaller())
	}

	// The global threshold gates every event on top of the logger's own, so it
	// has to be lowered too or a debug level in the configuration has no
	// effect.
	reggol.SetGlobalLevel(level)

	return slogr.New(reggol.New(reggol.SyncWriter(w), opts...)), nil
}

func encoder(format string) reggol.Encoder {
	switch strings.ToLower(format) {
	case "console":
		return reggol.NewConsoleEncoder()
	case "text":
		return reggol.NewTextEncoder()
	default:
		// slog's own key names, so a collector configured for the standard
		// library's JSON output reads these records unchanged.
		return reggol.NewJSONEncoder(
			reggol.WithKeyNames(slog.TimeKey, slog.LevelKey, slog.MessageKey),
		)
	}
}

// Nop returns a logger that discards everything, for tests.
func Nop() *slog.Logger { return slogr.New(reggol.Nop()) }
