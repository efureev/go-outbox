//go:build integration

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
)

// The dispatcher writes no log line per message, and this is what keeps it that
// way.
//
// The property is invisible at the call site: adding `p.log.Debug("published",
// …)` inside the publish loop reads like an improvement and costs an allocation
// and ~16ns per message even with the level switched off — plus the encoding
// work when it is not. At several thousand messages a second that is no longer
// a log, it is a second queue.
func TestHotPathDoesNotLogPerMessage(t *testing.T) {
	// Equality, not zero: one line per iteration would be a legitimate change
	// one day, growth proportional to the batch never is.
	small := drainAndCountLogLines(t, 100)
	large := drainAndCountLogLines(t, 1000)

	if small != large {
		t.Errorf("draining 100 messages logged %d lines and 1000 logged %d; "+
			"log volume must not follow message volume", small, large)
	}
	t.Logf("100 messages: %d lines, 1000 messages: %d lines", small, large)
}

// The counterpart: the logger must actually be wired, or the test above would
// pass just as happily against a logger that writes nothing at all.
func TestFailuresStillReachTheLog(t *testing.T) {
	f := newFixture(t)

	var buf syncBuffer

	log, err := logging.New(config.LogConfig{Level: "info", Format: "json"}, &buf)
	if err != nil {
		t.Fatalf("logger: %v", err)
	}

	// A store that cannot claim, which is what the loop reports as a failed
	// iteration.
	pipeline := dispatch.New("local", unreachableStore{f.Store}, newCountingRouter(),
		events.NewEmitter(nil, logging.Nop()), logConfig(t, f), log)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- pipeline.Run(ctx) }()

	waitFor(t, 5*time.Second, func() bool { return buf.String() != "" }, "the failure is logged")

	cancel()
	<-done

	if !strings.Contains(buf.String(), "iteration failed") {
		t.Errorf("the failure was logged as something unexpected: %q", buf.String())
	}
}

// syncBuffer is readable while the logger writes. reggol's SyncWriter
// serialises the writes; nothing serialises a test reading alongside them.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// unreachableStore stands in for a database that cannot be reached.
type unreachableStore struct{ dispatch.Store }

func (unreachableStore) Claim(context.Context, string, int, core.Lease) ([]core.Message, error) {
	return nil, errors.New("connection refused")
}

func drainAndCountLogLines(t *testing.T, messages int) int {
	t.Helper()

	f := newFixture(t)

	var buf syncBuffer

	// The most verbose level the service supports, in the production format —
	// anything the hot path might say would be captured here.
	log, err := logging.New(config.LogConfig{Level: "trace", Format: "json"}, &buf)
	if err != nil {
		t.Fatalf("logger: %v", err)
	}

	router := newCountingRouter()
	pipeline := dispatch.New("local", f.Store, router,
		events.NewEmitter(nil, logging.Nop()), logConfig(t, f), log)

	for i := range messages {
		f.insert(t, "local", "hotpath", fmt.Appendf(nil, `{"n":%d}`, i), nil)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- pipeline.Run(ctx) }()

	waitFor(t, 60*time.Second, func() bool {
		return f.countByStatus(t, core.StatusSent) == messages
	}, fmt.Sprintf("all %d messages delivered", messages))

	cancel()
	<-done

	return countLines(buf.String())
}

// countLines counts the log records written, one per line.
func countLines(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	return strings.Count(s, "\n") + 1
}

func logConfig(t *testing.T, f *fixture) config.Config {
	t.Helper()

	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_DSN="+f.Config.DB.DSN,
		"OUTBOX_DB_SCHEMA="+f.Schema,
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN="+amqpDSN(),
		"OUTBOX_DISPATCH_BATCH_SIZE=100",
		"OUTBOX_DISPATCH_WORKERS=4",
		"OUTBOX_DISPATCH_POLL_INTERVAL=20ms",
		"OUTBOX_DISPATCH_NOTIFY_ENABLED=false",
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	return cfg
}
