package dispatch

import (
	"context"
	"log/slog"
	"maps"
	"strconv"
	"time"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/events"
)

// Fetcher reads whole messages back by identifier.
type Fetcher interface {
	FetchByIDs(ctx context.Context, ids []string) ([]core.Message, error)
}

// DeadLetterEmitter reports each forwarding attempt.
type DeadLetterEmitter interface {
	DeadLetter(ctx context.Context, ev events.DeadLetter)
}

// DeadLetter forwards messages that stopped being retried to a configured
// destination, so a consumer can react to them instead of an operator having to
// notice a row in a table.
//
// The row stays in the outbox either way. The dead-letter topic is a signal,
// not the record: a failure to publish it must never make the record itself
// harder to find, which is why nothing here can change a message's status.
//
// It runs as an asynchronous subscriber on the bus. That is the difference
// between it and the metrics observer, which is synchronous: a counter must not
// be dropped under backpressure, whereas forwarding involves a database read and
// a broker round trip and has no business happening inside the publisher's own
// loop.
type DeadLetter struct {
	fetch   Fetcher
	router  Router
	emitter DeadLetterEmitter
	cfg     config.DLQConfig
	timeout time.Duration
	log     *slog.Logger
}

// NewDeadLetter builds the forwarder.
func NewDeadLetter(
	fetch Fetcher,
	router Router,
	emitter DeadLetterEmitter,
	cfg config.Config,
	log *slog.Logger,
) *DeadLetter {
	return &DeadLetter{
		fetch:   fetch,
		router:  router,
		emitter: emitter,
		cfg:     cfg.DLQ,
		timeout: cfg.Dispatch.PublishTimeout,
		log:     log.With(slog.String("component", "dead-letter")),
	}
}

// Handle forwards the failures reported by one iteration.
func (d *DeadLetter) Handle(ctx context.Context, ev events.Iteration) error {
	if !d.cfg.Enabled || len(ev.Failed) == 0 {
		return nil
	}

	ids := make([]string, len(ev.Failed))
	attempts := make(map[string]events.Terminal, len(ev.Failed))
	for i, f := range ev.Failed {
		ids[i] = f.ID
		attempts[f.ID] = f
	}

	fetchCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	messages, err := d.fetch.FetchByIDs(fetchCtx, ids)
	if err != nil {
		d.log.Error("could not read failed messages to forward", slog.Any("error", err))

		for _, id := range ids {
			d.emitter.DeadLetter(ctx, events.DeadLetter{Stream: ev.Stream, ID: id, Err: err})
		}

		return nil
	}

	forwarded := make([]core.Message, 0, len(messages))
	for _, m := range messages {
		forwarded = append(forwarded, d.rewrite(m, attempts[m.ID]))
	}

	publishCtx, publishCancel := context.WithTimeout(ctx, d.timeout)
	defer publishCancel()

	results := d.router.Publish(publishCtx, d.cfg.Stream, forwarded)

	for i, err := range results {
		d.emitter.DeadLetter(ctx, events.DeadLetter{
			Stream: d.cfg.Stream,
			ID:     forwarded[i].ID,
			Err:    err,
		})

		if err != nil {
			d.log.Error("could not forward a failed message",
				slog.String("id", forwarded[i].ID), slog.Any("error", err))
		}
	}

	return nil
}

// rewrite readdresses a message to the dead-letter destination, recording where
// it came from and why it stopped. The payload is untouched.
func (d *DeadLetter) rewrite(msg core.Message, terminal events.Terminal) core.Message {
	headers := make(map[string]string, len(msg.Headers)+4)
	maps.Copy(headers, msg.Headers)

	headers["x-outbox-original-topic"] = msg.Topic
	headers["x-outbox-original-stream"] = msg.Stream
	headers["x-outbox-attempts"] = strconv.Itoa(terminal.Attempts)
	headers["x-outbox-permanent"] = strconv.FormatBool(terminal.Permanent)

	msg.Headers = headers
	msg.Topic = d.cfg.Topic
	msg.Stream = d.cfg.Stream
	// The routing envelope belonged to the original destination; the
	// dead-letter topic gets the driver's default routing.
	msg.Target.Exchange = ""
	msg.Target.RoutingKey = ""
	msg.Target.Version = 0

	return msg
}
