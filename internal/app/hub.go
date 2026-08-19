package app

import (
	"context"
	"log/slog"

	"github.com/efureev/appmod/adapters/hubmod"
	"github.com/efureev/msghub/v3"

	"github.com/efureev/go-outbox/internal/logging"
)

// newHubModule owns the in-process bus that carries the dispatcher's domain
// events — published, failed, reclaimed — to whoever observes them: the metrics
// recorder, the dead-letter publisher, an audit sink.
//
// The bus is deliberately not on the hot path. Claiming, publishing and writing
// the outcome back run on plain channels and a worker pool, where backpressure
// has to be explicit and nothing may be dropped. What the bus decouples is the
// side effects: without it the publisher would call into Prometheus, the
// dead-letter writer and the audit log directly, which is how the previous
// version's publisher came to have three unrelated responsibilities in one
// function.
func newHubModule(log *slog.Logger) *hubmod.Module {
	l := log.With(slog.String(logging.ModuleKey, ModuleHub))

	hub := msghub.New(
		msghub.WithLogger(l),
		msghub.WithQueueSize(1024),
		// Block rather than drop: an observer falling behind should slow the
		// publisher down, not silently lose the record of what it published.
		msghub.WithOverflow(msghub.Block),
		msghub.WithErrorHandler(func(_ context.Context, topic string, err error) {
			l.Error("event handler failed", slog.String("topic", topic), slog.Any("error", err))
		}),
	)

	return hubmod.NewModule(hub)
}
