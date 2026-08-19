package events

import (
	"context"
	"log/slog"

	"github.com/efureev/msghub/v3"
)

// Emitter publishes domain events onto the bus.
//
// A publish failure is logged and swallowed. The bus carries observations —
// metrics, audit, dead-lettering — and none of them is worth failing a delivery
// that already happened: the message is in the broker either way, and the
// database write that records it has its own error path.
type Emitter struct {
	hub *msghub.Hub
	log *slog.Logger
}

// NewEmitter wraps a hub.
func NewEmitter(hub *msghub.Hub, log *slog.Logger) *Emitter {
	return &Emitter{hub: hub, log: log}
}

// Iteration announces one completed pipeline cycle.
func (e *Emitter) Iteration(ctx context.Context, ev Iteration) {
	emit(e, ctx, TopicIteration, ev)
}

// Reclaimed announces one lease returned to the queue.
func (e *Emitter) Reclaimed(ctx context.Context, ev Reclaimed) {
	emit(e, ctx, TopicReclaimed, ev)
}

// Stats announces a gauge sample.
func (e *Emitter) Stats(ctx context.Context, ev Stats) {
	emit(e, ctx, TopicStats, ev)
}

// Retention announces a sweep of delivered rows.
func (e *Emitter) Retention(ctx context.Context, ev Retention) {
	emit(e, ctx, TopicRetention, ev)
}

// DeadLetter announces an attempt to forward a failed message.
func (e *Emitter) DeadLetter(ctx context.Context, ev DeadLetter) {
	emit(e, ctx, TopicDeadLetter, ev)
}

// Breaker announces that a stream stopped or resumed claiming work.
func (e *Emitter) Breaker(ctx context.Context, ev Breaker) {
	emit(e, ctx, TopicBreaker, ev)
}

// emit is a function rather than a method because Go has no generic methods:
// the payload type comes from the topic, and a method cannot carry a type
// parameter.
func emit[T any](e *Emitter, ctx context.Context, topic msghub.Topic[T], ev T) {
	if e == nil || e.hub == nil {
		return
	}

	if err := msghub.Publish(ctx, e.hub, topic, ev); err != nil {
		e.log.Warn("could not publish a domain event",
			slog.String("topic", topic.Name()),
			slog.Any("error", err),
		)
	}
}
