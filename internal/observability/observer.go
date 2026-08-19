package observability

import (
	"context"

	"github.com/efureev/appmod/adapters/hubmod"
	"github.com/efureev/appmod/v4"
	"github.com/efureev/msghub/v3"

	"github.com/efureev/go-outbox/internal/events"
)

// Subscribe wires the metric set to the domain events, tying the subscriptions
// to the module's lifecycle so a restart does not leave a second handler behind.
//
// Every handler is Synchronous: it runs inline inside Publish rather than on a
// queue. That is deliberate. A queued handler can be delayed or, under an
// overflow policy, dropped — and a dropped counter increment is a metric that
// silently disagrees with reality. Running inline costs a few function calls per
// iteration, against a batch that just made hundreds of broker round trips.
func Subscribe(mod *appmod.BaseAppModule, hub *msghub.Hub, m *Metrics) error {
	if err := hubmod.SubscribeModule(mod, hub, events.TopicIteration,
		m.onIteration, msghub.Synchronous()); err != nil {
		return err
	}

	if err := hubmod.SubscribeModule(mod, hub, events.TopicReclaimed,
		m.onReclaimed, msghub.Synchronous()); err != nil {
		return err
	}

	if err := hubmod.SubscribeModule(mod, hub, events.TopicStats,
		m.onStats, msghub.Synchronous()); err != nil {
		return err
	}

	if err := hubmod.SubscribeModule(mod, hub, events.TopicRetention,
		m.onRetention, msghub.Synchronous()); err != nil {
		return err
	}

	if err := hubmod.SubscribeModule(mod, hub, events.TopicDeadLetter,
		m.onDeadLetter, msghub.Synchronous()); err != nil {
		return err
	}

	if err := hubmod.SubscribeModule(mod, hub, events.TopicBreaker,
		m.onBreaker, msghub.Synchronous()); err != nil {
		return err
	}

	return hubmod.SubscribeModule(mod, hub, events.TopicPartitions,
		m.onPartitions, msghub.Synchronous())
}

func (m *Metrics) onIteration(_ context.Context, ev events.Iteration) error {
	stream, driver := m.Stream(ev.Stream), m.Driver(ev.Driver)

	m.BatchSize.WithLabelValues(stream).Observe(float64(ev.Claimed))
	m.IterationDuration.WithLabelValues(stream).Observe(ev.Duration.Seconds())

	initial := ev.Claimed - ev.Retries
	if initial > 0 {
		m.MessagesClaimed.WithLabelValues(stream, driver, AttemptInitial).Add(float64(initial))
	}
	if ev.Retries > 0 {
		m.MessagesClaimed.WithLabelValues(stream, driver, AttemptRetry).Add(float64(ev.Retries))
	}

	for _, pub := range ev.Publishes {
		result := ResultSuccess
		if pub.Err != nil {
			result = ResultError

			m.BrokerErrors.WithLabelValues(stream, driver, "publish",
				ErrorKind(pub.Permanent, pub.Deferred)).Inc()
		}
		m.PublishDuration.WithLabelValues(stream, driver, result).Observe(pub.Duration.Seconds())
	}

	for _, d := range ev.Delivered {
		m.MessagesDispatched.WithLabelValues(stream, driver).Inc()
		m.DeliveryLag.WithLabelValues(stream, driver).Observe(d.Lag.Seconds())
	}

	if n := len(ev.Requeued); n > 0 {
		m.MessagesRetried.WithLabelValues(stream, driver).Add(float64(n))
	}

	// Deferrals are counted apart from retries on purpose. Both put a message
	// back, but one is the dispatcher working through a rejection and the other
	// is it waiting for a broker, and an alert that cannot tell them apart
	// fires on the wrong one.
	if n := len(ev.Deferred); n > 0 {
		m.MessagesDeferred.WithLabelValues(stream, driver).Add(float64(n))
	}

	for _, f := range ev.Failed {
		reason := ReasonExhausted
		switch {
		case f.Permanent:
			reason = ReasonPermanent
		case f.Deferred:
			reason = ReasonUnreachable
		}
		m.MessagesFailed.WithLabelValues(stream, driver, reason).Inc()
	}

	if ev.Conflicts > 0 {
		m.LeaseConflicts.WithLabelValues(stream).Add(float64(ev.Conflicts))
	}

	return nil
}

func (m *Metrics) onReclaimed(_ context.Context, ev events.Reclaimed) error {
	stream := m.Stream(ev.Stream)

	m.MessagesReclaimed.WithLabelValues(stream).Inc()
	m.ReclaimedAge.WithLabelValues(stream).Observe(ev.Overdue.Seconds())

	return nil
}

func (m *Metrics) onStats(_ context.Context, ev events.Stats) error {
	m.ObserveStatusCounts(StatusCounts{
		Pending:       ev.Pending,
		Processing:    ev.Processing,
		Failed:        ev.Failed,
		Deferred:      ev.Deferred,
		OldestPending: ev.OldestPending,
	})

	return nil
}

func (m *Metrics) onPartitions(_ context.Context, ev events.Partitions) error {
	m.PartitionsDropped.Add(float64(ev.Dropped))
	m.DefaultPartitionRows.Set(float64(ev.DefaultRows))

	return nil
}

func (m *Metrics) onRetention(_ context.Context, ev events.Retention) error {
	m.RetentionDeleted.Add(float64(ev.Deleted))

	return nil
}

func (m *Metrics) onBreaker(_ context.Context, ev events.Breaker) error {
	paused := 0.0
	if ev.Paused {
		paused = 1
	}
	m.StreamPaused.WithLabelValues(m.Stream(ev.Stream)).Set(paused)

	return nil
}

func (m *Metrics) onDeadLetter(_ context.Context, ev events.DeadLetter) error {
	result := ResultSuccess
	if ev.Err != nil {
		result = ResultError
	}
	m.DLQPublished.WithLabelValues(m.Stream(ev.Stream), result).Inc()

	return nil
}
