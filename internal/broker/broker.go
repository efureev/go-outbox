// Package broker publishes messages to the configured message brokers.
//
// The contract is batch-shaped: a publisher takes a slice and returns one error
// per message, positionally. That is not decoration. Kafka's writer accepts a
// batch in one round trip and reports per-message errors, so publishing one
// message per call would give up an order of magnitude of throughput for
// nothing.
package broker

import (
	"context"
	"fmt"
	"strings"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
)

// Publisher sends messages to one broker.
type Publisher interface {
	// Publish sends msgs and returns a slice of the same length, where entry i
	// is the outcome of msgs[i]: nil on success, a *core.PermanentError for a
	// failure retrying cannot fix, any other error for a retryable one.
	Publish(ctx context.Context, msgs []core.Message) []error
	// HealthCheck reports whether the broker is currently reachable.
	HealthCheck(ctx context.Context) error
	// Close releases the connection.
	Close(ctx context.Context) error
}

// Destination is where one message goes, with the naming rules already applied.
type Destination struct {
	// Topic is the effective name: prefix, topic and version suffix combined.
	// A consumer subscribes to this, not to the bare topic stored in the row.
	Topic string
	// Key is the partition key. Kafka uses it; RabbitMQ ignores it.
	Key []byte
	// Exchange and RoutingKey are RabbitMQ's. An empty exchange is the default
	// exchange and an empty routing key means the topic, so a message that says
	// nothing about routing still goes somewhere sensible.
	Exchange   string
	RoutingKey string
}

// Resolve applies a driver's naming rules to a message.
func Resolve(naming config.Naming, msg core.Message) Destination {
	topic := naming.Format(msg.Topic, msg.Target.Version)

	routingKey := msg.Target.RoutingKey
	if routingKey == "" {
		routingKey = topic
	}

	d := Destination{
		Topic:      topic,
		Exchange:   msg.Target.Exchange,
		RoutingKey: routingKey,
	}

	if key := strings.TrimSpace(msg.Target.Key); key != "" {
		d.Key = []byte(key)
	}

	return d
}

// Router sends a batch to the publisher configured for its stream.
type Router struct {
	streams   map[string]string
	publisher map[string]Publisher
}

// NewRouter builds the routing table. Every stream is verified to have a
// publisher, so an unroutable stream is a startup error rather than a run-time
// surprise on the first message that uses it.
func NewRouter(brokers config.BrokerConfig, publishers map[string]Publisher) (*Router, error) {
	streams := make(map[string]string, len(brokers.Streams))

	for name, stream := range brokers.Streams {
		if _, ok := publishers[stream.Driver]; !ok {
			return nil, fmt.Errorf("stream %q needs driver %q, which was not built: %w",
				name, stream.Driver, core.ErrUnknownDriver)
		}
		streams[name] = stream.Driver
	}

	return &Router{streams: streams, publisher: publishers}, nil
}

// DriverFor reports which driver serves a stream.
func (r *Router) DriverFor(stream string) (string, bool) {
	name, ok := r.streams[strings.ToLower(stream)]

	return name, ok
}

// Publish sends a batch that all belongs to one stream. The dispatcher runs a
// pipeline per stream, so a batch never mixes them.
//
// An unknown stream is permanent: no amount of retrying will make it appear in
// the configuration, so spending the retry budget to rediscover that is pure
// delay.
func (r *Router) Publish(ctx context.Context, stream string, msgs []core.Message) []error {
	if len(msgs) == 0 {
		return nil
	}

	driver, ok := r.DriverFor(stream)
	if !ok {
		return repeat(len(msgs), core.Permanent("unknown stream",
			fmt.Errorf("%w: %s", core.ErrUnknownStream, stream)))
	}

	publisher, ok := r.publisher[driver]
	if !ok {
		return repeat(len(msgs), core.Permanent("unknown driver",
			fmt.Errorf("%w: %s", core.ErrUnknownDriver, driver)))
	}

	results := publisher.Publish(ctx, msgs)
	if len(results) != len(msgs) {
		// A publisher that does not answer for every message is a programming
		// error in that driver. Treating the batch as retryable is the safe
		// reading: at-least-once tolerates a duplicate, not a silent drop.
		return repeat(len(msgs), fmt.Errorf(
			"driver %q returned %d results for %d messages", driver, len(results), len(msgs)))
	}

	return results
}

// Publishers returns the underlying publishers, keyed by driver name.
func (r *Router) Publishers() map[string]Publisher { return r.publisher }

// HealthCheck reports the first unhealthy driver.
func (r *Router) HealthCheck(ctx context.Context) error {
	for name, p := range r.publisher {
		if err := p.HealthCheck(ctx); err != nil {
			return fmt.Errorf("driver %q: %w", name, err)
		}
	}

	return nil
}

// Close shuts every publisher down, reporting the first failure but closing
// them all regardless.
func (r *Router) Close(ctx context.Context) error {
	var firstErr error
	for name, p := range r.publisher {
		if err := p.Close(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close driver %q: %w", name, err)
		}
	}

	return firstErr
}

func repeat(n int, err error) []error {
	out := make([]error, n)
	for i := range out {
		out[i] = err
	}

	return out
}
