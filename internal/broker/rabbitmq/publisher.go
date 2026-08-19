package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
)

// Publisher publishes to RabbitMQ.
type Publisher struct {
	conn *Conn
	cfg  *config.RabbitMQDriver
	log  *slog.Logger
}

// New connects and returns a publisher.
func New(ctx context.Context, cfg *config.RabbitMQDriver, log *slog.Logger) (*Publisher, error) {
	conn, err := Dial(ctx, cfg, log)
	if err != nil {
		return nil, err
	}

	return &Publisher{conn: conn, cfg: cfg, log: log}, nil
}

// Publish sends each message and waits for the broker to confirm it.
//
// Messages go one at a time — AMQP has no batch publish — but they go through
// a pool of channels, so the dispatcher's workers publish concurrently instead
// of queueing behind one channel and one mutex, which would cap throughput
// however many workers were configured.
func (p *Publisher) Publish(ctx context.Context, msgs []core.Message) []error {
	results := make([]error, len(msgs))

	for i, msg := range msgs {
		results[i] = p.publishOne(ctx, msg)
	}

	return results
}

func (p *Publisher) publishOne(ctx context.Context, msg core.Message) error {
	ch, conn, err := p.conn.acquire(ctx)
	if err != nil {
		return p.classify(err)
	}
	defer conn.release(ch)

	dest := broker.Resolve(p.cfg.Naming(), msg)

	if p.cfg.Declare && dest.Exchange == "" {
		if err := p.declare(ch, dest.RoutingKey); err != nil {
			return p.classify(err)
		}
	}

	publishCtx, cancel := context.WithTimeout(ctx, p.cfg.PublishTimeout)
	defer cancel()

	// Anything still in the return stream belongs to an earlier publish that
	// has already been answered.
	ch.drainReturns()

	confirmation, err := ch.ch.PublishWithDeferredConfirmWithContext(
		publishCtx,
		dest.Exchange,
		dest.RoutingKey,
		p.cfg.Mandatory,
		false,
		p.publishing(msg),
	)
	if err != nil {
		// A channel that failed a publish is not reliably usable afterwards;
		// asking the supervisor to rebuild is cheaper than reasoning about
		// which failures are recoverable in place.
		p.conn.requestRedial()

		return p.classify(fmt.Errorf("rabbitmq: publish %s: %w", msg.ID, err))
	}

	ack, err := confirmation.WaitContext(publishCtx)
	if err != nil {
		// The confirmation that never comes is what a broker disappearing
		// mid-publish looks like from here: the write succeeded into a socket
		// that had already gone, and what expires is this deadline.
		return p.classify(fmt.Errorf("rabbitmq: awaiting confirmation for %s: %w", msg.ID, err))
	}

	// The broker sends basic.return before basic.ack, so by the time the
	// confirmation has resolved the return is already waiting to be read.
	if reason, returned := ch.takeReturn(msg.ID); returned {
		return core.Permanent("unroutable", fmt.Errorf(
			"rabbitmq returned %s: %s (exchange %q, routing key %q)",
			msg.ID, reason, dest.Exchange, dest.RoutingKey))
	}

	if !ack {
		return fmt.Errorf("rabbitmq: broker nacked %s", msg.ID)
	}

	return nil
}

func (p *Publisher) declare(ch *publishChannel, queue string) error {
	if _, cached := p.conn.declared.Load(queue); cached {
		return nil
	}

	if _, err := ch.ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		p.conn.requestRedial()

		return fmt.Errorf("rabbitmq: declare queue %q: %w", queue, err)
	}

	p.conn.declared.Store(queue, struct{}{})

	return nil
}

// publishing builds the AMQP message.
//
// MessageId carries the outbox row id. Delivery is at-least-once, so consumers
// have to deduplicate; without an identifier in the properties they would have
// to find one inside a payload this dispatcher treats as opaque bytes.
func (p *Publisher) publishing(msg core.Message) amqp.Publishing {
	pub := amqp.Publishing{
		MessageId:    msg.ID,
		Timestamp:    msg.CreatedAt,
		DeliveryMode: amqp.Persistent,
		ContentType:  contentType(msg.Headers),
		Body:         msg.Payload,
	}

	if len(msg.Headers) > 0 {
		table := make(amqp.Table, len(msg.Headers))
		for k, v := range msg.Headers {
			table[k] = v
		}
		pub.Headers = table
	}

	return pub
}

const defaultContentType = "application/octet-stream"

func contentType(headers map[string]string) string {
	for _, key := range []string{"content-type", "Content-Type", "content_type"} {
		if v, ok := headers[key]; ok && v != "" {
			return v
		}
	}

	return defaultContentType
}

// HealthCheck reports whether the connection is live.
func (p *Publisher) HealthCheck(ctx context.Context) error { return p.conn.HealthCheck(ctx) }

// Close releases the connection.
func (p *Publisher) Close(ctx context.Context) error { return p.conn.Close(ctx) }

var _ broker.Publisher = (*Publisher)(nil)
