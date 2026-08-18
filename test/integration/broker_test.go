//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	segmentio "github.com/segmentio/kafka-go"

	"github.com/efureev/go-outbox/internal/broker"
	"github.com/efureev/go-outbox/internal/broker/kafka"
	"github.com/efureev/go-outbox/internal/broker/rabbitmq"
	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
	"github.com/efureev/go-outbox/internal/logging"
)

func amqpDSN() string {
	if v := os.Getenv("OUTBOX_TEST_AMQP_DSN"); v != "" {
		return v
	}

	return "amqp://outbox:outbox@localhost:55672/"
}

func kafkaBrokers() []string {
	if v := os.Getenv("OUTBOX_TEST_KAFKA_BROKERS"); v != "" {
		return []string{v}
	}

	return []string{"localhost:19092"}
}

// rabbitDriver builds a driver configuration through the public loader, so the
// tests exercise the same parsing production uses.
func rabbitDriver(t *testing.T, extra ...string) *config.RabbitMQDriver {
	t.Helper()

	pairs := append([]string{
		"OUTBOX_DB_USER=outbox",
		"OUTBOX_DB_NAME=outbox",
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN=" + amqpDSN(),
	}, extra...)

	cfg, err := config.LoadFrom(env(t, pairs...))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	d, ok := cfg.Brokers.Drivers["rmq"].(*config.RabbitMQDriver)
	if !ok {
		t.Fatalf("driver is %T, want *config.RabbitMQDriver", cfg.Brokers.Drivers["rmq"])
	}

	return d
}

func kafkaDriver(t *testing.T, extra ...string) *config.KafkaDriver {
	t.Helper()

	pairs := append([]string{
		"OUTBOX_DB_USER=outbox",
		"OUTBOX_DB_NAME=outbox",
		"OUTBOX_STREAMS=global",
		"OUTBOX_STREAM_GLOBAL_DRIVER=kfk",
		"OUTBOX_DRIVER_KFK_TYPE=kafka",
		"OUTBOX_DRIVER_KFK_BROKERS=" + kafkaBrokers()[0],
	}, extra...)

	cfg, err := config.LoadFrom(env(t, pairs...))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	d, ok := cfg.Brokers.Drivers["kfk"].(*config.KafkaDriver)
	if !ok {
		t.Fatalf("driver is %T, want *config.KafkaDriver", cfg.Brokers.Drivers["kfk"])
	}

	return d
}

func message(id, topic string, body []byte) core.Message {
	return core.Message{
		ID:        id,
		Stream:    "local",
		Topic:     topic,
		Payload:   body,
		Headers:   map[string]string{"traceparent": "00-" + id + "-0000000000000001-01"},
		CreatedAt: time.Now(),
	}
}

func TestRabbitMQPublishesAndIsConfirmed(t *testing.T) {
	driver := rabbitDriver(t, "OUTBOX_DRIVER_RMQ_DECLARE=true")

	p, err := rabbitmq.New(t.Context(), driver, logging.Nop())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(context.WithoutCancel(t.Context())) })

	queue := uniqueName("rmq.publish")
	id := newID()

	results := p.Publish(t.Context(), []core.Message{message(id, queue, []byte(`{"hello":"world"}`))})
	if len(results) != 1 {
		t.Fatalf("got %d results for 1 message", len(results))
	}
	if results[0] != nil {
		t.Fatalf("publish: %v", results[0])
	}

	got := consumeOne(t, queue)
	if string(got.Body) != `{"hello":"world"}` {
		t.Errorf("body = %q, want the payload byte for byte", got.Body)
	}
	// A consumer told to deduplicate needs an identifier that does not require
	// parsing the payload.
	if got.MessageId != id {
		t.Errorf("MessageId = %q, want the outbox row id %q", got.MessageId, id)
	}
	if got.Headers["traceparent"] != "00-"+id+"-0000000000000001-01" {
		t.Errorf("traceparent did not survive: %v", got.Headers["traceparent"])
	}
	if got.DeliveryMode != amqp.Persistent {
		t.Errorf("DeliveryMode = %d, want persistent", got.DeliveryMode)
	}
}

// An unroutable message is permanent: no exchange is going to appear because
// the dispatcher waited an hour and tried again.
func TestRabbitMQUnroutableMessageIsPermanent(t *testing.T) {
	driver := rabbitDriver(t)

	p, err := rabbitmq.New(t.Context(), driver, logging.Nop())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(context.WithoutCancel(t.Context())) })

	// Nothing declares this queue, and mandatory is on by default, so the
	// broker returns the message instead of discarding it.
	msg := message(newID(), uniqueName("rmq.nowhere"), []byte(`{}`))

	results := p.Publish(t.Context(), []core.Message{msg})
	if results[0] == nil {
		t.Fatal("publishing to a queue that does not exist must not report success")
	}
	if !core.IsPermanent(results[0]) {
		t.Errorf("error is %v; an unroutable message must be permanent so the retry budget is not spent on it",
			results[0])
	}
}

// The defect this guards against: sharing one confirmation channel across every
// publish and reading a single value after each. A publish that times out leaves
// its confirmation queued, and the next message consumes it — reporting success
// for a message the broker never confirmed.
//
// Each publish waits on its own deferred confirmation instead, so a batch of
// messages published back to back is confirmed one for one.
func TestRabbitMQConfirmationsAreNotCrossed(t *testing.T) {
	// One channel, so every publish shares a single confirmation stream — the
	// arrangement in which crossed confirmations would show up. Declaration
	// stays off: the publisher must not create the queue the middle message is
	// meant to miss.
	driver := rabbitDriver(t, "OUTBOX_DRIVER_RMQ_CHANNELS=1")

	p, err := rabbitmq.New(t.Context(), driver, logging.Nop())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(context.WithoutCancel(t.Context())) })

	good := uniqueName("rmq.confirm.good")
	declareQueue(t, good)

	// One deliverable message, then one that cannot be routed, then another
	// deliverable one. If confirmations were shared, the third would consume
	// the second's outcome.
	msgs := []core.Message{
		message(newID(), good, []byte(`1`)),
		message(newID(), uniqueName("rmq.confirm.missing"), []byte(`2`)),
		message(newID(), good, []byte(`3`)),
	}

	results := p.Publish(t.Context(), msgs)

	if results[0] != nil {
		t.Errorf("message 0 should have been delivered: %v", results[0])
	}
	if results[1] == nil {
		t.Error("message 1 is unroutable and must not report success")
	}
	if results[2] != nil {
		t.Errorf("message 2 should have been delivered: %v", results[2])
	}
}

func TestRabbitMQPrefixAndVersionReachTheBroker(t *testing.T) {
	driver := rabbitDriver(t,
		"OUTBOX_DRIVER_RMQ_DECLARE=true",
		"OUTBOX_DRIVER_RMQ_PREFIX=stage",
	)

	p, err := rabbitmq.New(t.Context(), driver, logging.Nop())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(context.WithoutCancel(t.Context())) })

	base := uniqueName("rmq.named")

	msg := message(newID(), base, []byte(`{}`))
	msg.Target.Version = 2

	if err := p.Publish(t.Context(), []core.Message{msg})[0]; err != nil {
		t.Fatalf("publish: %v", err)
	}

	// The consumer has to subscribe to the effective name, not the bare topic.
	effective := fmt.Sprintf("stage_%s_v2", base)
	if got := consumeOne(t, effective); string(got.Body) != `{}` {
		t.Errorf("nothing arrived on %s", effective)
	}
}

func TestRabbitMQHealthCheck(t *testing.T) {
	p, err := rabbitmq.New(t.Context(), rabbitDriver(t), logging.Nop())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	if err := p.HealthCheck(t.Context()); err != nil {
		t.Errorf("a freshly connected publisher is unhealthy: %v", err)
	}

	if err := p.Close(context.WithoutCancel(t.Context())); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := p.HealthCheck(t.Context()); !errors.Is(err, rabbitmq.ErrNotConnected) {
		t.Errorf("HealthCheck after Close = %v, want ErrNotConnected", err)
	}
}

func TestKafkaPublishesABatchInOneCall(t *testing.T) {
	driver := kafkaDriver(t, "OUTBOX_DRIVER_KFK_ALLOW_AUTO_TOPIC_CREATION=true")

	p, err := kafka.New(t.Context(), driver, logging.Nop())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(context.WithoutCancel(t.Context())) })

	topic := uniqueName("kafka-batch")

	const batch = 20
	msgs := make([]core.Message, batch)
	for i := range msgs {
		msgs[i] = message(newID(), topic, fmt.Appendf(nil, `{"n":%d}`, i))
		msgs[i].Target.Key = fmt.Sprintf("key-%d", i%4)
	}

	results := p.Publish(t.Context(), msgs)
	if len(results) != batch {
		t.Fatalf("got %d results for %d messages", len(results), batch)
	}
	for i, err := range results {
		if err != nil {
			t.Errorf("message %d failed: %v", i, err)
		}
	}

	read := consumeKafka(t, topic, batch)
	if len(read) != batch {
		t.Fatalf("read %d messages back, want %d", len(read), batch)
	}

	// Every message carries its outbox id, so a consumer can deduplicate.
	seen := map[string]bool{}
	for _, m := range read {
		for _, h := range m.Headers {
			if h.Key == "message_id" {
				seen[string(h.Value)] = true
			}
		}
	}
	if len(seen) != batch {
		t.Errorf("%d distinct message_id headers, want %d", len(seen), batch)
	}
}

// A topic that does not exist, with auto-creation off, is permanent: retrying
// cannot conjure it.
func TestKafkaUnknownTopicIsPermanent(t *testing.T) {
	driver := kafkaDriver(t)

	p, err := kafka.New(t.Context(), driver, logging.Nop())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(context.WithoutCancel(t.Context())) })

	msg := message(newID(), uniqueName("kafka-absent"), []byte(`{}`))

	results := p.Publish(t.Context(), []core.Message{msg})
	if results[0] == nil {
		t.Fatal("publishing to a topic that does not exist must not report success")
	}
	if !core.IsPermanent(results[0]) {
		t.Errorf("error is %v; an unknown topic must be permanent", results[0])
	}
}

func TestKafkaHealthCheck(t *testing.T) {
	p, err := kafka.New(t.Context(), kafkaDriver(t), logging.Nop())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = p.Close(context.WithoutCancel(t.Context())) })

	if err := p.HealthCheck(t.Context()); err != nil {
		t.Errorf("a reachable cluster reports unhealthy: %v", err)
	}
}

// The router turns an unconfigured stream into a permanent failure at once
// rather than spending five retries and an hour of backoff on it.
func TestRouterRejectsAnUnknownStreamPermanently(t *testing.T) {
	cfg, err := config.LoadFrom(env(t,
		"OUTBOX_DB_USER=outbox",
		"OUTBOX_DB_NAME=outbox",
		"OUTBOX_STREAMS=local",
		"OUTBOX_STREAM_LOCAL_DRIVER=rmq",
		"OUTBOX_DRIVER_RMQ_TYPE=rabbitmq",
		"OUTBOX_DRIVER_RMQ_DSN="+amqpDSN(),
	))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	router, err := broker.NewRouter(cfg.Brokers, map[string]broker.Publisher{"rmq": stubPublisher{}})
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	results := router.Publish(t.Context(), "nowhere", []core.Message{message(newID(), "t", nil)})
	if len(results) != 1 || results[0] == nil {
		t.Fatalf("results = %v, want one failure", results)
	}
	if !core.IsPermanent(results[0]) || !errors.Is(results[0], core.ErrUnknownStream) {
		t.Errorf("error = %v, want a permanent ErrUnknownStream", results[0])
	}
}

type stubPublisher struct{}

func (stubPublisher) Publish(_ context.Context, msgs []core.Message) []error {
	return make([]error, len(msgs))
}
func (stubPublisher) HealthCheck(context.Context) error { return nil }
func (stubPublisher) Close(context.Context) error       { return nil }

// declareQueue creates a queue up front, for tests where the publisher must not
// create it itself.
func declareQueue(t *testing.T, queue string) {
	t.Helper()

	conn, err := amqp.Dial(amqpDSN())
	if err != nil {
		t.Fatalf("declare dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("declare channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare %s: %v", queue, err)
	}
}

// consumeOne reads a single message from a queue, declaring it if needed so the
// test does not depend on the publisher having declared it.
func consumeOne(t *testing.T, queue string) amqp.Delivery {
	t.Helper()

	conn, err := amqp.Dial(amqpDSN())
	if err != nil {
		t.Fatalf("consumer dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("consumer channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("declare %s: %v", queue, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		msg, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("get from %s: %v", queue, err)
		}
		if ok {
			return msg
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("no message arrived on %s within the deadline", queue)

	return amqp.Delivery{}
}

func consumeKafka(t *testing.T, topic string, want int) []segmentio.Message {
	t.Helper()

	reader := segmentio.NewReader(segmentio.ReaderConfig{
		Brokers:  kafkaBrokers(),
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  250 * time.Millisecond,
	})
	t.Cleanup(func() { _ = reader.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	out := make([]segmentio.Message, 0, want)
	for len(out) < want {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read from %s after %d messages: %v", topic, len(out), err)
		}
		out = append(out, m)
	}

	return out
}
