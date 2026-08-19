//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/efureev/go-outbox/internal/dispatch"
	"github.com/efureev/go-outbox/internal/events"
	"github.com/efureev/go-outbox/internal/logging"
	"github.com/efureev/go-outbox/internal/tracing"
)

// The end the unit tests cannot reach: what the broker is actually handed.
//
// Rewriting a header in memory proves nothing on its own — the value has to
// survive being turned into an AMQP property and read back by a consumer, which
// is the only place the chain producer → outbox.publish → consumer is either
// whole or broken.
func TestTheBrokerReceivesTheDispatchersTraceparent(t *testing.T) {
	h := newHarness(t)

	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	cfg := mustConfig(t, h.fixture)
	pipeline := dispatch.New("local", h.Store, h.Router,
		events.NewEmitter(nil, logging.Nop()), cfg, logging.Nop(),
		dispatch.WithTracer(tracing.NewWithProvider(provider)))

	const (
		topic    = "traced.orders"
		trace    = "4bf92f3577b34da6a3ce929d0e0e4736"
		producer = "00-" + trace + "-00f067aa0ba902b7-01"
	)

	h.insertWithHeaders(t, "local", topic, []byte(`{"n":1}`),
		map[string]string{"traceparent": producer})

	if _, err := pipeline.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	span := spans[0]

	if got := span.SpanContext.TraceID().String(); got != trace {
		t.Errorf("the dispatcher's span is in trace %s, not the producer's %s", got, trace)
	}

	got := consumeOne(t, h.queueFor(topic))
	delivered, _ := got.Headers["traceparent"].(string)

	if delivered == producer {
		t.Fatal("the broker received the producer's traceparent unchanged, " +
			"so a consumer would attach to a span that ended before the message was sent")
	}
	if !strings.Contains(delivered, trace) {
		t.Errorf("traceparent = %q, want it to stay inside trace %s", delivered, trace)
	}
	if !strings.Contains(delivered, span.SpanContext.SpanID().String()) {
		t.Errorf("traceparent = %q, want it to name the dispatcher's span %s",
			delivered, span.SpanContext.SpanID())
	}
}

// With no collector configured the dispatcher must leave the header exactly as
// the producer wrote it. Anything else would make turning tracing on a change
// of behaviour for consumers rather than an addition to it.
func TestWithoutATracerTheHeaderIsUntouched(t *testing.T) {
	h := newHarness(t)

	const (
		topic    = "untraced.orders"
		producer = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	)

	h.insertWithHeaders(t, "local", topic, []byte(`{"n":1}`),
		map[string]string{"traceparent": producer})

	if _, err := h.Pipeline.RunOnce(t.Context()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	got := consumeOne(t, h.queueFor(topic))
	if delivered, _ := got.Headers["traceparent"].(string); delivered != producer {
		t.Errorf("traceparent = %q, want the producer's %q untouched", delivered, producer)
	}
}
