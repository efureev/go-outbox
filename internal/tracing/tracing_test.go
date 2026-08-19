package tracing

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/efureev/go-outbox/internal/core"
)

// recording builds a tracer that keeps its spans in memory, and the exporter to
// read them back from.
func recording(t *testing.T) (*Tracer, *tracetest.InMemoryExporter) {
	t.Helper()

	exp := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	return &Tracer{
		tracer: provider.Tracer("test"),
		prop:   propagation.TraceContext{},
		on:     true,
	}, exp
}

func message(id string, headers map[string]string) core.Message {
	return core.Message{
		ID:        id,
		Stream:    "local",
		Topic:     "order.created",
		Headers:   headers,
		CreatedAt: time.Now().Add(-90 * time.Second),
	}
}

var here = Destination{Stream: "local", Driver: "rmq", System: "rabbitmq"}

// The whole point: the dispatcher's span belongs to the producer's trace, not
// to one of its own. A span in a separate trace records the wait without
// closing the gap it was added to close.
func TestPublishJoinsTheProducersTrace(t *testing.T) {
	tr, exp := recording(t)

	const producer = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	msgs := []core.Message{message("a", map[string]string{"traceparent": producer})}

	tr.Publish(context.Background(), here, msgs).End([]error{nil})

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}

	got := spans[0]
	if got.Name != SpanName {
		t.Errorf("span name = %q, want %q", got.Name, SpanName)
	}
	if want := "4bf92f3577b34da6a3ce929d0e0e4736"; got.SpanContext.TraceID().String() != want {
		t.Errorf("trace id = %s, want the producer's %s", got.SpanContext.TraceID(), want)
	}
	if want := "00f067aa0ba902b7"; got.Parent.SpanID().String() != want {
		t.Errorf("parent = %s, want the producer's span %s", got.Parent.SpanID(), want)
	}
	if got.SpanKind != trace.SpanKindProducer {
		t.Errorf("span kind = %v, want producer", got.SpanKind)
	}
}

// The header is what travels. A span the consumer cannot find from its own end
// would record the gap without closing it, so publishing rewrites traceparent
// to point at this span rather than at a producer span that ended minutes ago.
func TestPublishRewritesTheHeaderForTheConsumer(t *testing.T) {
	tr, exp := recording(t)

	const producer = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	msgs := []core.Message{message("a", map[string]string{"traceparent": producer})}

	tr.Publish(context.Background(), here, msgs).End([]error{nil})

	after := msgs[0].Headers["traceparent"]
	if after == producer {
		t.Fatal("the header still points at the producer, so the consumer would skip over this span")
	}

	// Same trace, new parent: the consumer continues the producer's trace
	// through this span rather than starting somewhere else.
	span := exp.GetSpans()[0]
	if want := "00-" + span.SpanContext.TraceID().String() + "-" +
		span.SpanContext.SpanID().String() + "-01"; after != want {
		t.Errorf("traceparent = %q, want %q", after, want)
	}
}

// A producer that never traced still gets a span, and a header that lets its
// consumer start one. Requiring the producer to have traced first would make
// this useful only where it was needed least.
func TestPublishStartsATraceWhenTheProducerDidNot(t *testing.T) {
	tr, exp := recording(t)

	msgs := []core.Message{message("a", nil)}

	tr.Publish(context.Background(), here, msgs).End([]error{nil})

	if len(exp.GetSpans()) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(exp.GetSpans()))
	}
	if msgs[0].Headers == nil {
		t.Fatal("a message with no headers came back with none, so nothing propagates")
	}
	if msgs[0].Headers["traceparent"] == "" {
		t.Error("no traceparent was written")
	}
}

func TestPublishRecordsFailures(t *testing.T) {
	tr, exp := recording(t)

	msgs := []core.Message{message("a", nil), message("b", nil)}

	tr.Publish(context.Background(), here, msgs).End([]error{errors.New("broker nacked"), nil})

	spans := exp.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("recorded %d spans, want 2", len(spans))
	}

	var failed, ok int
	for _, s := range spans {
		switch s.Status.Code {
		case codes.Error:
			failed++
			if len(s.Events) == 0 {
				t.Error("a failed span records no exception event")
			}
		default:
			ok++
		}
	}
	if failed != 1 || ok != 1 {
		t.Errorf("failed = %d, ok = %d; want one of each", failed, ok)
	}
}

func TestPublishDescribesTheDestination(t *testing.T) {
	tr, exp := recording(t)

	tr.Publish(context.Background(), here, []core.Message{message("a", nil)}).End([]error{nil})

	attrs := map[string]string{}
	for _, a := range exp.GetSpans()[0].Attributes {
		attrs[string(a.Key)] = a.Value.String()
	}

	for key, want := range map[string]string{
		"messaging.system":           "rabbitmq",
		"messaging.destination.name": "order.created",
		"messaging.message.id":       "a",
		"outbox.stream":              "local",
		"outbox.driver":              "rmq",
	} {
		if attrs[key] != want {
			t.Errorf("%s = %q, want %q", key, attrs[key], want)
		}
	}

	// The span covers the publish alone, so the wait before it is otherwise
	// visible only as empty space.
	if attrs["outbox.wait_seconds"] == "" {
		t.Error("the wait between commit and publish was not recorded")
	}
}

// Tracing is off unless a collector is configured, and off has to mean free:
// this runs per message, several thousand a second. A no-op provider would
// still be a call, an interface dispatch and a context.
func TestDisabledTracerCostsNothing(t *testing.T) {
	tr := Disabled()
	msgs := []core.Message{message("a", nil)}

	allocs := testing.AllocsPerRun(100, func() {
		tr.Publish(context.Background(), here, msgs).End([]error{nil})
	})

	if allocs != 0 {
		t.Errorf("a disabled tracer allocated %v times per publish", allocs)
	}
	if msgs[0].Headers != nil {
		t.Errorf("a disabled tracer wrote headers: %v", msgs[0].Headers)
	}
}

// A nil tracer is what a pipeline built without the option would hold if the
// zero value were ever skipped; it must not be the thing that takes the process
// down.
func TestNilTracerIsSafe(t *testing.T) {
	var tr *Tracer

	tr.Publish(context.Background(), here, []core.Message{message("a", nil)}).End(nil)
}

func BenchmarkPublishDisabled(b *testing.B) {
	tr := Disabled()
	msgs := []core.Message{message("a", nil)}
	errs := []error{nil}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		tr.Publish(context.Background(), here, msgs).End(errs)
	}
}

func BenchmarkPublishRecording(b *testing.B) {
	exp := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	tr := &Tracer{tracer: provider.Tracer("bench"), prop: propagation.TraceContext{}, on: true}
	errs := []error{nil}

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		msgs := []core.Message{message("a", nil)}
		tr.Publish(context.Background(), here, msgs).End(errs)
		exp.Reset()
	}
}
