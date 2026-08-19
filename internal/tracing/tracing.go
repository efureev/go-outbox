// Package tracing turns the dispatcher into a visible stage of a producer's
// trace.
//
// A producer's span ends when its transaction commits. A consumer's span starts
// when the broker hands it a message. Between the two is a gap exactly the width
// of the outbox lag, and until something records it the only honest answer to
// "why did this arrive late?" is a shrug and a metric.
//
// The dispatcher already carried a producer's traceparent through to the broker
// untouched, so the two ends of that gap were connected but nothing described
// what happened inside it. What this adds is one span per published message,
// parented to the producer's context and re-injected into the message's headers
// so the consumer continues from it. The result reads producer → outbox.publish
// → consumer, in one trace, with the wait visible as the space in front of the
// middle span.
package tracing

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/efureev/go-outbox/internal/config"
	"github.com/efureev/go-outbox/internal/core"
)

// SpanName is what the dispatcher's own span is called. One name rather than
// one per destination: the destination is an attribute, and a span name that
// varies with it makes every aggregate view group by the wrong thing.
const SpanName = "outbox.publish"

// Tracer records the dispatcher's side of a message's journey.
//
// The zero value is usable and does nothing, which is the state a deployment
// with no collector configured stays in.
type Tracer struct {
	tracer trace.Tracer
	prop   propagation.TextMapPropagator
	// on is checked before any OpenTelemetry call rather than relying on a
	// no-op provider to be cheap. This runs per message at several thousand a
	// second, and cheap is not the same as free.
	on bool
}

// Disabled returns a tracer that records nothing.
func Disabled() *Tracer { return &Tracer{} }

// Enabled reports whether spans are being recorded.
func (t *Tracer) Enabled() bool { return t != nil && t.on }

// New builds a tracer and returns it with the function that flushes and closes
// the exporter.
//
// An empty endpoint is not an error: it is how tracing is turned off, and the
// tracer returned then costs nothing.
func New(ctx context.Context, cfg config.OTelConfig, svc Service) (
	*Tracer, func(context.Context) error, error,
) {
	noShutdown := func(context.Context) error { return nil }

	if cfg.Endpoint == "" {
		return Disabled(), noShutdown, nil
	}

	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("build the trace exporter: %w", err)
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		// Built without merging the default resource: that carries its own
		// schema URL, and merging two schema versions fails rather than
		// choosing between them.
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(svc.Name),
			semconv.ServiceVersion(svc.Version),
			semconv.ServiceInstanceID(svc.Instance),
		)),
		// Sampling defers to the parent when there is one, so a producer's
		// decision carries through: a trace sampled at the source does not lose
		// its middle here, and one that was not does not gain a lone span.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.Sampling))),
	)

	return &Tracer{
		tracer: provider.Tracer("github.com/efureev/go-outbox"),
		prop:   propagation.TraceContext{},
		on:     true,
	}, provider.Shutdown, nil
}

// NewWithProvider builds a tracer on a provider the caller owns.
//
// It exists so a test can read the spans back without standing up a collector,
// and so a program embedding this dispatcher can put its traces on the provider
// it already configured rather than on a second exporter pointed at the same
// place.
func NewWithProvider(tp trace.TracerProvider) *Tracer {
	return &Tracer{
		tracer: tp.Tracer("github.com/efureev/go-outbox"),
		prop:   propagation.TraceContext{},
		on:     true,
	}
}

// Service identifies this process to the collector.
type Service struct {
	Name     string
	Version  string
	Instance string
}

// Destination describes where a batch is going, for the span attributes.
type Destination struct {
	Stream string
	Driver string
	// System is the driver's type — "rabbitmq" or "kafka" — which is the
	// attribute generic tracing tools key their messaging views on.
	System string
}

// Batch holds the spans covering one publish call.
type Batch struct{ spans []trace.Span }

// Publish opens one span per message and rewrites each message's traceparent so
// the consumer's span continues from this one rather than from a producer span
// that ended minutes ago.
//
// The headers are mutated in place, and that is the point rather than a side
// effect: the header is what travels, so a span the consumer cannot find would
// record the gap without closing it. The trace id is unchanged, so everything
// stays in the producer's trace; only the parent moves.
//
// Every message in a batch gets its own span even where the driver writes the
// batch in one round trip, and they then share a duration. That matches how the
// same batch is already reported elsewhere: a per-message figure would be a
// fiction for a driver that made one call.
func (t *Tracer) Publish(ctx context.Context, dest Destination, msgs []core.Message) Batch {
	if !t.Enabled() || len(msgs) == 0 {
		return Batch{}
	}

	spans := make([]trace.Span, len(msgs))

	for i := range msgs {
		msg := &msgs[i]
		if msg.Headers == nil {
			msg.Headers = make(map[string]string, 1)
		}

		carrier := propagation.MapCarrier(msg.Headers)

		// The producer's context as it was at commit. Absent for a producer
		// that never traced, in which case this starts a trace of its own.
		parent := t.prop.Extract(ctx, carrier)

		_, span := t.tracer.Start(parent, SpanName,
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				semconv.MessagingSystemKey.String(dest.System),
				semconv.MessagingDestinationName(msg.Topic),
				semconv.MessagingMessageID(msg.ID),
				attribute.String("outbox.stream", dest.Stream),
				attribute.String("outbox.driver", dest.Driver),
				attribute.Int("outbox.attempts", msg.Attempts),
				// The span covers the publish alone, so without this the wait
				// before it is visible as empty space and nothing else. It is
				// measured against the database clock's created_at, which is
				// the same basis as the delivery-lag metric.
				attribute.Float64("outbox.wait_seconds", time.Since(msg.CreatedAt).Seconds()),
			),
		)

		t.prop.Inject(trace.ContextWithSpan(ctx, span), carrier)
		spans[i] = span
	}

	return Batch{spans: spans}
}

// End closes the batch, recording each message's outcome. errs is positional,
// as everywhere else a batch reports per-message results.
func (b Batch) End(errs []error) {
	for i, span := range b.spans {
		if i < len(errs) && errs[i] != nil {
			span.RecordError(errs[i])
			span.SetStatus(codes.Error, errs[i].Error())
		}
		span.End()
	}
}
