package observability

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation scope for every span we create.
const TracerName = "github.com/GarineSaiAjay/webevent"

// Tracer returns the service tracer (a no-op until SetupTracing installs a
// real provider).
func Tracer() trace.Tracer { return otel.Tracer(TracerName) }

// SetupTracing installs a global OpenTelemetry tracer provider that exports
// spans over OTLP/HTTP. It is configured entirely by the standard
// environment variables, so it works with any OTLP backend (Jaeger, Tempo,
// Honeycomb, Datadog, ...):
//
//	OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
//	OTEL_TRACES_SAMPLER=parentbased_traceidratio
//	OTEL_TRACES_SAMPLER_ARG=0.1
//
// If OTEL_EXPORTER_OTLP_ENDPOINT (or the traces-specific variant) is unset,
// tracing stays disabled and spans cost almost nothing. The returned
// function flushes buffered spans; call it during shutdown.
func SetupTracing(ctx context.Context, service, version string) (func(context.Context) error, error) {
	// Always install the W3C propagator so incoming traceparent headers are
	// honoured and outgoing webhooks carry ours.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	res, err := sdkresource.Merge(sdkresource.Default(), sdkresource.NewSchemaless(
		attribute.String("service.name", service),
		attribute.String("service.version", version),
	))
	if err != nil {
		return nil, err
	}
	// The SDK reads OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG itself.
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// TraceParentFrom serialises the span context in ctx as a W3C traceparent
// header value ("" if ctx carries no valid span).
func TraceParentFrom(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier["traceparent"]
}

// SpanContextFromTraceParent parses a stored traceparent, for linking the
// delivery span to the request that created the event.
func SpanContextFromTraceParent(tp string) trace.SpanContext {
	if tp == "" {
		return trace.SpanContext{}
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": tp})
	return trace.SpanContextFromContext(ctx)
}
