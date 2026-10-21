package delivery_test

import (
	"context"
	"encoding/json"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

func TestDeliverySpanLinksToIngestTrace(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })

	st := newStore(t)
	p := newPool(t, st, poolConfig{})
	ctx := context.Background()
	rcv, srv := newReceiver(t)
	if _, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: srv.URL, EventTypes: []string{"*"}}); err != nil {
		t.Fatal(err)
	}
	const ingest = "00-11111111111111111111111111111111-2222222222222222-01"
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: json.RawMessage(`{}`), TraceParent: ingest}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}

	spans := rec.Ended()
	if len(spans) != 1 || spans[0].Name() != "webhook.deliver" || spans[0].SpanKind() != trace.SpanKindClient {
		t.Fatalf("spans = %v", spans)
	}
	links := spans[0].Links()
	if len(links) != 1 || links[0].SpanContext.TraceID().String() != "11111111111111111111111111111111" {
		t.Fatalf("delivery span must link to the ingest trace, links = %+v", links)
	}

	// The receiver gets a traceparent for the delivery span.
	got := rcv.requests[0].Header.Get("Traceparent")
	want := spans[0].SpanContext().TraceID().String()
	if got == "" || got[3:35] != want {
		t.Fatalf("traceparent header = %q, want trace %s", got, want)
	}
}
