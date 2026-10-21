package observability_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/GarineSaiAjay/webevent/internal/logging"
	"github.com/GarineSaiAjay/webevent/internal/observability"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

func scrape(t *testing.T, m *observability.Metrics) string {
	t.Helper()
	srv := httptest.NewServer(observability.AdminHandler(m))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func TestMetricsExposition(t *testing.T) {
	m := observability.NewMetrics()
	m.ObserveHTTP("POST", "POST /v1/events", 202, 3*time.Millisecond)
	m.ObserveHTTP("GET", "", 404, time.Millisecond)
	m.EventIngested(3)
	m.Attempt("succeeded", 120*time.Millisecond)
	m.Attempt("retry", time.Second)
	m.InFlight(2)
	m.Claimed(5, 2*time.Millisecond)
	m.Reaped(1)
	m.EndpointDisabled()

	out := scrape(t, m)
	for _, want := range []string{
		`webevent_http_requests_total{code="202",method="POST",route="POST /v1/events"} 1`,
		`webevent_http_requests_total{code="404",method="GET",route="unmatched"} 1`,
		`webevent_events_ingested_total 1`,
		`webevent_deliveries_enqueued_total 3`,
		`webevent_delivery_attempts_total{outcome="succeeded"} 1`,
		`webevent_deliveries_in_flight 2`,
		`webevent_deliveries_claimed_total 5`,
		`webevent_leases_reaped_total 1`,
		`webevent_endpoints_disabled_total 1`,
		`go_goroutines`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

func TestNilMetricsAreSafe(t *testing.T) {
	var m *observability.Metrics
	m.ObserveHTTP("GET", "/", 200, 0)
	m.EventIngested(1)
	m.Attempt("dead", 0)
	m.InFlight(1)
	m.Claimed(1, 0)
	m.Reaped(1)
	m.EndpointDisabled()
	m.RegisterQueueCollector(nil, 0, nil)
}

func TestQueueCollector(t *testing.T) {
	pool := testutil.NewDB(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO endpoints (id, url, event_types, secret) VALUES ('ep_1', 'https://x', '{*}', 'whsec_x');
		INSERT INTO events (id, event_type, payload, request_hash) VALUES ('evt_1', 'a.b', '{}', '\x00');
		INSERT INTO deliveries (id, event_id, endpoint_id, next_attempt_at) VALUES ('dlv_1', 'evt_1', 'ep_1', now() - interval '90 seconds');
	`); err != nil {
		t.Fatal(err)
	}
	m := observability.NewMetrics()
	m.RegisterQueueCollector(pool, time.Second, logging.Discard())
	out := scrape(t, m)
	if !strings.Contains(out, `webevent_queue_depth{status="pending"} 1`) {
		t.Errorf("missing pending depth:\n%s", grep(out, "webevent_queue"))
	}
	if !strings.Contains(out, "webevent_queue_oldest_due_seconds 9") && !strings.Contains(out, "webevent_queue_oldest_due_seconds 8") {
		t.Errorf("oldest due should be ~90s:\n%s", grep(out, "webevent_queue"))
	}
}

func grep(s, substr string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, substr) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

func TestTraceParentRoundTrip(t *testing.T) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	if _, err := observability.SetupTracing(ctx, "test", "dev"); err != nil {
		t.Fatal(err)
	}
	tp := observability.TraceParentFrom(ctx)
	if tp != "00-0102030405060708090a0b0c0d0e0f10-0102030405060708-01" {
		t.Fatalf("traceparent = %q", tp)
	}
	back := observability.SpanContextFromTraceParent(tp)
	if back.TraceID() != sc.TraceID() || back.SpanID() != sc.SpanID() {
		t.Fatal("round trip lost the span context")
	}
	if observability.TraceParentFrom(context.Background()) != "" {
		t.Fatal("no span must give empty traceparent")
	}
	if observability.SpanContextFromTraceParent("garbage").IsValid() {
		t.Fatal("garbage must not parse")
	}
}

func TestPprofIsServed(t *testing.T) {
	srv := httptest.NewServer(observability.AdminHandler(observability.NewMetrics()))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pprof status %d", resp.StatusCode)
	}
}
