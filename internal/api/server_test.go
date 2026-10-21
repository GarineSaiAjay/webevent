package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GarineSaiAjay/webevent/internal/api"
	"github.com/GarineSaiAjay/webevent/internal/logging"
	"github.com/GarineSaiAjay/webevent/internal/observability"
	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

func TestHealthz(t *testing.T) {
	h := api.New(api.Options{Logger: logging.Discard(), Version: "test"})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" || body["version"] != "test" {
		t.Fatalf("unexpected body %v", body)
	}
	if rec.Header().Get("X-Request-Id") == "" {
		t.Fatal("missing X-Request-Id header")
	}
}

func TestRequestIDIsPropagatedWhenSafe(t *testing.T) {
	h := api.New(api.Options{Logger: logging.Discard()})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-Id", "abc-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-Id"); got != "abc-123" {
		t.Fatalf("X-Request-Id = %q, want abc-123", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-Id", "bad id\nwith newline")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-Id"); got == "bad id\nwith newline" || got == "" {
		t.Fatalf("unsafe request id should be replaced, got %q", got)
	}
}

func TestUnknownRouteReturnsJSON404(t *testing.T) {
	h := api.New(api.Options{Logger: logging.Discard()})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}
	var body struct {
		Error struct{ Code string } `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "not_found" {
		t.Fatalf("code = %q", body.Error.Code)
	}
}

func TestReadyzWithDatabase(t *testing.T) {
	pool := testutil.NewDB(t)
	h := api.New(api.Options{Logger: logging.Discard(), Store: store.New(pool)})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body)
	}

	pool.Close()
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("after closing pool status = %d, want 503", rec.Code)
	}
}

func TestMetricsUseRoutePatterns(t *testing.T) {
	m := observability.NewMetrics()
	st := store.New(testutil.NewDB(t))
	h := api.New(api.Options{Logger: logging.Discard(), Store: st, APIKeys: []string{testAPIKey}, Metrics: m})

	req := httptest.NewRequest(http.MethodGet, "/v1/endpoints/ep_00000000000000000000000000", nil)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	h.ServeHTTP(httptest.NewRecorder(), req)

	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range families {
		if f.GetName() != "webevent_http_requests_total" {
			continue
		}
		for _, metric := range f.GetMetric() {
			for _, l := range metric.GetLabel() {
				if l.GetName() == "route" && l.GetValue() == "GET /v1/endpoints/{id}" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("expected a sample labelled with the route pattern, not the raw path")
	}
}
