package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GarineSaiAjay/webevent/internal/api"
	"github.com/GarineSaiAjay/webevent/internal/logging"
	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

const testAPIKey = "test-api-key-0123456789abcdefghijklmnop"

type testAPI struct {
	t     *testing.T
	h     http.Handler
	store *store.Store
}

func newTestAPI(t *testing.T) *testAPI {
	t.Helper()
	st := store.New(testutil.NewDB(t))
	h := api.New(api.Options{Logger: logging.Discard(), Store: st, APIKeys: []string{testAPIKey}, Version: "test"})
	return &testAPI{t: t, h: h, store: st}
}

// do sends an authenticated request. body may be nil, a string (sent raw) or
// any value (JSON-encoded).
func (a *testAPI) do(method, path string, body any, headers ...string) *httptest.ResponseRecorder {
	a.t.Helper()
	var rdr io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rdr = bytes.NewBufferString(b)
	default:
		buf, err := json.Marshal(b)
		if err != nil {
			a.t.Fatal(err)
		}
		rdr = bytes.NewReader(buf)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	a.h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %T from %q: %v", v, rec.Body.String(), err)
	}
	return v
}

type errorBody struct {
	Error struct {
		Code    string            `json:"code"`
		Message string            `json:"message"`
		Fields  map[string]string `json:"fields"`
	} `json:"error"`
}

func expectStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d; body = %s", rec.Code, want, rec.Body.String())
	}
}
