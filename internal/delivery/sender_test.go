package delivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

func job(url string) store.Job {
	return store.Job{
		DeliveryID: "dlv_x", AttemptNumber: 1, EventID: "evt_x", EventType: "a.b",
		Payload: json.RawMessage(`{"k":"v"}`), EventCreatedAt: time.Unix(0, 0), EndpointID: "ep_x", URL: url,
	}
}

func TestSendDoesNotFollowRedirects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://169.254.169.254/latest/meta-data", http.StatusFound)
	}))
	defer srv.Close()

	res := NewSender(time.Second, "t").Send(context.Background(), job(srv.URL))
	if res.StatusCode != http.StatusFound || res.Error == "" {
		t.Fatalf("redirect must be reported as a failure, got %+v", res)
	}
}

func TestSendTimesOut(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	res := NewSender(200*time.Millisecond, "t").Send(context.Background(), job(srv.URL))
	if res.Error == "" || res.StatusCode != 0 {
		t.Fatalf("expected timeout error, got %+v", res)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("timeout was not enforced")
	}
}

func TestSendCapsResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("é", 10_000))) // 20 KB of 2-byte runes
	}))
	defer srv.Close()

	res := NewSender(time.Second, "t").Send(context.Background(), job(srv.URL))
	if len(res.ResponseBody) > maxStoredResponse+3 {
		t.Fatalf("stored body is %d bytes", len(res.ResponseBody))
	}
	if !utf8.ValidString(res.ResponseBody) {
		t.Fatal("stored body must be valid UTF-8")
	}
}

func TestSanitizeBody(t *testing.T) {
	got := sanitizeBody([]byte("a\x00b\xffc"), 100)
	if got != "ab�c" {
		t.Fatalf("got %q", got)
	}
}

func TestBuildBody(t *testing.T) {
	b, err := BuildBody(job("http://x"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"evt_x","type":"a.b","created_at":"1970-01-01T00:00:00Z","data":{"k":"v"}}`
	if string(b) != want {
		t.Fatalf("body = %s\nwant  %s", b, want)
	}
}

func TestSuccessful(t *testing.T) {
	for code, want := range map[int]bool{199: false, 200: true, 204: true, 299: true, 300: false, 404: false, 500: false} {
		if Successful(code) != want {
			t.Errorf("Successful(%d) = %v", code, !want)
		}
	}
}
