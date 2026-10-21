package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/api"
	"github.com/GarineSaiAjay/webevent/internal/cli"
	"github.com/GarineSaiAjay/webevent/internal/logging"
	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

const key = "cli-test-key-0123456789abcdefghijklmnop"

type harness struct {
	t      *testing.T
	server string
	store  *store.Store
	cfg    string
	env    map[string]string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	st := store.New(testutil.NewDB(t))
	srv := httptest.NewServer(api.New(api.Options{Logger: logging.Discard(), Store: st, APIKeys: []string{key}, Version: "test"}))
	t.Cleanup(srv.Close)
	cfg := filepath.Join(t.TempDir(), "config.json")
	return &harness{t: t, server: srv.URL, store: st, cfg: cfg, env: map[string]string{
		"WEBEVENT_SERVER": srv.URL, "WEBEVENT_API_KEY": key, "WEBEVENT_CONFIG": cfg,
	}}
}

// run executes the CLI in-process and returns stdout, stderr and the exit code.
func (h *harness) run(stdin string, args ...string) (string, string, int) {
	h.t.Helper()
	var out, errOut bytes.Buffer
	env := cli.Env{
		Stdin:  strings.NewReader(stdin),
		Stdout: &out,
		Stderr: &errOut,
		Getenv: func(k string) string { return h.env[k] },
	}
	code := cli.Execute(context.Background(), env, "test", args)
	return out.String(), errOut.String(), code
}

func (h *harness) mustRun(args ...string) string {
	h.t.Helper()
	out, errOut, code := h.run("", args...)
	if code != 0 {
		h.t.Fatalf("webevent %s: exit %d\nstderr: %s", strings.Join(args, " "), code, errOut)
	}
	return out
}

func TestEndToEndWorkflow(t *testing.T) {
	h := newHarness(t)

	out := h.mustRun("endpoints", "create", "--url", "https://example.com/hooks", "--events", "user.created,user.deleted",
		"--description", "crm", "--rate-limit", "5", "-o", "json")
	var ep struct {
		ID              string   `json:"id"`
		EventTypes      []string `json:"event_types"`
		RateLimitPerSec *int     `json:"rate_limit_per_sec"`
		Secret          string   `json:"secret"`
	}
	if err := json.Unmarshal([]byte(out), &ep); err != nil {
		t.Fatalf("bad json %q: %v", out, err)
	}
	if !strings.HasPrefix(ep.Secret, "whsec_") || len(ep.EventTypes) != 2 || ep.RateLimitPerSec == nil || *ep.RateLimitPerSec != 5 {
		t.Fatalf("created endpoint = %+v", ep)
	}

	if out := h.mustRun("endpoints", "list"); !strings.Contains(out, ep.ID) || !strings.Contains(out, "STATUS") {
		t.Fatalf("list output:\n%s", out)
	}
	if out := h.mustRun("endpoints", "secret", ep.ID); strings.TrimSpace(out) != ep.Secret {
		t.Fatalf("secret output %q", out)
	}
	if out := h.mustRun("endpoints", "pause", ep.ID); !strings.Contains(out, "paused") {
		t.Fatalf("pause output %q", out)
	}
	h.mustRun("endpoints", "resume", ep.ID)
	if out := h.mustRun("endpoints", "update", ep.ID, "--description", "billing"); !strings.Contains(out, "billing") {
		t.Fatalf("update output %q", out)
	}

	// Trigger with inline data, then from stdin with an idempotency key.
	out = h.mustRun("events", "trigger", "user.created", "--data", `{"id": 1}`)
	if !strings.Contains(out, "1 delivery queued") {
		t.Fatalf("trigger output %q", out)
	}
	out, errOut, code := h.run(`{"id": 2}`, "events", "trigger", "user.created", "--data", "-", "--idempotency-key", "k-2")
	if code != 0 || !strings.Contains(out, "created") {
		t.Fatalf("stdin trigger: %q %q", out, errOut)
	}
	out, _, _ = h.run(`{"id": 2}`, "events", "trigger", "user.created", "--data", "-", "--idempotency-key", "k-2")
	if !strings.Contains(out, "idempotent replay") {
		t.Fatalf("second trigger should be a replay: %q", out)
	}

	if out := h.mustRun("events", "list", "--type", "user.created"); strings.Count(out, "evt_") != 2 {
		t.Fatalf("events list:\n%s", out)
	}
	if out := h.mustRun("deliveries", "list", "--endpoint", ep.ID, "--status", "pending"); strings.Count(out, "dlv_") != 2 {
		t.Fatalf("deliveries list:\n%s", out)
	}
}

func TestLogsShowsAttempts(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	ep, _ := h.store.CreateEndpoint(ctx, store.NewEndpoint{URL: "https://example.com/h", EventTypes: []string{"*"}})
	if _, err := h.store.CreateEvent(ctx, store.NewEvent{EventType: "order.paid", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	jobs, _ := h.store.ClaimJobs(ctx, "w", 1, time.Minute)
	if _, err := h.store.RecordAttempt(ctx, store.AttemptRecord{
		Job: jobs[0], Result: store.AttemptResult{StatusCode: 503, Error: "receiver responded 503", Duration: 12 * time.Millisecond},
		Outcome: store.OutcomeRetry, RetryIn: time.Minute,
	}); err != nil {
		t.Fatal(err)
	}

	out := h.mustRun("logs", "--endpoint", ep.ID)
	for _, want := range []string{"order.paid", "503", "12ms", "receiver responded 503"} {
		if !strings.Contains(out, want) {
			t.Errorf("logs output missing %q:\n%s", want, out)
		}
	}
	out = h.mustRun("deliveries", "get", jobs[0].DeliveryID)
	if !strings.Contains(out, "Attempts:") || !strings.Contains(out, "503") {
		t.Fatalf("delivery detail:\n%s", out)
	}
}

func TestErrorsExitNonZero(t *testing.T) {
	h := newHarness(t)
	_, errOut, code := h.run("", "endpoints", "get", "ep_00000000000000000000000000")
	if code != 1 || !strings.Contains(errOut, "endpoint not found") || !strings.Contains(errOut, "Request ID") {
		t.Fatalf("code=%d stderr=%q", code, errOut)
	}
	_, errOut, code = h.run("", "endpoints", "create", "--url", "ftp://nope")
	if code != 1 || !strings.Contains(errOut, "url:") {
		t.Fatalf("validation errors should list fields: %q", errOut)
	}
	_, errOut, code = h.run("", "events", "trigger", "a.b", "--data", "{not json")
	if code != 1 || !strings.Contains(errOut, "not valid JSON") {
		t.Fatalf("bad payload: %q", errOut)
	}
	_, _, code = h.run("", "endpoints", "list", "-o", "yaml")
	if code != 1 {
		t.Fatal("unknown output format must fail")
	}
}

func TestConfigPrecedence(t *testing.T) {
	h := newHarness(t)
	// Without env, the config file supplies server and key.
	delete(h.env, "WEBEVENT_API_KEY")
	delete(h.env, "WEBEVENT_SERVER")
	_, errOut, code := h.run("", "endpoints", "list")
	if code != 1 || !strings.Contains(errOut, "no API key") {
		t.Fatalf("expected missing key error, got %q", errOut)
	}
	h.mustRun("config", "set", "server", h.server)
	h.mustRun("config", "set", "api-key", key)
	info, err := os.Stat(h.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config file mode = %v, want 0600", info.Mode().Perm())
	}
	h.mustRun("endpoints", "list")

	out := h.mustRun("config", "show")
	if strings.Contains(out, key) || !strings.Contains(out, "cli-") {
		t.Fatalf("config show must mask the key:\n%s", out)
	}

	// A flag beats the config file.
	_, errOut, code = h.run("", "endpoints", "list", "--api-key", "wrong-key-0123456789abcdef0123456789")
	if code != 1 || !strings.Contains(errOut, "unauthorized") && !strings.Contains(errOut, "invalid API key") {
		t.Fatalf("flag should override config: %q", errOut)
	}
}

func TestHealth(t *testing.T) {
	h := newHarness(t)
	if out := h.mustRun("health"); !strings.Contains(out, "server version test") {
		t.Fatalf("health output %q", out)
	}
}
