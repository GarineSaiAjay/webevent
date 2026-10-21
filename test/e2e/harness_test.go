//go:build e2e

// Package e2e runs the real webevent-server binary as a subprocess against
// a real Postgres and real HTTP receivers. These tests exercise what unit
// tests cannot: configuration, migrations on boot, signals, process crashes
// and restarts.
//
//	make test-e2e
package e2e

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/testutil"
	"github.com/GarineSaiAjay/webevent/pkg/client"
	"github.com/GarineSaiAjay/webevent/pkg/webhook"
)

const apiKey = "e2e-test-key-0123456789abcdefghijklmnop"

var serverBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "webevent-e2e")
	if err != nil {
		panic(err)
	}
	serverBin = filepath.Join(dir, "webevent-server")
	build := exec.Command("go", "build", "-race", "-o", serverBin, "../../cmd/webevent-server")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		panic("build server: " + err.Error())
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// server is one running webevent-server process.
type server struct {
	t    *testing.T
	cmd  *exec.Cmd
	addr string
	logs *lockedBuffer
	done chan error
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startServer launches the binary with sensible test settings; extra env
// entries ("KEY=value") override them.
func startServer(t *testing.T, dbURL string, extra ...string) *server {
	t.Helper()
	addr := freePort(t)
	cmd := exec.Command(serverBin)
	cmd.Env = append(os.Environ(),
		"WEBEVENT_DATABASE_URL="+dbURL,
		"WEBEVENT_HTTP_ADDR="+addr,
		"WEBEVENT_ADMIN_ADDR="+freePort(t),
		"WEBEVENT_API_KEYS="+apiKey,
		"WEBEVENT_ALLOW_PRIVATE_NETWORKS=true",
		"WEBEVENT_LOG_FORMAT=json",
		"WEBEVENT_WORKER_POLL_INTERVAL=100ms",
		"WEBEVENT_RETRY_BASE_DELAY=200ms",
		"WEBEVENT_RETRY_MAX_DELAY=1s",
		"WEBEVENT_DELIVERY_TIMEOUT=2s",
		"WEBEVENT_LEASE_DURATION=17s",
		"WEBEVENT_REAP_INTERVAL=1s",
		"WEBEVENT_SHUTDOWN_TIMEOUT=15s",
	)
	cmd.Env = append(cmd.Env, extra...)
	logs := &lockedBuffer{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := &server{t: t, cmd: cmd, addr: addr, logs: logs, done: make(chan error, 1)}
	go func() { s.done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-s.done
		if t.Failed() {
			t.Logf("server %s logs:\n%s", addr, logs.String())
		}
	})

	// Wait until it is ready (migrations done, DB reachable).
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return s
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("server did not become ready; logs:\n%s", logs.String())
	return nil
}

func (s *server) client() *client.Client { return client.New("http://"+s.addr, apiKey) }

// stop sends sig and waits for the process to exit.
func (s *server) stop(sig syscall.Signal) error {
	_ = s.cmd.Process.Signal(sig)
	select {
	case err := <-s.done:
		s.done <- err // let Cleanup's receive succeed
		return err
	case <-time.After(30 * time.Second):
		s.t.Fatal("server did not exit")
		return nil
	}
}

// receiver verifies signatures and counts deliveries per webhook-id.
type receiver struct {
	mu       sync.Mutex
	verifier *webhook.Verifier
	counts   map[string]int
	bad      int
	handler  func(w http.ResponseWriter, r *http.Request) bool // return true if handled
	srv      *httptest.Server
}

func newReceiver(t *testing.T) *receiver {
	t.Helper()
	r := &receiver{counts: map[string]int{}}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		v, h := r.verifier, r.handler
		r.mu.Unlock()
		if v != nil {
			if err := v.Verify(req.Header, body); err != nil {
				r.mu.Lock()
				r.bad++
				r.mu.Unlock()
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		if h != nil && h(w, req) {
			return
		}
		r.mu.Lock()
		r.counts[req.Header.Get(webhook.HeaderID)]++
		r.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(r.srv.Close)
	return r
}

// setHandler installs a custom behaviour; it runs before the default
// "count and answer 204" logic and returns true if it wrote a response.
func (r *receiver) setHandler(h func(w http.ResponseWriter, r *http.Request) bool) {
	r.mu.Lock()
	r.handler = h
	r.mu.Unlock()
}

func (r *receiver) setSecret(t *testing.T, secret string) {
	v, err := webhook.NewVerifier(secret)
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.verifier = v
	r.mu.Unlock()
}

func (r *receiver) snapshot() (distinct, total, duplicates, bad int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.counts {
		total += n
		if n > 1 {
			duplicates += n - 1
		}
	}
	return len(r.counts), total, duplicates, r.bad
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func newDatabase(t *testing.T) string {
	t.Helper()
	_, url := testutil.NewDBWithURL(t)
	return url
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return c
}

func publish(t *testing.T, c *client.Client, n int, prefix string) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := c.CreateEvent(ctx(t), client.CreateEventInput{
			EventType: "e2e.event", Payload: map[string]int{"n": i}, IdempotencyKey: fmt.Sprintf("%s-%d", prefix, i),
		}); err != nil {
			t.Fatal(err)
		}
	}
}
