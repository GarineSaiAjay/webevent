package netguard

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

func TestAllowed(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "127.8.9.10", "10.1.2.3", "172.16.0.1", "172.31.255.255", "192.168.1.1",
		"169.254.169.254", "100.64.0.1", "0.0.0.0", "224.0.0.1", "255.255.255.255",
		"::1", "::", "fe80::1", "fd00::1", "ff02::1", "::ffff:127.0.0.1", "::ffff:10.0.0.1",
		"64:ff9b::a00:1", "2002:a00:1::",
	}
	for _, s := range blocked {
		if Allowed(netip.MustParseAddr(s)) {
			t.Errorf("%s should be blocked", s)
		}
	}
	allowed := []string{"8.8.8.8", "1.1.1.1", "172.32.0.1", "2606:4700:4700::1111", "::ffff:8.8.8.8"}
	for _, s := range allowed {
		if !Allowed(netip.MustParseAddr(s)) {
			t.Errorf("%s should be allowed", s)
		}
	}
	if Allowed(netip.Addr{}) {
		t.Error("invalid address must not be allowed")
	}
}

func TestCheckLiteral(t *testing.T) {
	if err := CheckLiteral("10.0.0.1"); !errors.Is(err, ErrBlocked) {
		t.Errorf("10.0.0.1: %v", err)
	}
	if err := CheckLiteral("example.com"); err != nil {
		t.Errorf("hostnames are checked later: %v", err)
	}
	if err := CheckLiteral("93.184.216.34"); err != nil {
		t.Errorf("public IP: %v", err)
	}
}

// TestDialerBlocksLoopback proves the guard works where it matters: at
// connect time, even when the URL looks harmless.
func TestDialerBlocksLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("request must never reach the server")
	}))
	defer srv.Close()

	dialer := &net.Dialer{Timeout: time.Second, Control: Control}
	client := &http.Client{Transport: &http.Transport{DialContext: dialer.DialContext}}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected the dial to be blocked")
	}
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("err = %v, want ErrBlocked", err)
	}
}
