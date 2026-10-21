package secrets

import (
	"encoding/base64"
	"strings"
	"testing"
)

func testKey() string { return base64.StdEncoding.EncodeToString(make([]byte, 32)) }

func TestSealOpenRoundTrip(t *testing.T) {
	box, err := NewBox(testKey())
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := box.Seal("whsec_abc")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sealed, prefix) || strings.Contains(sealed, "whsec_abc") {
		t.Fatalf("not sealed: %q", sealed)
	}
	got, err := box.Open(sealed)
	if err != nil || got != "whsec_abc" {
		t.Fatalf("Open = %q, %v", got, err)
	}
}

func TestPlaintextPassthrough(t *testing.T) {
	box, _ := NewBox("")
	if box.Encrypting() {
		t.Fatal("empty key must not encrypt")
	}
	s, _ := box.Seal("x")
	if s != "x" {
		t.Fatalf("Seal = %q", s)
	}
	if got, err := box.Open("legacy"); err != nil || got != "legacy" {
		t.Fatalf("Open = %q, %v", got, err)
	}
}

func TestOpenErrors(t *testing.T) {
	keyed, _ := NewBox(testKey())
	sealed, _ := keyed.Seal("s")

	plain, _ := NewBox("")
	if _, err := plain.Open(sealed); err == nil {
		t.Fatal("expected error opening encrypted value without a key")
	}

	other, _ := NewBox(base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32))))
	if _, err := other.Open(sealed); err == nil {
		t.Fatal("expected error with wrong key")
	}
}

func TestNewBoxRejectsBadKeys(t *testing.T) {
	for _, k := range []string{"!!!notbase64", base64.StdEncoding.EncodeToString([]byte("short"))} {
		if _, err := NewBox(k); err == nil {
			t.Errorf("NewBox(%q) succeeded", k)
		}
	}
}
