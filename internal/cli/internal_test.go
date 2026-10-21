package cli

import (
	"testing"
	"time"
)

func TestParseTimeFlag(t *testing.T) {
	if got, err := parseTimeFlag(""); err != nil || !got.IsZero() {
		t.Fatalf("empty: %v %v", got, err)
	}
	got, err := parseTimeFlag("2h")
	if err != nil || time.Since(got) < 119*time.Minute || time.Since(got) > 121*time.Minute {
		t.Fatalf("2h: %v %v", got, err)
	}
	got, err = parseTimeFlag("2026-01-02T03:04:05Z")
	if err != nil || got.Year() != 2026 {
		t.Fatalf("rfc3339: %v %v", got, err)
	}
	if _, err := parseTimeFlag("yesterday"); err == nil {
		t.Fatal("garbage must fail")
	}
}

func TestMask(t *testing.T) {
	for in, want := range map[string]string{"": "(not set)", "short": "****", "abcdefghijkl": "abcd…ijkl"} {
		if got := mask(in); got != want {
			t.Errorf("mask(%q) = %q, want %q", in, got, want)
		}
	}
}
