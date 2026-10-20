package id

import (
	"sort"
	"testing"
	"time"
)

func TestFormat(t *testing.T) {
	s := New(PrefixEvent)
	if len(s) != len("evt_")+26 {
		t.Fatalf("unexpected length %d for %q", len(s), s)
	}
	if !Valid(PrefixEvent, s) {
		t.Fatalf("Valid(%q) = false", s)
	}
	if Valid(PrefixEndpoint, s) {
		t.Fatal("prefix mismatch should be invalid")
	}
}

func TestInvalid(t *testing.T) {
	for _, s := range []string{"", "evt_", "evt_short", "evt_" + "UUUUUUUUUUUUUUUUUUUUUUUUUU", "ep_01j9z3k6q8m2x7c4v5b6n7p8r9"} {
		if Valid(PrefixEvent, s) {
			t.Errorf("Valid(%q) = true, want false", s)
		}
	}
}

func TestSortableByTime(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var ids []string
	for i := 0; i < 100; i++ {
		ids = append(ids, NewAt("x", base.Add(time.Duration(i)*time.Millisecond)))
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatal("ids generated with increasing timestamps are not sorted")
	}
}

func TestUnique(t *testing.T) {
	seen := make(map[string]struct{}, 10000)
	for i := 0; i < 10000; i++ {
		s := New("x")
		if _, dup := seen[s]; dup {
			t.Fatalf("duplicate id %q", s)
		}
		seen[s] = struct{}{}
	}
}

// TestMonotonicWithinProcess: ids created back to back (many in the same
// millisecond) must still be strictly increasing.
func TestMonotonicWithinProcess(t *testing.T) {
	prev := New("x")
	for i := 0; i < 100000; i++ {
		next := New("x")
		if next <= prev {
			t.Fatalf("id %d not increasing: %s <= %s", i, next, prev)
		}
		prev = next
	}
}

func TestIncrementCarries(t *testing.T) {
	b := []byte{0x00, 0xff, 0xff}
	if overflow := increment(b); overflow || b[0] != 1 || b[1] != 0 || b[2] != 0 {
		t.Fatalf("increment = %x overflow=%v", b, overflow)
	}
	b = []byte{0xff, 0xff}
	if overflow := increment(b); !overflow {
		t.Fatal("expected overflow")
	}
}
