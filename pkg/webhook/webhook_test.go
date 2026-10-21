package webhook

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

var fixedNow = time.Unix(1_700_000_000, 0)

func verifier(t *testing.T, secrets ...string) *Verifier {
	t.Helper()
	v, err := NewVerifier(secrets...)
	if err != nil {
		t.Fatal(err)
	}
	v.Now = func() time.Time { return fixedNow }
	return v
}

func signedHeaders(t *testing.T, id string, ts time.Time, body []byte, secrets ...string) http.Header {
	t.Helper()
	h := http.Header{}
	if err := SetHeaders(h, id, ts, body, secrets...); err != nil {
		t.Fatal(err)
	}
	return h
}

func TestRoundTrip(t *testing.T) {
	secret := NewSecret()
	body := []byte(`{"id":"evt_1","type":"user.created"}`)
	h := signedHeaders(t, "evt_1", fixedNow, body, secret)
	if err := verifier(t, secret).Verify(h, body); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !strings.HasPrefix(h.Get(HeaderSignature), "v1,") {
		t.Fatalf("signature header = %q", h.Get(HeaderSignature))
	}
}

// TestKnownVector pins the exact algorithm so an accidental change (e.g. a
// different separator) is caught. The vector is from the Standard Webhooks
// reference implementation.
func TestKnownVector(t *testing.T) {
	// Public vector from the spec, not a real secret. Split so secret
	// scanners don't flag the "whsec_" prefix.
	secret := "whsec_" + "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	id := "msg_p5jXN8AQM9LWM0D4loKWxJek"
	ts := time.Unix(1614265330, 0)
	body := []byte(`{"test": 2432232314}`)
	got, err := Sign(id, ts, body, secret)
	if err != nil {
		t.Fatal(err)
	}
	want := "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
	if got != want {
		t.Fatalf("Sign = %s, want %s", got, want)
	}
}

func TestTamperingIsDetected(t *testing.T) {
	secret := NewSecret()
	body := []byte(`{"amount":100}`)
	h := signedHeaders(t, "evt_1", fixedNow, body, secret)
	v := verifier(t, secret)

	if err := v.Verify(h, []byte(`{"amount":999}`)); !errors.Is(err, ErrNoMatch) {
		t.Errorf("modified body: err = %v", err)
	}
	h2 := h.Clone()
	h2.Set(HeaderID, "evt_2")
	if err := v.Verify(h2, body); !errors.Is(err, ErrNoMatch) {
		t.Errorf("modified id: err = %v", err)
	}
	h3 := h.Clone()
	h3.Set(HeaderTimestamp, "1700000001")
	if err := v.Verify(h3, body); !errors.Is(err, ErrNoMatch) {
		t.Errorf("modified timestamp: err = %v", err)
	}
	if err := verifier(t, NewSecret()).Verify(h, body); !errors.Is(err, ErrNoMatch) {
		t.Errorf("wrong secret: err = %v", err)
	}
}

func TestTimestampTolerance(t *testing.T) {
	secret := NewSecret()
	body := []byte(`{}`)
	v := verifier(t, secret)
	old := signedHeaders(t, "m", fixedNow.Add(-6*time.Minute), body, secret)
	if err := v.Verify(old, body); !errors.Is(err, ErrTimestampTooOld) {
		t.Errorf("old: err = %v", err)
	}
	future := signedHeaders(t, "m", fixedNow.Add(6*time.Minute), body, secret)
	if err := v.Verify(future, body); !errors.Is(err, ErrTimestampTooNew) {
		t.Errorf("future: err = %v", err)
	}
	edge := signedHeaders(t, "m", fixedNow.Add(-4*time.Minute), body, secret)
	if err := v.Verify(edge, body); err != nil {
		t.Errorf("within tolerance: err = %v", err)
	}
}

func TestRotationWithMultipleSignatures(t *testing.T) {
	oldSecret, newSecret := NewSecret(), NewSecret()
	body := []byte(`{}`)
	// The sender signs with both during the grace period.
	h := signedHeaders(t, "m", fixedNow, body, newSecret, oldSecret)
	if n := len(strings.Fields(h.Get(HeaderSignature))); n != 2 {
		t.Fatalf("want 2 signatures, got %d", n)
	}
	// Receivers that still only know the old secret keep working...
	if err := verifier(t, oldSecret).Verify(h, body); err != nil {
		t.Errorf("old secret: %v", err)
	}
	// ...and so do receivers that already switched.
	if err := verifier(t, newSecret).Verify(h, body); err != nil {
		t.Errorf("new secret: %v", err)
	}
}

func TestMalformedInput(t *testing.T) {
	secret := NewSecret()
	v := verifier(t, secret)
	if err := v.Verify(http.Header{}, nil); !errors.Is(err, ErrMissingHeaders) {
		t.Errorf("no headers: %v", err)
	}
	h := signedHeaders(t, "m", fixedNow, nil, secret)
	h.Set(HeaderTimestamp, "yesterday")
	if err := v.Verify(h, nil); !errors.Is(err, ErrInvalidTimestamp) {
		t.Errorf("bad timestamp: %v", err)
	}
	for _, bad := range []string{"", "nope", "whsec_!!!", "whsec_c2hvcnQ="} {
		if _, err := NewVerifier(bad); err == nil {
			t.Errorf("NewVerifier(%q) should fail", bad)
		}
	}
	if _, err := Sign("m", fixedNow, nil); err == nil {
		t.Error("Sign without secrets should fail")
	}
}

func FuzzVerifyNeverPanics(f *testing.F) {
	f.Add("m", "1700000000", "v1,abc", []byte("{}"))
	f.Add("", "", "", []byte(nil))
	f.Add("x", "-1", "v1, v2,zzz", []byte("\x00"))
	secret := NewSecret()
	v, _ := NewVerifier(secret)
	v.Now = func() time.Time { return fixedNow }
	f.Fuzz(func(t *testing.T, id, ts, sig string, body []byte) {
		h := http.Header{}
		h.Set(HeaderID, id)
		h.Set(HeaderTimestamp, ts)
		h.Set(HeaderSignature, sig)
		if err := v.Verify(h, body); err == nil {
			t.Fatalf("random input verified: id=%q ts=%q sig=%q", id, ts, sig)
		}
	})
}
