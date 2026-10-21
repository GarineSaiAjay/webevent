package api

import (
	"net/url"
	"strings"
	"testing"
)

// FuzzValidateEndpointURL: arbitrary input must never panic, and anything
// accepted must really be an absolute http(s) URL without credentials.
func FuzzValidateEndpointURL(f *testing.F) {
	for _, s := range []string{"https://example.com/h", "http://[::1]:80/", "ftp://x", "", "https://u:p@x", "http://%41", "https://ex ample.com"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if msg := validateEndpointURL(raw, false); msg == "" {
			u, err := url.Parse(raw)
			if err != nil {
				t.Fatalf("accepted unparsable URL %q", raw)
			}
			if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
				t.Fatalf("accepted bad URL %q", raw)
			}
		}
	})
}

// FuzzNormalizeEventTypes: output is always valid and duplicate-free.
func FuzzNormalizeEventTypes(f *testing.F) {
	f.Add("user.created,*,user.created")
	f.Add("a b,c")
	f.Add("")
	f.Fuzz(func(t *testing.T, csv string) {
		out, msg := normalizeEventTypes(strings.Split(csv, ","))
		if msg != "" {
			return
		}
		seen := map[string]bool{}
		for _, e := range out {
			if e != "*" && !validEventType(e) {
				t.Fatalf("invalid type %q accepted", e)
			}
			if seen[e] {
				t.Fatalf("duplicate %q in output", e)
			}
			seen[e] = true
		}
	})
}

func FuzzIdempotencyKey(f *testing.F) {
	f.Add("order-1")
	f.Add("has space")
	f.Add(strings.Repeat("k", 300))
	f.Fuzz(func(t *testing.T, k string) {
		if validIdempotencyKey(k) {
			if len(k) == 0 || len(k) > maxIdempotencyKeyLen || strings.ContainsAny(k, " \t\r\n") {
				t.Fatalf("accepted bad key %q", k)
			}
		}
	})
}
