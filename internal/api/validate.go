package api

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/GarineSaiAjay/webevent/internal/netguard"
	"github.com/GarineSaiAjay/webevent/internal/store"
)

const (
	maxURLLength         = 2048
	maxDescriptionLength = 512
	maxEventTypes        = 100
)

var eventTypeRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.\-]{0,127}$`)

// validEventType reports whether s is a legal event type name such as
// "user.created" or "invoice.payment_failed".
func validEventType(s string) bool { return eventTypeRE.MatchString(s) }

// validateEndpointURL accepts absolute http(s) URLs without credentials or
// fragments. Unless allowPrivate is set, IP-literal hosts in private ranges
// are rejected here for a friendly error; the authoritative SSRF check runs
// at connect time, because DNS answers can change after creation.
func validateEndpointURL(raw string, allowPrivate bool) string {
	if raw == "" {
		return "is required"
	}
	if len(raw) > maxURLLength {
		return "must be at most " + strconv.Itoa(maxURLLength) + " characters"
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "is not a valid URL"
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return "must use http or https"
	}
	if u.Host == "" || u.Hostname() == "" {
		return "must include a host"
	}
	if u.User != nil {
		return "must not contain credentials"
	}
	if u.Fragment != "" {
		return "must not contain a fragment"
	}
	if !allowPrivate {
		if strings.EqualFold(u.Hostname(), "localhost") || netguard.CheckLiteral(u.Hostname()) != nil {
			return "must not point at a private, loopback or link-local address"
		}
	}
	return ""
}

// normalizeEventTypes validates and de-duplicates a subscription list while
// preserving order. "*" subscribes to every event type.
func normalizeEventTypes(in []string) ([]string, string) {
	if len(in) == 0 {
		return nil, `must contain at least one event type (use "*" for all)`
	}
	if len(in) > maxEventTypes {
		return nil, "must contain at most " + strconv.Itoa(maxEventTypes) + " entries"
	}
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, t := range in {
		t = strings.TrimSpace(t)
		if t != "*" && !validEventType(t) {
			return nil, "contains invalid event type " + strconv.Quote(t)
		}
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out, ""
}

func validEndpointStatus(s string) bool {
	return s == store.EndpointActive || s == store.EndpointPaused
}
