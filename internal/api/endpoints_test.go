package api_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

func createEndpoint(t *testing.T, a *testAPI, url string, types ...string) store.Endpoint {
	t.Helper()
	if len(types) == 0 {
		types = []string{"*"}
	}
	rec := a.do(http.MethodPost, "/v1/endpoints", map[string]any{"url": url, "event_types": types})
	expectStatus(t, rec, http.StatusCreated)
	return decode[store.Endpoint](t, rec)
}

func TestAuthRequired(t *testing.T) {
	a := newTestAPI(t)
	for _, auth := range []string{"", "Bearer", "Bearer wrong-key", "Basic " + testAPIKey, testAPIKey} {
		req := httptest.NewRequest(http.MethodGet, "/v1/endpoints", nil)
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		a.h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("Authorization %q: status = %d, want 401", auth, rec.Code)
		}
		if rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("Authorization %q: missing WWW-Authenticate", auth)
		}
	}
}

func TestHealthEndpointsNeedNoAuth(t *testing.T) {
	a := newTestAPI(t)
	for _, path := range []string{"/healthz", "/readyz"} {
		rec := httptest.NewRecorder()
		a.h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		expectStatus(t, rec, http.StatusOK)
	}
}

func TestEndpointCRUD(t *testing.T) {
	a := newTestAPI(t)

	// Create
	rec := a.do(http.MethodPost, "/v1/endpoints", map[string]any{
		"url":         "https://example.com/webhooks",
		"description": "billing service",
		"event_types": []string{"invoice.paid", "invoice.paid", "user.created"},
	})
	expectStatus(t, rec, http.StatusCreated)
	ep := decode[store.Endpoint](t, rec)
	if !strings.HasPrefix(ep.ID, "ep_") || ep.Status != "active" || ep.Description != "billing service" {
		t.Fatalf("unexpected endpoint %+v", ep)
	}
	if len(ep.EventTypes) != 2 {
		t.Fatalf("duplicates should be removed: %v", ep.EventTypes)
	}

	// Get
	rec = a.do(http.MethodGet, "/v1/endpoints/"+ep.ID, nil)
	expectStatus(t, rec, http.StatusOK)
	if got := decode[store.Endpoint](t, rec); got.URL != ep.URL {
		t.Fatalf("get returned %+v", got)
	}

	// Patch only the status; everything else must stay as it was.
	rec = a.do(http.MethodPatch, "/v1/endpoints/"+ep.ID, map[string]any{"status": "paused"})
	expectStatus(t, rec, http.StatusOK)
	patched := decode[store.Endpoint](t, rec)
	if patched.Status != "paused" || patched.URL != ep.URL || patched.Description != ep.Description {
		t.Fatalf("patch changed too much: %+v", patched)
	}
	if !patched.UpdatedAt.After(ep.UpdatedAt) && !patched.UpdatedAt.Equal(ep.UpdatedAt) {
		t.Fatal("updated_at went backwards")
	}

	// Patch description to empty string is allowed and distinct from "absent".
	rec = a.do(http.MethodPatch, "/v1/endpoints/"+ep.ID, map[string]any{"description": ""})
	expectStatus(t, rec, http.StatusOK)
	if got := decode[store.Endpoint](t, rec); got.Description != "" {
		t.Fatalf("description = %q, want empty", got.Description)
	}

	// Delete
	rec = a.do(http.MethodDelete, "/v1/endpoints/"+ep.ID, nil)
	expectStatus(t, rec, http.StatusNoContent)
	rec = a.do(http.MethodGet, "/v1/endpoints/"+ep.ID, nil)
	expectStatus(t, rec, http.StatusNotFound)
	rec = a.do(http.MethodDelete, "/v1/endpoints/"+ep.ID, nil)
	expectStatus(t, rec, http.StatusNotFound)
	rec = a.do(http.MethodPatch, "/v1/endpoints/"+ep.ID, map[string]any{"status": "active"})
	expectStatus(t, rec, http.StatusNotFound)
}

func TestCreateEndpointValidation(t *testing.T) {
	a := newTestAPI(t)
	cases := []struct {
		name  string
		body  any
		field string
	}{
		{"missing url", map[string]any{"event_types": []string{"*"}}, "url"},
		{"ftp url", map[string]any{"url": "ftp://x.com", "event_types": []string{"*"}}, "url"},
		{"relative url", map[string]any{"url": "/hooks", "event_types": []string{"*"}}, "url"},
		{"credentials", map[string]any{"url": "https://u:p@x.com", "event_types": []string{"*"}}, "url"},
		{"fragment", map[string]any{"url": "https://x.com/#a", "event_types": []string{"*"}}, "url"},
		{"no types", map[string]any{"url": "https://x.com"}, "event_types"},
		{"bad type", map[string]any{"url": "https://x.com", "event_types": []string{"has space"}}, "event_types"},
		{"long description", map[string]any{"url": "https://x.com", "event_types": []string{"*"}, "description": strings.Repeat("x", 513)}, "description"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := a.do(http.MethodPost, "/v1/endpoints", tc.body)
			expectStatus(t, rec, http.StatusUnprocessableEntity)
			body := decode[errorBody](t, rec)
			if body.Error.Code != "validation_failed" || body.Error.Fields[tc.field] == "" {
				t.Fatalf("expected validation error on %q, got %+v", tc.field, body.Error)
			}
		})
	}
}

func TestMalformedBodies(t *testing.T) {
	a := newTestAPI(t)
	cases := map[string]struct {
		body string
		want int
	}{
		"empty":         {"", http.StatusBadRequest},
		"syntax":        {`{"url":`, http.StatusBadRequest},
		"unknown field": {`{"url":"https://x.com","event_types":["*"],"color":"red"}`, http.StatusBadRequest},
		"wrong type":    {`{"url":42}`, http.StatusBadRequest},
		"two objects":   {`{"url":"https://x.com","event_types":["*"]}{}`, http.StatusBadRequest},
		"too large":     {`{"description":"` + strings.Repeat("x", 600<<10) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := a.do(http.MethodPost, "/v1/endpoints", tc.body)
			expectStatus(t, rec, tc.want)
		})
	}

	rec := a.do(http.MethodPost, "/v1/endpoints", `{"url":"https://x.com","event_types":["*"]}`, "Content-Type", "text/plain")
	expectStatus(t, rec, http.StatusUnsupportedMediaType)
}

func TestUpdateEndpointValidation(t *testing.T) {
	a := newTestAPI(t)
	ep := createEndpoint(t, a, "https://example.com/h")
	rec := a.do(http.MethodPatch, "/v1/endpoints/"+ep.ID, map[string]any{"status": "deleted", "url": "nope", "event_types": []string{}})
	expectStatus(t, rec, http.StatusUnprocessableEntity)
	body := decode[errorBody](t, rec)
	for _, f := range []string{"status", "url", "event_types"} {
		if body.Error.Fields[f] == "" {
			t.Errorf("missing validation error for %s: %+v", f, body.Error.Fields)
		}
	}
}

func TestGetEndpointWithMalformedID(t *testing.T) {
	a := newTestAPI(t)
	rec := a.do(http.MethodGet, "/v1/endpoints/not-an-id", nil)
	expectStatus(t, rec, http.StatusNotFound)
}

func TestListEndpointsPagination(t *testing.T) {
	a := newTestAPI(t)
	var created []string
	for i := 0; i < 7; i++ {
		created = append(created, createEndpoint(t, a, fmt.Sprintf("https://example.com/%d", i)).ID)
	}

	var seen []string
	cursor := ""
	pages := 0
	for {
		path := "/v1/endpoints?limit=3"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		rec := a.do(http.MethodGet, path, nil)
		expectStatus(t, rec, http.StatusOK)
		page := decode[struct {
			Data       []store.Endpoint `json:"data"`
			NextCursor string           `json:"next_cursor"`
		}](t, rec)
		pages++
		for _, e := range page.Data {
			seen = append(seen, e.ID)
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if pages != 3 || len(seen) != 7 {
		t.Fatalf("pages=%d seen=%d, want 3 pages / 7 endpoints", pages, len(seen))
	}
	// Newest first: the reverse of creation order.
	for i := range seen {
		if seen[i] != created[len(created)-1-i] {
			t.Fatalf("order mismatch at %d: %s vs %s", i, seen[i], created[len(created)-1-i])
		}
	}

	rec := a.do(http.MethodGet, "/v1/endpoints?limit=0", nil)
	expectStatus(t, rec, http.StatusBadRequest)
	rec = a.do(http.MethodGet, "/v1/endpoints?limit=101", nil)
	expectStatus(t, rec, http.StatusBadRequest)
}

func TestListExcludesDeleted(t *testing.T) {
	a := newTestAPI(t)
	keep := createEndpoint(t, a, "https://example.com/keep")
	gone := createEndpoint(t, a, "https://example.com/gone")
	expectStatus(t, a.do(http.MethodDelete, "/v1/endpoints/"+gone.ID, nil), http.StatusNoContent)

	rec := a.do(http.MethodGet, "/v1/endpoints", nil)
	page := decode[struct {
		Data []store.Endpoint `json:"data"`
	}](t, rec)
	if len(page.Data) != 1 || page.Data[0].ID != keep.ID {
		t.Fatalf("list = %+v", page.Data)
	}
}
