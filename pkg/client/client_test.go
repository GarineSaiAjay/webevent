package client_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/api"
	"github.com/GarineSaiAjay/webevent/internal/logging"
	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
	"github.com/GarineSaiAjay/webevent/pkg/client"
)

const key = "client-test-key-0123456789abcdefghijkl"

func newClient(t *testing.T) (*client.Client, *store.Store) {
	t.Helper()
	st := store.New(testutil.NewDB(t))
	srv := httptest.NewServer(api.New(api.Options{Logger: logging.Discard(), Store: st, APIKeys: []string{key}}))
	t.Cleanup(srv.Close)
	return client.New(srv.URL+"/", key), st
}

func TestClientRoundTrip(t *testing.T) {
	c, _ := newClient(t)
	ctx := context.Background()

	ep, err := c.CreateEndpoint(ctx, client.CreateEndpointInput{URL: "https://example.com/h", EventTypes: []string{"*"}})
	if err != nil {
		t.Fatal(err)
	}
	if ep.Secret == "" || ep.MaxInFlight != 10 {
		t.Fatalf("endpoint = %+v", ep)
	}

	res, err := c.CreateEvent(ctx, client.CreateEventInput{EventType: "a.b", Payload: map[string]int{"n": 1}, IdempotencyKey: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if res.DeliveryCount != 1 || res.Replayed {
		t.Fatalf("event = %+v", res)
	}
	again, err := c.CreateEvent(ctx, client.CreateEventInput{EventType: "a.b", Payload: map[string]int{"n": 1}, IdempotencyKey: "x"})
	if err != nil || !again.Replayed || again.ID != res.ID {
		t.Fatalf("replay = %+v, %v", again, err)
	}

	page, err := c.ListDeliveries(ctx, client.DeliveryFilter{EventID: res.ID}, client.ListOptions{})
	if err != nil || len(page.Data) != 1 {
		t.Fatalf("deliveries = %+v, %v", page, err)
	}
	d, err := c.GetDelivery(ctx, page.Data[0].ID)
	if err != nil || d.Status != "pending" || d.Attempts == nil && len(d.Attempts) != 0 {
		t.Fatalf("delivery = %+v, %v", d, err)
	}

	rot, err := c.RotateEndpointSecret(ctx, ep.ID, time.Hour)
	if err != nil || rot.Secret == ep.Secret {
		t.Fatalf("rotate = %+v, %v", rot, err)
	}
	if s, err := c.GetEndpointSecret(ctx, ep.ID); err != nil || s != rot.Secret {
		t.Fatalf("secret = %q, %v", s, err)
	}
	if err := c.DeleteEndpoint(ctx, ep.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetEndpoint(ctx, ep.ID); !client.IsNotFound(err) {
		t.Fatalf("after delete: %v", err)
	}
}

func TestAPIErrorDecoding(t *testing.T) {
	c, _ := newClient(t)
	_, err := c.CreateEndpoint(context.Background(), client.CreateEndpointInput{URL: "nope"})
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %T %v", err, err)
	}
	if apiErr.StatusCode != http.StatusUnprocessableEntity || apiErr.Code != "validation_failed" ||
		apiErr.Fields["url"] == "" || apiErr.RequestID == "" {
		t.Fatalf("apiErr = %+v", apiErr)
	}
}

func TestNonJSONErrorBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad gateway from proxy", http.StatusBadGateway)
	}))
	defer srv.Close()
	_, err := client.New(srv.URL, "k").ListEndpoints(context.Background(), client.ListOptions{})
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 502 || apiErr.Message != "bad gateway from proxy" {
		t.Fatalf("err = %v", err)
	}
}
