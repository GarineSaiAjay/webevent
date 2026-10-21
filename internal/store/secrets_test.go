package store_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/GarineSaiAjay/webevent/internal/secrets"
	"github.com/GarineSaiAjay/webevent/internal/store"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

func TestSecretsAreEncryptedAtRest(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	box, err := secrets.NewBox(base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	pool := testutil.NewDB(t)
	st := store.New(pool, store.WithSecretBox(box))
	ctx := context.Background()

	ep, err := st.CreateEndpoint(ctx, store.NewEndpoint{URL: "https://x.example", EventTypes: []string{"*"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ep.Secret, "whsec_") {
		t.Fatalf("create must return the plaintext secret once, got %q", ep.Secret)
	}

	var raw string
	if err := pool.QueryRow(ctx, `SELECT secret FROM endpoints WHERE id = $1`, ep.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(raw, "enc:v1:") || strings.Contains(raw, ep.Secret) {
		t.Fatalf("secret stored in plaintext: %q", raw)
	}

	got, err := st.GetEndpointSecret(ctx, ep.ID)
	if err != nil || got != ep.Secret {
		t.Fatalf("GetEndpointSecret = %q, %v", got, err)
	}

	// Ordinary reads never include the secret.
	read, _ := st.GetEndpoint(ctx, ep.ID)
	if read.Secret != "" {
		t.Fatal("GetEndpoint must not expose the secret")
	}

	// Claims decrypt for signing.
	if _, err := st.CreateEvent(ctx, store.NewEvent{EventType: "a.b", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	jobs, err := st.ClaimJobs(ctx, "w", 1, time.Minute)
	if err != nil || len(jobs) != 1 {
		t.Fatal(err)
	}
	if jobs[0].SecretErr != nil || len(jobs[0].Secrets) != 1 || jobs[0].Secrets[0] != ep.Secret {
		t.Fatalf("job secrets = %v (err %v)", jobs[0].Secrets, jobs[0].SecretErr)
	}
}

func TestRotateSecret(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	ep := mustEndpoint(t, st, "https://x.example", "*")
	newSecret, err := st.RotateEndpointSecret(ctx, ep.ID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if newSecret == ep.Secret {
		t.Fatal("rotation must produce a new secret")
	}
	if got, _ := st.GetEndpointSecret(ctx, ep.ID); got != newSecret {
		t.Fatal("current secret must be the new one")
	}
	if _, err := st.RotateEndpointSecret(ctx, "ep_missing", time.Hour); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing endpoint: %v", err)
	}
}
