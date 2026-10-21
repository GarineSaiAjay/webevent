// Package testutil provides helpers shared by integration tests.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GarineSaiAjay/webevent/internal/db"
	"github.com/GarineSaiAjay/webevent/internal/logging"
)

// EnvTestDatabaseURL points at a Postgres server the tests may create and
// drop databases on, e.g. postgres://webevent:webevent@localhost:5433/postgres
const EnvTestDatabaseURL = "WEBEVENT_TEST_DATABASE_URL"

// NewDB creates a brand-new, fully migrated database for a single test and
// drops it when the test finishes. Every test gets total isolation, so tests
// can run in parallel and never see each other's rows.
func NewDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool, _ := NewDBWithURL(t)
	return pool
}

// NewDBWithURL is NewDB but also returns the connection string of the new
// database (needed by code that opens its own connections, e.g. LISTEN).
func NewDBWithURL(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	return newDB(t, true)
}

// NewEmptyDB creates a fresh database WITHOUT running migrations, for tests
// that exercise migrations themselves.
func NewEmptyDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	pool, _ := newDB(t, false)
	return pool
}

// NewEmptyDBWithURL is NewEmptyDB plus the connection string.
func NewEmptyDBWithURL(t testing.TB) (*pgxpool.Pool, string) {
	t.Helper()
	return newDB(t, false)
}

func newDB(t testing.TB, migrate bool) (*pgxpool.Pool, string) {
	t.Helper()
	adminURL := os.Getenv(EnvTestDatabaseURL)
	if adminURL == "" {
		t.Skipf("%s not set; skipping integration test", EnvTestDatabaseURL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect admin database: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }()

	name := "webevent_t_" + randomHex(8)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create test database: %v", err)
	}

	u, err := url.Parse(adminURL)
	if err != nil {
		t.Fatalf("parse %s: %v", EnvTestDatabaseURL, err)
	}
	u.Path = "/" + name
	dbURL := u.String()

	pool, err := db.Connect(ctx, dbURL, 10, logging.Discard())
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	if migrate {
		migs, err := db.Migrations()
		if err != nil {
			t.Fatalf("load migrations: %v", err)
		}
		if err := db.Migrate(ctx, pool, migs, logging.Discard()); err != nil {
			t.Fatalf("migrate test database: %v", err)
		}
	}

	t.Cleanup(func() {
		pool.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		admin, err := pgx.Connect(ctx, adminURL)
		if err != nil {
			t.Logf("cleanup: connect admin: %v", err)
			return
		}
		defer func() { _ = admin.Close(ctx) }()
		if _, err := admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", pgx.Identifier{name}.Sanitize())); err != nil {
			t.Logf("cleanup: drop %s: %v", name, err)
		}
	})
	return pool, dbURL
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
