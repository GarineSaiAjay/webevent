package db_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/GarineSaiAjay/webevent/internal/db"
	"github.com/GarineSaiAjay/webevent/internal/logging"
	"github.com/GarineSaiAjay/webevent/internal/testutil"
)

func TestLoadMigrationsSortsAndValidates(t *testing.T) {
	fsys := fstest.MapFS{
		"0002_second.sql": {Data: []byte("SELECT 2")},
		"0001_first.sql":  {Data: []byte("SELECT 1")},
		"README.md":       {Data: []byte("ignored")},
	}
	migs, err := db.LoadMigrations(fsys)
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) != 2 || migs[0].Version != 1 || migs[1].Version != 2 || migs[0].Name != "first" {
		t.Fatalf("unexpected migrations: %+v", migs)
	}
}

func TestLoadMigrationsRejectsBadNames(t *testing.T) {
	_, err := db.LoadMigrations(fstest.MapFS{"create_things.sql": {Data: []byte("x")}})
	if err == nil {
		t.Fatal("expected error for badly named migration")
	}
}

func TestLoadMigrationsRejectsDuplicateVersions(t *testing.T) {
	_, err := db.LoadMigrations(fstest.MapFS{
		"0001_a.sql": {Data: []byte("x")},
		"0001_b.sql": {Data: []byte("y")},
	})
	if err == nil || !strings.Contains(err.Error(), "share version") {
		t.Fatalf("expected duplicate version error, got %v", err)
	}
}

func TestEmbeddedMigrationsParse(t *testing.T) {
	migs, err := db.Migrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migs) == 0 {
		t.Fatal("no embedded migrations found")
	}
	for i, m := range migs {
		if m.Version != i+1 {
			t.Fatalf("migration versions must be contiguous starting at 1; got %d at index %d", m.Version, i)
		}
	}
}

func TestMigrateIsIdempotentAndConcurrencySafe(t *testing.T) {
	pool := testutil.NewDB(t) // already migrated once
	ctx := context.Background()
	migs, err := db.Migrations()
	if err != nil {
		t.Fatal(err)
	}

	// Five "replicas" racing to migrate must all succeed without error.
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- db.Migrate(ctx, pool, migs, logging.Discard())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent migrate failed: %v", err)
		}
	}

	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != len(migs) {
		t.Fatalf("schema_migrations has %d rows, want %d", n, len(migs))
	}
}

func TestFailedMigrationRollsBack(t *testing.T) {
	pool := testutil.NewDB(t)
	ctx := context.Background()
	bad := []db.Migration{{
		Version: 9999,
		Name:    "broken",
		SQL:     "CREATE TABLE half_done (id int); SELECT * FROM table_that_does_not_exist;",
	}}
	if err := db.Migrate(ctx, pool, bad, logging.Discard()); err == nil {
		t.Fatal("expected failure")
	}
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('half_done') IS NOT NULL").Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("table from failed migration should have been rolled back")
	}
}
