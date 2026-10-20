package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var embedded embed.FS

// Migration is one forward-only schema change.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// migrationLockID is an arbitrary constant used with pg_advisory_lock so that
// two server replicas starting at the same time never run migrations twice.
const migrationLockID int64 = 7_270_461_002

var fileRE = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// Migrations returns the migrations embedded in the binary, sorted by version.
func Migrations() ([]Migration, error) {
	sub, err := fs.Sub(embedded, "migrations")
	if err != nil {
		return nil, err
	}
	return LoadMigrations(sub)
}

// LoadMigrations parses NNNN_name.sql files from fsys.
func LoadMigrations(fsys fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	var out []Migration
	seen := map[int]string{}
	for _, e := range entries {
		if e.IsDir() || path.Ext(e.Name()) != ".sql" {
			continue
		}
		m := fileRE.FindStringSubmatch(e.Name())
		if m == nil {
			return nil, fmt.Errorf("migration %q: name must look like 0001_create_things.sql", e.Name())
		}
		v, _ := strconv.Atoi(m[1])
		if prev, dup := seen[v]; dup {
			return nil, fmt.Errorf("migrations %q and %q share version %d", prev, e.Name(), v)
		}
		seen[v] = e.Name()
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, Migration{Version: v, Name: m[2], SQL: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}

// Migrate applies every migration that has not been applied yet. Each
// migration runs in its own transaction together with the bookkeeping insert,
// so a failure leaves the schema exactly at the previous version.
func Migrate(ctx context.Context, pool *pgxpool.Pool, migs []Migration, log *slog.Logger) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("take migration lock: %w", err)
	}
	defer func() {
		// Use a fresh context: ctx may already be cancelled, and we must not
		// leave the session-level lock behind on a pooled connection.
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockID)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    integer PRIMARY KEY,
			name       text        NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[int]bool{}
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return err
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		return err
	}
	for _, v := range versions {
		applied[v] = true
	}

	for _, m := range migs {
		if applied[m.Version] {
			continue
		}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)", m.Version, m.Name)
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %04d_%s: %w", m.Version, m.Name, err)
		}
		log.Info("applied migration", "migration", m.Version, "name", m.Name)
	}
	return nil
}
