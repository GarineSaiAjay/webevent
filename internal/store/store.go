// Package store is the only package that talks SQL. Everything else works with
// the plain Go types defined here.
package store

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a row does not exist. Callers use errors.Is.
var ErrNotFound = errors.New("not found")

// Store wraps the connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool exposes the underlying pool for health checks.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Page is the cursor pagination input shared by all list calls.
type Page struct {
	Limit  int    // 1..100
	Cursor string // return rows with id < Cursor (ids are time-sortable)
}

// normalize clamps the limit into its allowed range.
func (p Page) normalize() Page {
	if p.Limit <= 0 {
		p.Limit = 20
	}
	if p.Limit > 100 {
		p.Limit = 100
	}
	return p
}

// trimPage cuts the extra "peek" row fetched to detect another page and
// returns the cursor for the next request ("" when there is none).
func trimPage[T any](rows []T, limit int, idOf func(T) string) ([]T, string) {
	if len(rows) <= limit {
		return rows, ""
	}
	rows = rows[:limit]
	return rows, idOf(rows[len(rows)-1])
}

func notFoundIfNoRows(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
