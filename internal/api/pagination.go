package api

import (
	"net/http"
	"strconv"

	"github.com/GarineSaiAjay/webevent/internal/store"
)

// listResponse is the envelope for every list endpoint.
type listResponse[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"next_cursor,omitempty"`
}

// newList builds a listResponse, turning a nil slice into [] so clients
// always receive a JSON array.
func newList[T any](data []T, next string) listResponse[T] {
	if data == nil {
		data = []T{}
	}
	return listResponse[T]{Data: data, NextCursor: next}
}

// parsePage reads ?limit= and ?cursor= from the query string.
func parsePage(r *http.Request) (store.Page, error) {
	q := r.URL.Query()
	p := store.Page{Limit: 20, Cursor: q.Get("cursor")}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			return p, errBadRequest("limit must be an integer between 1 and 100")
		}
		p.Limit = n
	}
	if len(p.Cursor) > 64 {
		return p, errBadRequest("cursor is invalid")
	}
	return p, nil
}
