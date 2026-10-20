// Package id generates prefixed, time-sortable identifiers such as
// "evt_01j9z3k6q8m2x7c4v5b6n7p8r9".
//
// Layout (128 bits, like a ULID): 48-bit Unix millisecond timestamp followed by
// 80 random bits, encoded as 26 lowercase Crockford base32 characters. Because
// the alphabet is in ascending ASCII order and the length is fixed, sorting the
// strings sorts by creation time, which makes "id < cursor" pagination work.
//
// Within one process, New is strictly monotonic: several ids created in the
// same millisecond reuse that millisecond and increment the random part by
// one, so they still sort in creation order (the ULID "monotonic" variant).
// Ids from different processes created in the same millisecond have no
// defined order relative to each other.
package id

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/binary"
	"strings"
	"sync"
	"time"
)

const alphabet = "0123456789abcdefghjkmnpqrstvwxyz"

var encoding = base32.NewEncoding(alphabet).WithPadding(base32.NoPadding)

// Prefixes used across the system.
const (
	PrefixEndpoint = "ep"
	PrefixEvent    = "evt"
	PrefixDelivery = "dlv"
	PrefixAttempt  = "att"
)

var (
	mu      sync.Mutex
	lastMS  uint64
	lastRnd [10]byte
)

// New returns a new identifier with the given prefix. Successive calls in one
// process always return increasing identifiers.
func New(prefix string) string {
	ms := uint64(time.Now().UnixMilli())
	mu.Lock()
	if ms <= lastMS {
		// Same millisecond, or the clock stepped backwards: stay on the last
		// timestamp and increment the random part (carrying into the
		// timestamp in the astronomically unlikely case it overflows).
		ms = lastMS
		if increment(lastRnd[:]) {
			ms++
			lastMS = ms
		}
	} else {
		lastMS = ms
		if _, err := rand.Read(lastRnd[:]); err != nil {
			mu.Unlock()
			panic("id: crypto/rand failed: " + err.Error())
		}
	}
	rnd := lastRnd
	mu.Unlock()
	return encode(prefix, ms, rnd[:])
}

// increment adds one to a big-endian number and reports overflow.
func increment(b []byte) bool {
	for i := len(b) - 1; i >= 0; i-- {
		b[i]++
		if b[i] != 0 {
			return false
		}
	}
	return true
}

func encode(prefix string, ms uint64, rnd []byte) string {
	var b [16]byte
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], ms)
	copy(b[:6], ts[2:])
	copy(b[6:], rnd)
	return prefix + "_" + encoding.EncodeToString(b[:])
}

// NewAt returns an identifier for an explicit timestamp with a fresh random
// part (not monotonic; useful in tests).
func NewAt(prefix string, t time.Time) string {
	var rnd [10]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		// crypto/rand never fails on supported platforms; if it does the
		// process cannot safely continue generating identifiers.
		panic("id: crypto/rand failed: " + err.Error())
	}
	return encode(prefix, uint64(t.UnixMilli()), rnd[:])
}

// Valid reports whether s looks like an identifier with the given prefix.
func Valid(prefix, s string) bool {
	rest, ok := strings.CutPrefix(s, prefix+"_")
	if !ok || len(rest) != 26 {
		return false
	}
	_, err := encoding.DecodeString(rest)
	return err == nil
}
