package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// apiKeyAuth guards the /v1 API with static bearer tokens.
//
// We keep only SHA-256 digests of the configured keys in memory and compare
// digests in constant time. Hashing first means every comparison is over 32
// bytes regardless of the presented key length, so timing reveals nothing
// about which key (or how much of it) matched.
type apiKeyAuth struct {
	digests [][32]byte
}

func newAPIKeyAuth(keys []string) *apiKeyAuth {
	a := &apiKeyAuth{}
	for _, k := range keys {
		a.digests = append(a.digests, sha256.Sum256([]byte(k)))
	}
	return a
}

func (a *apiKeyAuth) valid(presented string) bool {
	d := sha256.Sum256([]byte(presented))
	ok := 0
	for i := range a.digests {
		// No early return: always check every key.
		ok |= subtle.ConstantTimeCompare(d[:], a.digests[i][:])
	}
	return ok == 1
}

func (s *Server) requireAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !found || token == "" || !s.auth.valid(token) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="webevent"`)
			writeError(w, r, s.log, errUnauthorized())
			return
		}
		next.ServeHTTP(w, r)
	})
}
