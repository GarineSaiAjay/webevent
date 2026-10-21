// Package webhook signs and verifies webhooks using the Standard Webhooks
// scheme (https://www.standardwebhooks.com), the same scheme used by many
// webhook providers.
//
// Every request carries three headers:
//
//	webhook-id:        unique message id (stable across retries; use it to dedupe)
//	webhook-timestamp: unix seconds when this attempt was signed
//	webhook-signature: space-separated list of "v1,<base64 HMAC-SHA256>"
//
// The signature covers "<id>.<timestamp>.<body>", so neither the id, the
// time nor a single byte of the body can be altered without detection, and
// the timestamp lets receivers reject replays of old requests.
//
// Receivers (your customers) import this package to verify requests:
//
//	v, err := webhook.NewVerifier(os.Getenv("WEBHOOK_SECRET"))
//	...
//	body, _ := io.ReadAll(r.Body)
//	if err := v.Verify(r.Header, body); err != nil {
//		http.Error(w, "bad signature", http.StatusUnauthorized)
//		return
//	}
package webhook

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Header names.
const (
	HeaderID        = "Webhook-Id"
	HeaderTimestamp = "Webhook-Timestamp"
	HeaderSignature = "Webhook-Signature"
)

// SecretPrefix marks webhook signing secrets.
const SecretPrefix = "whsec_"

// DefaultTolerance is how far a timestamp may be from the receiver's clock.
const DefaultTolerance = 5 * time.Minute

// Errors returned by Verify.
var (
	ErrMissingHeaders   = errors.New("webhook: missing webhook-id, webhook-timestamp or webhook-signature header")
	ErrInvalidTimestamp = errors.New("webhook: invalid timestamp")
	ErrTimestampTooOld  = errors.New("webhook: timestamp too old")
	ErrTimestampTooNew  = errors.New("webhook: timestamp too far in the future")
	ErrNoMatch          = errors.New("webhook: no matching signature")
)

// NewSecret generates a random signing secret: "whsec_" + base64(24 bytes).
func NewSecret() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic("webhook: crypto/rand failed: " + err.Error())
	}
	return SecretPrefix + base64.StdEncoding.EncodeToString(b)
}

// DecodeSecret returns the raw HMAC key of a "whsec_..." secret.
func DecodeSecret(secret string) ([]byte, error) {
	raw, ok := strings.CutPrefix(secret, SecretPrefix)
	if !ok {
		return nil, fmt.Errorf("webhook: secret must start with %q", SecretPrefix)
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("webhook: secret is not valid base64: %w", err)
	}
	if len(key) < 16 {
		return nil, errors.New("webhook: secret key must be at least 16 bytes")
	}
	return key, nil
}

func sign(key []byte, msgID string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(msgID))
	mac.Write([]byte{'.'})
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// Sign returns the webhook-signature header value for body, with one
// signature per secret. Signing with several secrets is how rotation works:
// during the grace period receivers holding either the old or the new
// secret can verify.
func Sign(msgID string, ts time.Time, body []byte, secrets ...string) (string, error) {
	if len(secrets) == 0 {
		return "", errors.New("webhook: at least one secret is required")
	}
	sigs := make([]string, 0, len(secrets))
	for _, s := range secrets {
		key, err := DecodeSecret(s)
		if err != nil {
			return "", err
		}
		sigs = append(sigs, sign(key, msgID, ts.Unix(), body))
	}
	return strings.Join(sigs, " "), nil
}

// SetHeaders signs body and sets all three webhook headers on h.
func SetHeaders(h http.Header, msgID string, ts time.Time, body []byte, secrets ...string) error {
	sig, err := Sign(msgID, ts, body, secrets...)
	if err != nil {
		return err
	}
	h.Set(HeaderID, msgID)
	h.Set(HeaderTimestamp, strconv.FormatInt(ts.Unix(), 10))
	h.Set(HeaderSignature, sig)
	return nil
}

// Verifier checks incoming webhooks.
type Verifier struct {
	keys      [][]byte
	Tolerance time.Duration
	// Now returns the current time; override in tests.
	Now func() time.Time
}

// NewVerifier accepts one or more secrets (several during a rotation).
func NewVerifier(secrets ...string) (*Verifier, error) {
	if len(secrets) == 0 {
		return nil, errors.New("webhook: at least one secret is required")
	}
	v := &Verifier{Tolerance: DefaultTolerance, Now: time.Now}
	for _, s := range secrets {
		key, err := DecodeSecret(s)
		if err != nil {
			return nil, err
		}
		v.keys = append(v.keys, key)
	}
	return v, nil
}

// Verify checks the headers and body of a webhook request. body must be the
// raw bytes received: re-encoding parsed JSON would change them.
func (v *Verifier) Verify(h http.Header, body []byte) error {
	msgID, tsRaw, sigHeader := h.Get(HeaderID), h.Get(HeaderTimestamp), h.Get(HeaderSignature)
	if msgID == "" || tsRaw == "" || sigHeader == "" {
		return ErrMissingHeaders
	}
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return ErrInvalidTimestamp
	}
	now := v.Now()
	sent := time.Unix(ts, 0)
	if now.Sub(sent) > v.Tolerance {
		return ErrTimestampTooOld
	}
	if sent.Sub(now) > v.Tolerance {
		return ErrTimestampTooNew
	}

	for _, key := range v.keys {
		expected := sign(key, msgID, ts, body)
		for _, candidate := range strings.Fields(sigHeader) {
			// Constant-time comparison: never leak how many bytes matched.
			if hmac.Equal([]byte(candidate), []byte(expected)) {
				return nil
			}
		}
	}
	return ErrNoMatch
}
