// Package secrets encrypts endpoint signing secrets at rest.
//
// We must be able to read signing secrets back (we sign every request with
// them), so they cannot be hashed like passwords. Instead they are sealed
// with AES-256-GCM under a key that lives outside the database
// (WEBEVENT_SECRET_KEY). A database dump or a leaked backup alone then
// reveals nothing usable.
//
// Stored format: "enc:v1:" + base64(nonce || ciphertext || tag).
// Values without that prefix are treated as plaintext, which lets you turn
// encryption on for an existing installation: old rows keep working and new
// or rotated secrets are encrypted.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

const prefix = "enc:v1:"

// Box seals and opens secrets. The zero value (nil key) stores plaintext.
type Box struct {
	aead cipher.AEAD
}

// NewBox builds a Box from a base64-encoded 32-byte key. An empty key gives a
// Box that stores secrets in plaintext (acceptable only for development).
func NewBox(b64Key string) (*Box, error) {
	if b64Key == "" {
		return &Box{}, nil
	}
	key, err := base64.StdEncoding.DecodeString(b64Key)
	if err != nil {
		return nil, fmt.Errorf("secret key is not valid base64: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("secret key must decode to 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Box{aead: aead}, nil
}

// Encrypting reports whether the box actually encrypts.
func (b *Box) Encrypting() bool { return b != nil && b.aead != nil }

// Seal encrypts plaintext (or returns it unchanged without a key).
func (b *Box) Seal(plaintext string) (string, error) {
	if !b.Encrypting() {
		return plaintext, nil
	}
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := b.aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return prefix + base64.StdEncoding.EncodeToString(sealed), nil
}

// Open decrypts a value produced by Seal. Plaintext values pass through.
func (b *Box) Open(stored string) (string, error) {
	raw, encrypted := strings.CutPrefix(stored, prefix)
	if !encrypted {
		return stored, nil
	}
	if !b.Encrypting() {
		return "", errors.New("secret is encrypted but WEBEVENT_SECRET_KEY is not set")
	}
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return "", fmt.Errorf("decode sealed secret: %w", err)
	}
	ns := b.aead.NonceSize()
	if len(data) < ns {
		return "", errors.New("sealed secret is too short")
	}
	plain, err := b.aead.Open(nil, data[:ns], data[ns:], nil)
	if err != nil {
		return "", errors.New("cannot decrypt secret: wrong WEBEVENT_SECRET_KEY or corrupted value")
	}
	return string(plain), nil
}
