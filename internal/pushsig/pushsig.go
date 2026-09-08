// Package pushsig is the HMAC contract between a push work pool's dispatcher
// and the receiver that executes its runs.
//
// It is its own package for one reason: the receiver lives in a worker binary,
// and the dispatcher lives in the scheduler. Leaving Verify in the scheduler
// meant every worker linked the cron parser and the embedded time-zone database
// behind it, to reach twenty lines of HMAC.
package pushsig

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// Header is the request header the signature travels in.
const Header = "X-PrimeFlow-Signature"

const prefix = "sha256="

// Sign returns the "sha256=<hex>" value for body under secret.
func Sign(secret string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return prefix + hex.EncodeToString(m.Sum(nil))
}

// Verify checks a signature header against body.
//
// An empty secret accepts unconditionally: a pool with no secret configured is
// declaring that its dispatches are authenticated by the network instead.
func Verify(secret, header string, body []byte) bool {
	if secret == "" {
		return true
	}
	if len(header) <= len(prefix) || header[:len(prefix)] != prefix {
		return false
	}
	want, err := hex.DecodeString(header[len(prefix):])
	if err != nil {
		return false
	}
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return hmac.Equal(want, m.Sum(nil))
}
