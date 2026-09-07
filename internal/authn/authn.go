// Package authn is the operator-side authentication toolkit: password hashing,
// the operator role model, and a small in-process login throttle.
//
// It has no dependency on the store or the HTTP layer so it can be unit-tested
// without a database.
package authn

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ------------------------------------------------------------------ roles ---

// Role is an operator's capability level. The zero value is the empty string,
// which is not a valid role — callers must set one explicitly.
type Role string

const (
	// RoleViewer can read every /api/v1 GET but mutate nothing.
	RoleViewer Role = "viewer"
	// RoleOperator can do everything an operator does day to day: trigger,
	// cancel, retry, reprioritise, pause queues, edit deployments and
	// automations. It cannot manage users, API keys or instance settings.
	RoleOperator Role = "operator"
	// RoleAdmin adds user, API-key and settings management.
	RoleAdmin Role = "admin"
)

// ValidRole reports whether s names a known role.
func ValidRole(s string) bool {
	switch Role(s) {
	case RoleViewer, RoleOperator, RoleAdmin:
		return true
	}
	return false
}

// CanMutate reports whether the role may perform state-changing /api/v1 calls.
func (r Role) CanMutate() bool { return r == RoleOperator || r == RoleAdmin }

// CanAdmin reports whether the role may manage users, API keys and settings.
func (r Role) CanAdmin() bool { return r == RoleAdmin }

// --------------------------------------------------------------- passwords ---

// Password hashing uses PBKDF2-HMAC-SHA256 from the standard library. The stored
// form is Django-compatible so it is easy to eyeball and to migrate:
//
//	pbkdf2_sha256$<iterations>$<salt-b64>$<hash-b64>
const (
	pbkdf2Iterations = 600_000
	pbkdf2KeyLength  = 32
	pbkdf2SaltLength = 16
	pbkdf2Prefix     = "pbkdf2_sha256"
)

// ErrMalformedHash is returned by VerifyPassword when the stored hash is not in
// the expected format.
var ErrMalformedHash = errors.New("authn: malformed password hash")

// HashPassword derives a storable hash for pw. It panics only if the system CSPRNG
// fails, which Go treats as unrecoverable anyway.
func HashPassword(pw string) string {
	salt := make([]byte, pbkdf2SaltLength)
	if _, err := rand.Read(salt); err != nil {
		panic("authn: crypto/rand failed: " + err.Error())
	}
	dk, err := pbkdf2.Key(sha256.New, pw, salt, pbkdf2Iterations, pbkdf2KeyLength)
	if err != nil {
		panic("authn: pbkdf2 failed: " + err.Error())
	}
	return fmt.Sprintf("%s$%d$%s$%s", pbkdf2Prefix, pbkdf2Iterations,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(dk))
}

// VerifyPassword reports whether pw matches the stored hash, in constant time
// with respect to the derived key.
func VerifyPassword(stored, pw string) (bool, error) {
	parts := strings.Split(stored, "$")
	if len(parts) != 4 || parts[0] != pbkdf2Prefix {
		return false, ErrMalformedHash
	}
	iter, err := strconv.Atoi(parts[1])
	if err != nil || iter <= 0 {
		return false, ErrMalformedHash
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil {
		return false, ErrMalformedHash
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil {
		return false, ErrMalformedHash
	}
	got, err := pbkdf2.Key(sha256.New, pw, salt, iter, len(want))
	if err != nil {
		return false, ErrMalformedHash
	}
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

// ------------------------------------------------------------- login throttle ---

// Throttle is a per-process brute-force guard: after MaxFailures failed attempts
// for the same key within the window it locks that key for LockFor. It is
// intentionally in-memory — a shared limiter would need Redis, and the blast
// radius of a per-replica guard is acceptable for an operator console.
type Throttle struct {
	MaxFailures int
	LockFor     time.Duration

	mu      sync.Mutex
	entries map[string]*throttleEntry
}

type throttleEntry struct {
	failures  int
	lockUntil time.Time
	last      time.Time
}

// NewThrottle returns a throttle with sensible defaults (5 failures, 15 min lock).
func NewThrottle() *Throttle {
	return &Throttle{MaxFailures: 5, LockFor: 15 * time.Minute, entries: map[string]*throttleEntry{}}
}

// Locked reports whether key is currently locked out, and for how long.
func (t *Throttle) Locked(key string) (bool, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[key]
	if e == nil {
		return false, 0
	}
	if d := time.Until(e.lockUntil); d > 0 {
		return true, d
	}
	return false, 0
}

// Fail records a failed attempt for key and locks it once the ceiling is hit.
func (t *Throttle) Fail(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.gcLocked()
	e := t.entries[key]
	if e == nil {
		e = &throttleEntry{}
		t.entries[key] = e
	}
	e.failures++
	e.last = time.Now()
	if e.failures >= t.MaxFailures {
		e.lockUntil = time.Now().Add(t.LockFor)
		e.failures = 0
	}
}

// Reset clears any failure count for key, called after a successful login.
func (t *Throttle) Reset(key string) {
	t.mu.Lock()
	delete(t.entries, key)
	t.mu.Unlock()
}

// gcLocked drops entries untouched for an hour so the map cannot grow without
// bound under a spray of distinct emails. Caller holds t.mu.
func (t *Throttle) gcLocked() {
	if len(t.entries) < 1024 {
		return
	}
	cutoff := time.Now().Add(-time.Hour)
	for k, e := range t.entries {
		if e.last.Before(cutoff) && e.lockUntil.Before(time.Now()) {
			delete(t.entries, k)
		}
	}
}
