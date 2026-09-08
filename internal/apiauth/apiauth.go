// Package apiauth is the External API authorization toolkit: the role/scope
// catalogue, the route→scope map, API-key secret generation, trusted-proxy
// client-IP resolution, and a per-key rate limiter.
//
// Like internal/authn it has no store or HTTP dependency and is unit-testable
// without a database.
package apiauth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ------------------------------------------------------------ roles & scopes ---

// Scope is a single capability an API-key role may carry.
type Scope string

const (
	ScopeReadRuns         Scope = "read:runs"
	ScopeReadDeployments  Scope = "read:deployments"
	ScopeReadQueues       Scope = "read:queues"
	ScopeReadEvents       Scope = "read:events"
	ScopeReadWorkers      Scope = "read:workers"
	ScopeReadWorkerSpecs  Scope = "read:worker-specs"
	ScopeWriteRuns        Scope = "write:runs"
	ScopeWriteDeployments Scope = "write:deployments"
	ScopeWriteQueues      Scope = "write:queues"
	ScopeWriteWorkerSpecs Scope = "write:worker-specs"
)

// Role is a named, fixed bundle of scopes. Roles are defined in code, not the
// database, so the set an operator can choose from always matches what the
// server actually enforces.
type Role struct {
	ID          string  `json:"id"`
	Label       string  `json:"label"`
	Description string  `json:"description"`
	Scopes      []Scope `json:"scopes"`
}

// Has reports whether the role carries scope s.
func (r Role) Has(s Scope) bool {
	for _, x := range r.Scopes {
		if x == s {
			return true
		}
	}
	return false
}

var roles = map[string]Role{
	"api-admin": {
		ID: "api-admin", Label: "API Administrator",
		Description: "Full read and write across runs, deployments and queues.",
		Scopes: []Scope{
			ScopeReadRuns, ScopeReadDeployments, ScopeReadQueues, ScopeReadEvents, ScopeReadWorkers,
			ScopeReadWorkerSpecs,
			ScopeWriteRuns, ScopeWriteDeployments, ScopeWriteQueues, ScopeWriteWorkerSpecs,
		},
	},
	"api-trigger": {
		ID: "api-trigger", Label: "API Trigger",
		Description: "Trigger deployments and read the runs they produce. For upstream systems that kick off work.",
		Scopes:      []Scope{ScopeReadDeployments, ScopeReadRuns, ScopeWriteRuns},
	},
	"api-worker": {
		ID: "api-worker", Label: "Worker",
		Description: "Claims work from its own pools and reports on it. For workers that reach the API instead of the database — a remote site, or anywhere a database credential should not go.",
		Scopes: []Scope{
			ScopeWorkerLease, ScopeWorkerReport, ScopeReadRuns, ScopeReadDeployments,
		},
	},
	"api-readonly": {
		ID: "api-readonly", Label: "API Read-Only",
		Description: "BI, analytics and audit pipelines. Reads every collection; mutates nothing.",
		Scopes:      []Scope{ScopeReadRuns, ScopeReadDeployments, ScopeReadQueues, ScopeReadEvents, ScopeReadWorkers},
	},
}

// LookupRole returns the role with the given id.
func LookupRole(id string) (Role, bool) { r, ok := roles[id]; return r, ok }

// ValidRole reports whether id names a known API-key role.
func ValidRole(id string) bool { _, ok := roles[id]; return ok }

// Roles returns the whole catalogue, ordered from most to least privileged, for
// the console's key form and API Explorer.
func Roles() []Role {
	out := make([]Role, 0, len(roles))
	for _, r := range roles {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return len(out[i].Scopes) > len(out[j].Scopes) })
	return out
}

// Route is one External API endpoint and the scope it requires. An empty Scope
// means "any valid key" (used for the health probe).
type Route struct {
	Method string `json:"method"`
	Path   string `json:"path"`
	Scope  Scope  `json:"scope,omitempty"`
}

// Routes is the single source of truth the external router, the auth middleware
// and the API Explorer all read. Paths are relative to /api/external/v1.
var Routes = []Route{
	{Method: "GET", Path: "/health"},
	{Method: "GET", Path: "/runs", Scope: ScopeReadRuns},
	{Method: "POST", Path: "/runs", Scope: ScopeWriteRuns},
	{Method: "GET", Path: "/runs/{id}", Scope: ScopeReadRuns},
	{Method: "GET", Path: "/runs/{id}/tasks", Scope: ScopeReadRuns},
	{Method: "GET", Path: "/runs/{id}/logs", Scope: ScopeReadRuns},
	{Method: "GET", Path: "/runs/{id}/artifacts", Scope: ScopeReadRuns},
	{Method: "GET", Path: "/deployments", Scope: ScopeReadDeployments},
	{Method: "GET", Path: "/deployments/{id}", Scope: ScopeReadDeployments},
	{Method: "POST", Path: "/deployments/{id}/run", Scope: ScopeWriteRuns},
	{Method: "GET", Path: "/queues", Scope: ScopeReadQueues},
	{Method: "POST", Path: "/queues", Scope: ScopeWriteQueues},
	{Method: "GET", Path: "/queues/{name}/pending", Scope: ScopeReadQueues},
	{Method: "GET", Path: "/workers", Scope: ScopeReadWorkers},
	{Method: "GET", Path: "/worker-specs", Scope: ScopeReadWorkerSpecs},
	{Method: "GET", Path: "/worker-specs/{id}", Scope: ScopeReadWorkerSpecs},
	{Method: "POST", Path: "/worker-specs", Scope: ScopeWriteWorkerSpecs},
	{Method: "DELETE", Path: "/worker-specs/{id}", Scope: ScopeWriteWorkerSpecs},
	{Method: "POST", Path: "/worker-specs/{id}/sync", Scope: ScopeWriteWorkerSpecs},
	{Method: "GET", Path: "/events", Scope: ScopeReadEvents},
}

// --------------------------------------------------------------- secrets ---

// SecretPrefixLabel is the human-facing marker on every issued key.
const SecretPrefixLabel = "pmx_"

// NewSecret mints an API-key secret. It returns the full secret (shown to the
// operator exactly once), the lookup prefix stored in the clear, and the hex
// SHA-256 of the full secret stored for verification.
//
// Layout: "pmx_" + 8 hex prefix chars + 48 hex random chars. 24 random bytes is
// 192 bits, so the fast hash below is the right tool — there is nothing to brute.
func NewSecret() (full, prefix, hash string) {
	p := make([]byte, 4)
	body := make([]byte, 24)
	if _, err := rand.Read(p); err != nil {
		panic("apiauth: crypto/rand failed: " + err.Error())
	}
	if _, err := rand.Read(body); err != nil {
		panic("apiauth: crypto/rand failed: " + err.Error())
	}
	prefix = hex.EncodeToString(p)
	full = SecretPrefixLabel + prefix + hex.EncodeToString(body)
	return full, prefix, HashSecret(full)
}

// HashSecret returns the hex SHA-256 of a full secret.
func HashSecret(full string) string {
	sum := sha256.Sum256([]byte(full))
	return hex.EncodeToString(sum[:])
}

// SecretMatches compares a presented secret against a stored hash in constant time.
func SecretMatches(storedHash, presented string) bool {
	return subtle.ConstantTimeCompare([]byte(storedHash), []byte(HashSecret(presented))) == 1
}

// PrefixOf extracts the lookup prefix from a presented secret, or "" if it is not
// shaped like one of our keys.
func PrefixOf(presented string) string {
	if !strings.HasPrefix(presented, SecretPrefixLabel) {
		return ""
	}
	rest := presented[len(SecretPrefixLabel):]
	if len(rest) < 8 {
		return ""
	}
	return rest[:8]
}

// ----------------------------------------------------------- client IP ---

// ParseCIDRs turns a comma-separated list of IPs and CIDR blocks into networks.
// A bare IP becomes a /32 or /128. Unparseable entries are skipped.
func ParseCIDRs(csv string) []*net.IPNet {
	var out []*net.IPNet
	for _, part := range strings.Split(csv, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "/") {
			if ip := net.ParseIP(part); ip != nil {
				bits := 32
				if ip.To4() == nil {
					bits = 128
				}
				part += "/" + strconv.Itoa(bits)
			}
		}
		if _, n, err := net.ParseCIDR(part); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// IPInAny reports whether ip falls inside any of nets.
func IPInAny(ip net.IP, nets []*net.IPNet) bool {
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// ClientIP resolves the real caller address. When the immediate peer is a
// trusted proxy, the right-most X-Forwarded-For entry that is not itself a
// trusted proxy is taken; otherwise the peer address is used as-is. This is the
// only safe way to honour a per-key IP allowlist behind an ingress.
func ClientIP(r *http.Request, trusted []*net.IPNet) net.IP {
	peerStr, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		peerStr = r.RemoteAddr
	}
	peer := net.ParseIP(strings.TrimSpace(peerStr))
	if peer == nil || len(trusted) == 0 || !IPInAny(peer, trusted) {
		return peer
	}
	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return peer
	}
	parts := strings.Split(xff, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		cand := net.ParseIP(strings.TrimSpace(parts[i]))
		if cand == nil {
			continue
		}
		if !IPInAny(cand, trusted) {
			return cand
		}
	}
	return peer
}

// ViaTrustedProxy reports whether the request's immediate peer is a trusted
// proxy — the precondition for believing any proxy-set header (mTLS verification,
// forwarded client cert state).
func ViaTrustedProxy(r *http.Request, trusted []*net.IPNet) bool {
	if len(trusted) == 0 {
		return false
	}
	peerStr, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		peerStr = r.RemoteAddr
	}
	peer := net.ParseIP(strings.TrimSpace(peerStr))
	return peer != nil && IPInAny(peer, trusted)
}

// ------------------------------------------------------------ rate limiter ---

// RateLimiter is a per-key token bucket, refilled continuously at rate
// tokens/minute up to a burst ceiling of the same size. In-process by design;
// see the package doc.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens float64
	rate   float64 // tokens per second
	burst  float64
	last   time.Time
}

// NewRateLimiter returns an empty limiter.
func NewRateLimiter() *RateLimiter { return &RateLimiter{buckets: map[string]*bucket{}} }

// Allow consumes one token for key, provisioning or reconfiguring its bucket to
// perMinute. It returns whether the call is allowed and, if not, how long until a
// token frees up.
func (l *RateLimiter) Allow(key string, perMinute int) (bool, time.Duration) {
	if perMinute <= 0 {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	rate := float64(perMinute) / 60.0
	burst := float64(perMinute)

	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: burst, rate: rate, burst: burst, last: now}
		l.buckets[key] = b
	}
	// Adopt a changed limit without punishing the caller.
	if b.burst != burst {
		b.rate, b.burst = rate, burst
		if b.tokens > burst {
			b.tokens = burst
		}
	}
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now

	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	wait := time.Duration((1-b.tokens)/b.rate*float64(time.Second)) + time.Millisecond
	return false, wait
}

// Forget drops a key's bucket, called when a key is deleted or rotated.
func (l *RateLimiter) Forget(key string) {
	l.mu.Lock()
	delete(l.buckets, key)
	l.mu.Unlock()
}
