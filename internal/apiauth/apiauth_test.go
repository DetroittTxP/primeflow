package apiauth

import (
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSecretRoundTrip(t *testing.T) {
	full, prefix, hash := NewSecret()
	if !strings.HasPrefix(full, "pmx_") {
		t.Fatalf("secret missing label: %s", full)
	}
	if PrefixOf(full) != prefix {
		t.Fatalf("PrefixOf(%q) = %q, want %q", full, PrefixOf(full), prefix)
	}
	if !SecretMatches(hash, full) {
		t.Fatal("SecretMatches should accept the original secret")
	}
	if SecretMatches(hash, full+"x") {
		t.Fatal("SecretMatches must reject a tampered secret")
	}
	if PrefixOf("nope") != "" {
		t.Fatal("PrefixOf should return empty for a non-key string")
	}
}

func TestRolesEnforceScopes(t *testing.T) {
	ro, ok := LookupRole("api-readonly")
	if !ok {
		t.Fatal("api-readonly missing")
	}
	if !ro.Has(ScopeReadRuns) || ro.Has(ScopeWriteRuns) {
		t.Fatal("api-readonly scope set wrong")
	}
	admin, _ := LookupRole("api-admin")
	if !admin.Has(ScopeWriteQueues) {
		t.Fatal("api-admin should carry write:queues")
	}
	if ValidRole("api-superuser") {
		t.Fatal("unknown role reported valid")
	}
	if len(Roles()) == 0 || Roles()[0].ID != "api-admin" {
		t.Fatal("Roles() should be privilege-ordered with api-admin first")
	}
}

func TestClientIPHonoursTrustedProxy(t *testing.T) {
	trusted := ParseCIDRs("10.0.0.0/8")
	r := &http.Request{RemoteAddr: "10.1.2.3:5555", Header: http.Header{}}
	r.Header.Set("X-Forwarded-For", "203.0.113.9, 10.9.9.9")
	if got := ClientIP(r, trusted); got.String() != "203.0.113.9" {
		t.Fatalf("via trusted proxy: got %v, want 203.0.113.9", got)
	}

	// An untrusted peer: the header is ignored, the peer wins.
	r2 := &http.Request{RemoteAddr: "198.51.100.7:40000", Header: http.Header{}}
	r2.Header.Set("X-Forwarded-For", "203.0.113.9")
	if got := ClientIP(r2, trusted); got.String() != "198.51.100.7" {
		t.Fatalf("untrusted peer: got %v, want 198.51.100.7", got)
	}
}

func TestIPInAny(t *testing.T) {
	nets := ParseCIDRs("192.168.1.0/24, 10.0.0.5")
	if !IPInAny(net.ParseIP("192.168.1.42"), nets) || !IPInAny(net.ParseIP("10.0.0.5"), nets) {
		t.Fatal("expected addresses to be in the allowlist")
	}
	if IPInAny(net.ParseIP("192.168.2.1"), nets) {
		t.Fatal("address outside the allowlist reported inside")
	}
}

func TestRateLimiter(t *testing.T) {
	l := NewRateLimiter()
	// 60/min == 1/sec, burst 60: the first 60 calls pass, the 61st does not.
	for i := 0; i < 60; i++ {
		if ok, _ := l.Allow("k", 60); !ok {
			t.Fatalf("call %d unexpectedly limited", i+1)
		}
	}
	if ok, retry := l.Allow("k", 60); ok || retry <= 0 {
		t.Fatalf("expected the burst to be exhausted, got ok=%v retry=%v", ok, retry)
	}
	// A zero limit means unlimited.
	for i := 0; i < 1000; i++ {
		if ok, _ := l.Allow("z", 0); !ok {
			t.Fatal("zero limit should never block")
		}
	}
	_ = time.Second
}
