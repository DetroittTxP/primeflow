package authn

import (
	"strings"
	"testing"
	"time"
)

func TestPasswordRoundTrip(t *testing.T) {
	h := HashPassword("correct horse battery staple")
	if !strings.HasPrefix(h, "pbkdf2_sha256$") {
		t.Fatalf("unexpected hash format: %s", h)
	}
	ok, err := VerifyPassword(h, "correct horse battery staple")
	if err != nil || !ok {
		t.Fatalf("verify good password: ok=%v err=%v", ok, err)
	}
	ok, err = VerifyPassword(h, "wrong password")
	if err != nil || ok {
		t.Fatalf("verify wrong password should fail cleanly: ok=%v err=%v", ok, err)
	}
}

func TestPasswordSaltIsRandom(t *testing.T) {
	if HashPassword("x") == HashPassword("x") {
		t.Fatal("two hashes of the same password must differ (per-hash salt)")
	}
}

func TestVerifyRejectsMalformed(t *testing.T) {
	for _, bad := range []string{"", "plaintext", "pbkdf2_sha256$notanumber$a$b", "md5$1$a$b"} {
		if ok, err := VerifyPassword(bad, "x"); ok || err == nil {
			t.Fatalf("expected malformed error for %q, got ok=%v err=%v", bad, ok, err)
		}
	}
}

func TestRoleCapabilities(t *testing.T) {
	if RoleViewer.CanMutate() || RoleViewer.CanAdmin() {
		t.Fatal("viewer must not mutate or admin")
	}
	if !RoleOperator.CanMutate() || RoleOperator.CanAdmin() {
		t.Fatal("operator mutates but does not admin")
	}
	if !RoleAdmin.CanMutate() || !RoleAdmin.CanAdmin() {
		t.Fatal("admin does everything")
	}
	if ValidRole("root") || !ValidRole("operator") {
		t.Fatal("ValidRole wrong")
	}
}

func TestThrottleLocksAfterCeiling(t *testing.T) {
	tr := &Throttle{MaxFailures: 3, LockFor: time.Minute, entries: map[string]*throttleEntry{}}
	key := "user@x|10.0.0.1"
	for i := 0; i < 2; i++ {
		tr.Fail(key)
		if locked, _ := tr.Locked(key); locked {
			t.Fatalf("locked too early after %d failures", i+1)
		}
	}
	tr.Fail(key) // third failure trips the lock
	if locked, d := tr.Locked(key); !locked || d <= 0 {
		t.Fatalf("expected lock after ceiling, got locked=%v d=%v", locked, d)
	}
	tr.Reset(key)
	if locked, _ := tr.Locked(key); locked {
		t.Fatal("Reset should clear the lock")
	}
}
