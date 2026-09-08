package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/internal/store/postgres"
)

func resetAuth(t *testing.T, st *postgres.Store) {
	t.Helper()
	if _, err := st.DB().ExecContext(context.Background(),
		`TRUNCATE pf_api_key_events, pf_api_keys, pf_sessions, pf_password_resets, pf_users RESTART IDENTITY CASCADE;
		 UPDATE pf_settings SET value = '{"enabled": false, "default_rate_limit_per_min": 600}'
		 WHERE key = 'external_api';`); err != nil {
		t.Fatalf("reset auth tables: %v", err)
	}
}

func TestPasswordResetTokens(t *testing.T) {
	st := newStore(t)
	resetAuth(t, st)
	ctx := context.Background()

	u, err := st.CreateUser(ctx, store.UserInput{
		ID: uuid.NewString(), Email: "reset@x", PasswordHash: "h", Role: "operator", Active: true,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if u.AuthProvider != "local" {
		t.Fatalf("default auth_provider = %q, want local", u.AuthProvider)
	}

	tok, expires, err := st.CreatePasswordReset(ctx, u.ID, time.Hour)
	if err != nil || tok == "" || !expires.After(time.Now()) {
		t.Fatalf("CreatePasswordReset: tok=%q exp=%v err=%v", tok, expires, err)
	}

	// Consume once → returns the user id.
	got, err := st.ConsumePasswordReset(ctx, tok)
	if err != nil || got != u.ID {
		t.Fatalf("ConsumePasswordReset: got=%q err=%v", got, err)
	}
	// Second use → gone.
	if _, err := st.ConsumePasswordReset(ctx, tok); err != store.ErrNotFound {
		t.Fatalf("token must be single-use, got %v", err)
	}
	// Unknown token → not found.
	if _, err := st.ConsumePasswordReset(ctx, "nope"); err != store.ErrNotFound {
		t.Fatalf("unknown token: %v", err)
	}

	// Expired token is rejected and swept.
	old, _, _ := st.CreatePasswordReset(ctx, u.ID, -time.Minute)
	if _, err := st.ConsumePasswordReset(ctx, old); err != store.ErrNotFound {
		t.Fatalf("expired token consumed: %v", err)
	}
	if n, err := st.DeleteExpiredPasswordResets(ctx, time.Now().UTC()); err != nil || n < 1 {
		t.Fatalf("DeleteExpiredPasswordResets: n=%d err=%v", n, err)
	}

	// A JIT-provisioned SSO user round-trips its provider.
	sso, err := st.CreateUser(ctx, store.UserInput{
		ID: uuid.NewString(), Email: "sso@x", PasswordHash: "unusable", Role: "viewer",
		Active: true, AuthProvider: "oidc",
	})
	if err != nil || sso.AuthProvider != "oidc" {
		t.Fatalf("oidc user: %+v err=%v", sso, err)
	}
}

func TestUserLifecycleAndLogin(t *testing.T) {
	st := newStore(t)
	resetAuth(t, st)
	ctx := context.Background()

	if n, _ := st.CountUsers(ctx); n != 0 {
		t.Fatalf("fresh table should have no users, got %d", n)
	}

	u, err := st.CreateUser(ctx, store.UserInput{
		ID: uuid.NewString(), Email: "Ops@PrimeX", PasswordHash: "hash-1",
		Role: "operator", Active: true,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Case-insensitive email uniqueness.
	if _, err := st.CreateUser(ctx, store.UserInput{
		ID: uuid.NewString(), Email: "ops@primex", PasswordHash: "hash-2", Role: "viewer", Active: true,
	}); err == nil {
		t.Fatal("expected a conflict on a case-differing duplicate email")
	}

	got, hash, err := st.GetUserAuth(ctx, "ops@primex")
	if err != nil || got.ID != u.ID || hash != "hash-1" {
		t.Fatalf("GetUserAuth: user=%v hash=%q err=%v", got, hash, err)
	}

	// Role change + password change.
	newRole := "admin"
	if _, err := st.UpdateUser(ctx, u.ID, &newRole, nil, "hash-3"); err != nil {
		t.Fatalf("update: %v", err)
	}
	_, hash, _ = st.GetUserAuth(ctx, "ops@primex")
	if hash != "hash-3" {
		t.Fatalf("password not updated, hash=%q", hash)
	}
	after, _ := st.GetUser(ctx, u.ID)
	if after.Role != "admin" {
		t.Fatalf("role not updated: %s", after.Role)
	}
}

func TestSessionExpiryAndInactiveUser(t *testing.T) {
	st := newStore(t)
	resetAuth(t, st)
	ctx := context.Background()

	u, _ := st.CreateUser(ctx, store.UserInput{
		ID: uuid.NewString(), Email: "s@x", PasswordHash: "h", Role: "viewer", Active: true,
	})

	live := &core.Session{ID: "live-session", UserID: u.ID, ExpiresAt: time.Now().Add(time.Hour)}
	stale := &core.Session{ID: "stale-session", UserID: u.ID, ExpiresAt: time.Now().Add(-time.Minute)}
	if err := st.CreateSession(ctx, live); err != nil {
		t.Fatalf("create live: %v", err)
	}
	if err := st.CreateSession(ctx, stale); err != nil {
		t.Fatalf("create stale: %v", err)
	}

	now := time.Now().UTC()
	if _, _, err := st.GetSession(ctx, "live-session", now); err != nil {
		t.Fatalf("live session should resolve: %v", err)
	}
	if _, _, err := st.GetSession(ctx, "stale-session", now); err != store.ErrNotFound {
		t.Fatalf("expired session should be ErrNotFound, got %v", err)
	}

	// Deactivating the user invalidates the still-unexpired session.
	no := false
	if _, err := st.UpdateUser(ctx, u.ID, nil, &no, ""); err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if _, _, err := st.GetSession(ctx, "live-session", now); err != store.ErrNotFound {
		t.Fatalf("session of an inactive user must not resolve, got %v", err)
	}

	if n, err := st.DeleteExpiredSessions(ctx, now); err != nil || n < 1 {
		t.Fatalf("DeleteExpiredSessions: n=%d err=%v", n, err)
	}
}

func TestAPIKeyCreateLookupRotate(t *testing.T) {
	st := newStore(t)
	resetAuth(t, st)
	ctx := context.Background()

	rl := 120
	k := &core.APIKey{
		ID: uuid.NewString(), Name: "billing sync", OwnerEmail: "bi@x",
		Prefix: "abcd1234", SecretHash: "hash-of-secret", Role: "api-readonly", Active: true,
		RateLimitPerMin: &rl, IPAllowlist: []string{"203.0.113.0/24"}, RedactPII: true,
	}
	if err := st.CreateAPIKey(ctx, k); err != nil {
		t.Fatalf("create key: %v", err)
	}

	got, err := st.GetAPIKeyByPrefix(ctx, "abcd1234")
	if err != nil {
		t.Fatalf("by prefix: %v", err)
	}
	if got.SecretHash != "hash-of-secret" || got.Role != "api-readonly" ||
		got.RateLimitPerMin == nil || *got.RateLimitPerMin != 120 ||
		len(got.IPAllowlist) != 1 || !got.RedactPII {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	// Rotation swaps prefix + hash; the old prefix stops resolving.
	if err := st.SetAPIKeySecret(ctx, k.ID, "ffff9999", "new-hash"); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if _, err := st.GetAPIKeyByPrefix(ctx, "abcd1234"); err != store.ErrNotFound {
		t.Fatalf("old prefix should be gone, got %v", err)
	}
	rot, _ := st.GetAPIKeyByPrefix(ctx, "ffff9999")
	if rot.SecretHash != "new-hash" {
		t.Fatalf("hash not rotated: %s", rot.SecretHash)
	}

	// Filtering.
	inactive := false
	if ks, _ := st.ListAPIKeys(ctx, store.APIKeyFilter{Active: &inactive}); len(ks) != 0 {
		t.Fatalf("no inactive keys expected, got %d", len(ks))
	}
	if ks, _ := st.ListAPIKeys(ctx, store.APIKeyFilter{Role: "api-readonly"}); len(ks) != 1 {
		t.Fatalf("role filter: got %d", len(ks))
	}
	if ks, _ := st.ListAPIKeys(ctx, store.APIKeyFilter{Search: "billing"}); len(ks) != 1 {
		t.Fatalf("search filter: got %d", len(ks))
	}
}

func TestAPIKeyEventsTrim(t *testing.T) {
	st := newStore(t)
	resetAuth(t, st)
	ctx := context.Background()

	k := &core.APIKey{ID: uuid.NewString(), Name: "k", Prefix: "11112222", SecretHash: "h", Role: "api-admin", Active: true}
	if err := st.CreateAPIKey(ctx, k); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := st.AppendAPIKeyEvent(ctx, &core.APIKeyEvent{APIKeyID: k.ID, Actor: "t", Action: "auth-denied"}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	evs, _ := st.ListAPIKeyEvents(ctx, k.ID, 100)
	if len(evs) != 10 || evs[0].ID < evs[len(evs)-1].ID {
		t.Fatalf("expected 10 events newest-first, got %d", len(evs))
	}
	if n, err := st.TrimAPIKeyEvents(ctx, 4); err != nil || n != 6 {
		t.Fatalf("trim: n=%d err=%v", n, err)
	}
	if evs, _ := st.ListAPIKeyEvents(ctx, k.ID, 100); len(evs) != 4 {
		t.Fatalf("after trim expected 4, got %d", len(evs))
	}
}

func TestExternalAPISettingsRoundTrip(t *testing.T) {
	st := newStore(t)
	resetAuth(t, st)
	ctx := context.Background()

	cfg, err := st.GetExternalAPISettings(ctx)
	if err != nil || cfg.Enabled {
		t.Fatalf("default should be disabled: %+v err=%v", cfg, err)
	}
	if err := st.PutExternalAPISettings(ctx, core.ExternalAPISettings{Enabled: true, DefaultRateLimitPerMin: 300}); err != nil {
		t.Fatalf("put: %v", err)
	}
	cfg, _ = st.GetExternalAPISettings(ctx)
	if !cfg.Enabled || cfg.DefaultRateLimitPerMin != 300 {
		t.Fatalf("round-trip: %+v", cfg)
	}
}
