package core

import (
	"encoding/json"
	"time"
)

// User is an operator account. Password material never leaves the store layer,
// so it is not a field here.
type User struct {
	ID     string `json:"id"`
	Email  string `json:"email"`
	Role   string `json:"role"` // authn.Role: admin | operator | viewer
	Active bool   `json:"active"`
	// AuthProvider is "local" (password) or "oidc" (SSO, JIT-provisioned).
	AuthProvider string     `json:"auth_provider"`
	LastLoginAt  *time.Time `json:"last_login_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// Session is a browser login. The ID is the opaque secret stored in the
// pf_session cookie; it is high-entropy and kept only server-side.
type Session struct {
	ID         string    `json:"id"`
	UserID     string    `json:"user_id"`
	CreatedAt  time.Time `json:"created_at"`
	LastSeenAt time.Time `json:"last_seen_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	IP         string    `json:"ip,omitempty"`
	UserAgent  string    `json:"user_agent,omitempty"`
}

// APIKey is one external integration's credential. Secret and Prefix together
// authenticate a request; the rest are the per-key security controls enforced on
// every call the key makes.
type APIKey struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	OwnerEmail  string `json:"owner_email,omitempty"`
	// Prefix is the visible lookup half, shown as "pmx_<prefix>…".
	Prefix string `json:"prefix"`
	// SecretHash is never serialised to API clients; the handlers zero it.
	SecretHash string `json:"-"`
	Role       string `json:"role"` // apiauth role id
	Active     bool   `json:"active"`

	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// RateLimitPerMin nil means "use the role / global default".
	RateLimitPerMin *int     `json:"rate_limit_per_min,omitempty"`
	IPAllowlist     []string `json:"ip_allowlist,omitempty"`
	RequireMTLS     bool     `json:"require_mtls"`
	RedactPII       bool     `json:"redact_pii"`

	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	CreatedBy  string     `json:"created_by,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

// Expired reports whether the key has an expiry that has passed.
func (k APIKey) Expired(now time.Time) bool {
	return k.ExpiresAt != nil && now.After(*k.ExpiresAt)
}

// APIKeyEvent is one row of a key's audit trail.
type APIKeyEvent struct {
	ID       int64           `json:"id"`
	APIKeyID string          `json:"api_key_id"`
	At       time.Time       `json:"at"`
	Actor    string          `json:"actor"`
	Action   string          `json:"action"`
	Detail   json.RawMessage `json:"detail,omitempty"`
}

// ExternalAPISettings is the pf_settings row keyed "external_api".
type ExternalAPISettings struct {
	Enabled                bool `json:"enabled"`
	DefaultRateLimitPerMin int  `json:"default_rate_limit_per_min"`
}

// LogRetention is the pf_settings row keyed "log_retention". The janitor deletes
// pf_logs rows older than MaxAgeHours when Enabled.
type LogRetention struct {
	Enabled     bool `json:"enabled"`
	MaxAgeHours int  `json:"max_age_hours"`
}

// GitConnection is the pf_settings row keyed "git_connection". It is the target
// repository the GitOps worker-delivery flow renders manifests into. Token is
// write-only: it is stored but never returned to a client (HasToken signals
// whether one is set).
type GitConnection struct {
	RepoURL     string `json:"repo_url"`
	Branch      string `json:"branch"`
	BasePath    string `json:"base_path"`
	Provider    string `json:"provider"` // github | gitlab | other (auto-detected on save)
	AutoSync    bool   `json:"auto_sync"`
	AuthorName  string `json:"author_name"`
	AuthorEmail string `json:"author_email"`
	Token       string `json:"token,omitempty"` // inbound only; cleared before any read
	HasToken    bool   `json:"has_token"`       // outbound only
	UpdatedAt   string `json:"updated_at,omitempty"`
}
