// Package oidcauth wraps the OIDC Authorization-Code + PKCE dance and the
// claim → operator-role mapping. It is separate from the server so the mapping
// is unit-testable and discovery failures are handled at start-up.
package oidcauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

// Config is the OIDC provider configuration, sourced from PRIMEFLOW_OIDC_*.
type Config struct {
	Issuer       string
	ClientID     string
	ClientSecret string
	RedirectURL  string
	Scopes       []string          // default: openid, profile, email
	DefaultRole  string            // default: viewer
	RoleClaim    string            // e.g. "groups" or "roles"; empty disables mapping
	RoleMap      map[string]string // claim value -> operator role
}

// Enabled reports whether OIDC login should be offered.
func (c Config) Enabled() bool { return c.Issuer != "" && c.ClientID != "" }

// Provider holds the discovered endpoints and the verifier.
type Provider struct {
	cfg      Config
	oauth    *oauth2.Config
	verifier *oidc.IDTokenVerifier
}

// New performs OIDC discovery against the issuer and builds the provider.
func New(ctx context.Context, c Config) (*Provider, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("oidcauth: issuer and client id are required")
	}
	if len(c.Scopes) == 0 {
		c.Scopes = []string{oidc.ScopeOpenID, "profile", "email"}
	}
	if c.DefaultRole == "" {
		c.DefaultRole = "viewer"
	}
	prov, err := oidc.NewProvider(ctx, strings.TrimRight(c.Issuer, "/"))
	if err != nil {
		return nil, fmt.Errorf("oidc discovery: %w", err)
	}
	return &Provider{
		cfg: c,
		oauth: &oauth2.Config{
			ClientID:     c.ClientID,
			ClientSecret: c.ClientSecret,
			RedirectURL:  c.RedirectURL,
			Endpoint:     prov.Endpoint(),
			Scopes:       c.Scopes,
		},
		verifier: prov.Verifier(&oidc.Config{ClientID: c.ClientID}),
	}, nil
}

// pkceChallenge returns the S256 challenge for a verifier.
func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// AuthURL builds the provider redirect for a login attempt. redirectOverride,
// when non-empty, replaces the configured RedirectURL (used to derive it from
// the incoming request host).
func (p *Provider) AuthURL(state, verifier, redirectOverride string) string {
	opts := []oauth2.AuthCodeOption{
		oauth2.SetAuthURLParam("code_challenge", pkceChallenge(verifier)),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	}
	if redirectOverride != "" {
		opts = append(opts, oauth2.SetAuthURLParam("redirect_uri", redirectOverride))
	}
	return p.oauth.AuthCodeURL(state, opts...)
}

// Claims is what Exchange returns after verifying the ID token.
type Claims struct {
	Email string
	Role  string
	Sub   string
}

// Exchange trades the code for tokens, verifies the ID token, and maps the
// operator role from the configured claim.
func (p *Provider) Exchange(ctx context.Context, code, verifier, redirectOverride string) (Claims, error) {
	opts := []oauth2.AuthCodeOption{oauth2.SetAuthURLParam("code_verifier", verifier)}
	if redirectOverride != "" {
		opts = append(opts, oauth2.SetAuthURLParam("redirect_uri", redirectOverride))
	}
	tok, err := p.oauth.Exchange(ctx, code, opts...)
	if err != nil {
		return Claims{}, fmt.Errorf("code exchange: %w", err)
	}
	rawID, ok := tok.Extra("id_token").(string)
	if !ok {
		return Claims{}, fmt.Errorf("no id_token in response")
	}
	idt, err := p.verifier.Verify(ctx, rawID)
	if err != nil {
		return Claims{}, fmt.Errorf("verify id_token: %w", err)
	}
	var raw map[string]any
	if err := idt.Claims(&raw); err != nil {
		return Claims{}, fmt.Errorf("decode claims: %w", err)
	}
	email, _ := raw["email"].(string)
	if email == "" {
		return Claims{}, fmt.Errorf("id_token has no email claim")
	}
	return Claims{
		Email: strings.ToLower(strings.TrimSpace(email)),
		Sub:   idt.Subject,
		Role:  MapRole(raw, p.cfg.RoleClaim, p.cfg.RoleMap, p.cfg.DefaultRole),
	}, nil
}

// MapRole picks an operator role from the ID-token claims. It reads roleClaim
// (which may be a string or a []string), returns the first value present in
// roleMap, and otherwise defaultRole. Pure — unit-tested.
func MapRole(claims map[string]any, roleClaim string, roleMap map[string]string, defaultRole string) string {
	if roleClaim == "" || len(roleMap) == 0 {
		return defaultRole
	}
	// Prefer the most privileged match when several groups map.
	rank := map[string]int{"admin": 3, "operator": 2, "viewer": 1}
	best, bestRank := defaultRole, rank[defaultRole]
	for _, v := range claimValues(claims[roleClaim]) {
		if mapped, ok := roleMap[v]; ok && rank[mapped] > bestRank {
			best, bestRank = mapped, rank[mapped]
		}
	}
	return best
}

func claimValues(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// ParseRoleMap turns "grpA=admin,grpB=operator" into a map.
func ParseRoleMap(s string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if ok && k != "" && v != "" {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return out
}
