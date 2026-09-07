package server

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/primex/primeflow/internal/apiauth"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

// ------------------------------------------------------- external settings ---

func (s *Server) getExternalAPISettings(w http.ResponseWriter, r *http.Request) {
	cfg, err := s.store.GetExternalAPISettings(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

func (s *Server) putExternalAPISettings(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Enabled                *bool `json:"enabled,omitempty"`
		DefaultRateLimitPerMin *int  `json:"default_rate_limit_per_min,omitempty"`
	}
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	cur, err := s.store.GetExternalAPISettings(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	if b.Enabled != nil {
		cur.Enabled = *b.Enabled
	}
	if b.DefaultRateLimitPerMin != nil {
		if *b.DefaultRateLimitPerMin < 0 {
			writeErr(w, http.StatusBadRequest, errors.New("default_rate_limit_per_min must be >= 0"))
			return
		}
		cur.DefaultRateLimitPerMin = *b.DefaultRateLimitPerMin
	}
	if err := s.store.PutExternalAPISettings(r.Context(), cur); err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "external-api.settings-changed", "settings", "external_api",
		mustJSON(cur))
	writeJSON(w, http.StatusOK, cur)
}

// apiRoles feeds the console's key form and the API Explorer: the role catalogue
// plus the route→scope map, straight from the package that enforces them.
func (s *Server) apiRoles(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"roles":  apiauth.Roles(),
		"routes": apiauth.Routes,
	})
}

// ------------------------------------------------------------- api keys ---

// apiKeyBody is the shared create/update wire shape. Pointer fields on update
// mean "leave unchanged"; on create a nil pointer takes the documented default.
type apiKeyBody struct {
	Name            string    `json:"name,omitempty"`
	Description     *string   `json:"description,omitempty"`
	OwnerEmail      *string   `json:"owner_email,omitempty"`
	Role            string    `json:"role,omitempty"`
	Active          *bool     `json:"active,omitempty"`
	ExpiresAt       *string   `json:"expires_at,omitempty"` // RFC3339 or "" to clear
	RateLimitPerMin *int      `json:"rate_limit_per_min,omitempty"`
	IPAllowlist     *[]string `json:"ip_allowlist,omitempty"`
	RequireMTLS     *bool     `json:"require_mtls,omitempty"`
	RedactPII       *bool     `json:"redact_pii,omitempty"`
}

func parseExpiry(v string) (*time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, nil
	}
	// Accept a bare date (from <input type=date>) or a full RFC3339 timestamp.
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		if t, err := time.Parse(layout, v); err == nil {
			u := t.UTC()
			return &u, nil
		}
	}
	return nil, fmt.Errorf("expires_at %q is not a date or RFC3339 timestamp", v)
}

func validIPAllowlist(entries []string) error {
	joined := strings.Join(entries, ",")
	if joined == "" {
		return nil
	}
	if len(apiauth.ParseCIDRs(joined)) != len(nonEmpty(entries)) {
		return errors.New("ip_allowlist entries must be IPs or CIDR blocks")
	}
	return nil
}

func nonEmpty(in []string) []string {
	out := in[:0]
	for _, s := range in {
		if strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return out
}

func (s *Server) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	f := store.APIKeyFilter{
		Role:   r.URL.Query().Get("role"),
		Search: r.URL.Query().Get("search"),
	}
	switch r.URL.Query().Get("status") {
	case "active":
		t := true
		f.Active = &t
	case "inactive":
		t := false
		f.Active = &t
	}
	ks, err := s.store.ListAPIKeys(r.Context(), f)
	if err != nil {
		fail(w, err)
		return
	}
	for i := range ks {
		ks[i].SecretHash = ""
	}
	writeJSON(w, http.StatusOK, ks)
}

func (s *Server) getAPIKey(w http.ResponseWriter, r *http.Request) {
	k, err := s.store.GetAPIKey(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	k.SecretHash = ""
	writeJSON(w, http.StatusOK, k)
}

func (s *Server) createAPIKey(w http.ResponseWriter, r *http.Request) {
	var b apiKeyBody
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	b.Name = strings.TrimSpace(b.Name)
	if b.Name == "" {
		writeErr(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	if !apiauth.ValidRole(b.Role) {
		writeErr(w, http.StatusBadRequest, errors.New("unknown role"))
		return
	}
	var expiry *time.Time
	if b.ExpiresAt != nil {
		t, err := parseExpiry(*b.ExpiresAt)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		expiry = t
	}
	allow := []string{}
	if b.IPAllowlist != nil {
		allow = nonEmpty(*b.IPAllowlist)
		if err := validIPAllowlist(allow); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
	}
	if b.RateLimitPerMin != nil && *b.RateLimitPerMin < 0 {
		writeErr(w, http.StatusBadRequest, errors.New("rate_limit_per_min must be >= 0"))
		return
	}

	full, prefix, hash := apiauth.NewSecret()
	k := &core.APIKey{
		ID: newID(), Name: b.Name, Prefix: prefix, SecretHash: hash, Role: b.Role,
		Active: true, ExpiresAt: expiry, RateLimitPerMin: b.RateLimitPerMin,
		IPAllowlist: allow, CreatedBy: principalEmail(r),
	}
	if b.Description != nil {
		k.Description = strings.TrimSpace(*b.Description)
	}
	if b.OwnerEmail != nil {
		k.OwnerEmail = strings.TrimSpace(*b.OwnerEmail)
	}
	if b.Active != nil {
		k.Active = *b.Active
	}
	if b.RequireMTLS != nil {
		k.RequireMTLS = *b.RequireMTLS
	}
	if b.RedactPII != nil {
		k.RedactPII = *b.RedactPII
	}
	if err := s.store.CreateAPIKey(r.Context(), k); err != nil {
		fail(w, err)
		return
	}
	s.recordKeyEvent(r, k.ID, "created", map[string]any{"role": k.Role})
	k.SecretHash = ""
	writeJSON(w, http.StatusOK, map[string]any{"key": k, "secret": full})
}

func (s *Server) updateAPIKey(w http.ResponseWriter, r *http.Request) {
	k, err := s.store.GetAPIKey(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	var b apiKeyBody
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	wasActive := k.Active

	if b.Name != "" {
		k.Name = strings.TrimSpace(b.Name)
	}
	if b.Description != nil {
		k.Description = strings.TrimSpace(*b.Description)
	}
	if b.OwnerEmail != nil {
		k.OwnerEmail = strings.TrimSpace(*b.OwnerEmail)
	}
	if b.Role != "" {
		if !apiauth.ValidRole(b.Role) {
			writeErr(w, http.StatusBadRequest, errors.New("unknown role"))
			return
		}
		k.Role = b.Role
	}
	if b.Active != nil {
		k.Active = *b.Active
	}
	if b.ExpiresAt != nil {
		t, err := parseExpiry(*b.ExpiresAt)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		k.ExpiresAt = t
	}
	if b.RateLimitPerMin != nil {
		if *b.RateLimitPerMin < 0 {
			writeErr(w, http.StatusBadRequest, errors.New("rate_limit_per_min must be >= 0"))
			return
		}
		if *b.RateLimitPerMin == 0 {
			k.RateLimitPerMin = nil
		} else {
			k.RateLimitPerMin = b.RateLimitPerMin
		}
	}
	if b.IPAllowlist != nil {
		allow := nonEmpty(*b.IPAllowlist)
		if err := validIPAllowlist(allow); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		k.IPAllowlist = allow
	}
	if b.RequireMTLS != nil {
		k.RequireMTLS = *b.RequireMTLS
	}
	if b.RedactPII != nil {
		k.RedactPII = *b.RedactPII
	}
	if err := s.store.UpdateAPIKey(r.Context(), k); err != nil {
		fail(w, err)
		return
	}
	s.rateLimiter.Forget(k.ID) // re-provision the bucket at the new limit
	switch {
	case wasActive && !k.Active:
		s.recordKeyEvent(r, k.ID, "deactivated", nil)
	case !wasActive && k.Active:
		s.recordKeyEvent(r, k.ID, "activated", nil)
	default:
		s.recordKeyEvent(r, k.ID, "updated", nil)
	}
	k.SecretHash = ""
	writeJSON(w, http.StatusOK, k)
}

func (s *Server) rotateAPIKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.GetAPIKey(r.Context(), id); err != nil {
		fail(w, err)
		return
	}
	full, prefix, hash := apiauth.NewSecret()
	if err := s.store.SetAPIKeySecret(r.Context(), id, prefix, hash); err != nil {
		fail(w, err)
		return
	}
	s.rateLimiter.Forget(id)
	s.recordKeyEvent(r, id, "rotated", nil)
	writeJSON(w, http.StatusOK, map[string]any{"prefix": prefix, "secret": full})
}

func (s *Server) deleteAPIKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.DeleteAPIKey(r.Context(), id); err != nil {
		fail(w, err)
		return
	}
	s.rateLimiter.Forget(id)
	s.events.Emit(r.Context(), "external-api.key-deleted", "api-key", id, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) apiKeyHistory(w http.ResponseWriter, r *http.Request) {
	evs, err := s.store.ListAPIKeyEvents(r.Context(), r.PathValue("id"), intParam(r, "limit", 200))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, evs)
}

// recordKeyEvent writes an audit line and mirrors it onto the event feed.
func (s *Server) recordKeyEvent(r *http.Request, keyID, action string, detail map[string]any) {
	var raw []byte
	if detail != nil {
		raw = mustJSON(detail)
	}
	_ = s.store.AppendAPIKeyEvent(r.Context(), &core.APIKeyEvent{
		APIKeyID: keyID, Actor: principalEmail(r), Action: action, Detail: raw,
	})
	s.events.Emit(r.Context(), "external-api.key-"+action, "api-key", keyID, raw)
}

func principalEmail(r *http.Request) string {
	if p := principalFrom(r.Context()); p != nil {
		if p.Machine {
			return "machine-token"
		}
		return p.Email
	}
	return "system"
}
