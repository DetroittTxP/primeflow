package server

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/primex/primeflow/internal/authn"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

const oidcCookie = "pf_oidc"

// authConfig tells the login page which sign-in methods are available.
func (s *Server) authConfig(w http.ResponseWriter, r *http.Request) {
	label := s.cfg.OIDCLabel
	if label == "" {
		label = "SSO"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"oidc":       s.cfg.OIDC != nil,
		"oidc_label": label,
	})
}

// oidcRedirectURL is the configured callback, or one derived from the request.
func (s *Server) oidcRedirectURL(r *http.Request) string {
	if s.cfg.OIDCRedirectURL != "" {
		return s.cfg.OIDCRedirectURL
	}
	scheme := "http"
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") || s.cfg.CookieSecure {
		scheme = "https"
	}
	return scheme + "://" + r.Host + "/api/v1/auth/oidc/callback"
}

func (s *Server) oidcLogin(w http.ResponseWriter, r *http.Request) {
	if s.cfg.OIDC == nil {
		writeErr(w, http.StatusNotFound, errors.New("SSO is not configured"))
		return
	}
	state := randToken(16)
	verifier := randToken(32)
	// state|verifier in a short-lived HttpOnly cookie; the state match on
	// callback is the CSRF check, the verifier completes PKCE.
	http.SetCookie(w, &http.Cookie{
		Name: oidcCookie, Value: state + "|" + verifier, Path: "/",
		HttpOnly: true, Secure: s.cfg.CookieSecure, SameSite: http.SameSiteLaxMode,
		MaxAge: 600,
	})
	http.Redirect(w, r, s.cfg.OIDC.AuthURL(state, verifier, s.oidcRedirectURL(r)), http.StatusFound)
}

func (s *Server) oidcCallback(w http.ResponseWriter, r *http.Request) {
	if s.cfg.OIDC == nil {
		writeErr(w, http.StatusNotFound, errors.New("SSO is not configured"))
		return
	}
	if e := r.URL.Query().Get("error"); e != "" {
		http.Redirect(w, r, "/login.html?sso_error="+e, http.StatusSeeOther)
		return
	}
	c, err := r.Cookie(oidcCookie)
	if err != nil {
		http.Redirect(w, r, "/login.html?sso_error=state", http.StatusSeeOther)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: oidcCookie, Value: "", Path: "/", MaxAge: -1})
	state, verifier, ok := strings.Cut(c.Value, "|")
	if !ok || state == "" || r.URL.Query().Get("state") != state {
		http.Redirect(w, r, "/login.html?sso_error=state", http.StatusSeeOther)
		return
	}

	claims, err := s.cfg.OIDC.Exchange(r.Context(), r.URL.Query().Get("code"), verifier, s.oidcRedirectURL(r))
	if err != nil {
		s.log.Warn("oidc callback failed", "err", err)
		http.Redirect(w, r, "/login.html?sso_error=exchange", http.StatusSeeOther)
		return
	}

	// JIT provisioning / role sync.
	user, err := s.store.GetUserByEmail(r.Context(), claims.Email)
	switch {
	case errors.Is(err, store.ErrNotFound):
		user, err = s.store.CreateUser(r.Context(), store.UserInput{
			ID: newID(), Email: claims.Email, Role: claims.Role, Active: true,
			AuthProvider: "oidc", PasswordHash: authn.HashPassword(randToken(24)), // unusable
		})
		if err != nil {
			fail(w, err)
			return
		}
		s.events.Emit(r.Context(), "operator.user-created", "user", user.ID, mustJSON(map[string]string{"via": "oidc"}))
	case err != nil:
		fail(w, err)
		return
	default:
		if !user.Active {
			http.Redirect(w, r, "/login.html?sso_error=disabled", http.StatusSeeOther)
			return
		}
		// Sync the role from the IdP for SSO-provisioned accounts, but never
		// demote the last admin and never touch a local account's role.
		if user.AuthProvider == "oidc" && user.Role != claims.Role {
			if !(claims.Role != string(authn.RoleAdmin) && s.lastActiveAdminSafe(r, user.ID)) {
				role := claims.Role
				if u2, uerr := s.store.UpdateUser(r.Context(), user.ID, &role, nil, ""); uerr == nil {
					user = u2
				}
			}
		}
	}

	_ = s.store.TouchUserLogin(r.Context(), user.ID, time.Now().UTC())
	if err := s.startSession(w, r, user); err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "operator.login", "user", user.ID, mustJSON(map[string]string{"via": "oidc"}))
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// lastActiveAdminSafe reports whether id is the only active admin (so it must
// not be demoted). Errors are treated as "not the last" to avoid blocking.
func (s *Server) lastActiveAdminSafe(r *http.Request, id string) bool {
	last, err := s.isLastActiveAdmin(r.Context(), id)
	return err == nil && last
}

// startSession creates a session row and sets the auth cookies. Shared by
// password login and OIDC.
func (s *Server) startSession(w http.ResponseWriter, r *http.Request, user *core.User) error {
	ttl := s.cfg.SessionTTL
	sess := &core.Session{
		ID: randToken(32), UserID: user.ID, ExpiresAt: time.Now().Add(ttl),
		IP: s.clientIP(r), UserAgent: truncate(r.UserAgent(), 400),
	}
	if err := s.store.CreateSession(r.Context(), sess); err != nil {
		return err
	}
	s.setAuthCookies(w, sess.ID, randToken(24), ttl)
	return nil
}

// ------------------------------------------------- password reset links ---

// createResetLink (admin) issues a single-use reset URL for a local account.
func (s *Server) createResetLink(w http.ResponseWriter, r *http.Request) {
	u, err := s.store.GetUser(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	if u.AuthProvider != "local" {
		writeErr(w, http.StatusConflict, errors.New("this account signs in via SSO; no password to reset"))
		return
	}
	ttl := s.cfg.ResetTTL
	if ttl <= 0 {
		ttl = time.Hour
	}
	token, expires, err := s.store.CreatePasswordReset(r.Context(), u.ID, ttl)
	if err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "operator.reset-link-issued", "user", u.ID, nil)
	writeJSON(w, http.StatusOK, map[string]any{
		"url":        "/reset.html?token=" + token,
		"expires_at": expires,
	})
}

// resetPassword (unauthenticated) consumes a token and sets a new password.
func (s *Server) resetPassword(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Token    string `json:"token"`
		Password string `json:"password"`
	}
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if b.Token == "" || len(b.Password) < 8 {
		writeErr(w, http.StatusBadRequest, errors.New("token and a password of at least 8 characters are required"))
		return
	}
	userID, err := s.store.ConsumePasswordReset(r.Context(), b.Token)
	if err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("this reset link is invalid or has expired"))
		return
	}
	if _, err := s.store.UpdateUser(r.Context(), userID, nil, nil, authn.HashPassword(b.Password)); err != nil {
		fail(w, err)
		return
	}
	_ = s.store.DeleteUserSessions(r.Context(), userID)
	s.events.Emit(r.Context(), "operator.password-reset", "user", userID, nil)
	w.WriteHeader(http.StatusNoContent)
}
