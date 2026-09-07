package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/primex/primeflow/internal/authn"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

// principal is the authenticated identity behind a request. A machine principal
// comes from the static PRIMEFLOW_API_TOKEN (workers, CLI) and is treated as an
// admin; a user principal comes from a browser session cookie and carries its
// account's role.
type principal struct {
	Machine bool
	UserID  string
	Email   string
	Role    authn.Role
}

func (p *principal) canMutate() bool { return p.Machine || p.Role.CanMutate() }
func (p *principal) canAdmin() bool  { return p.Machine || p.Role.CanAdmin() }

type ctxKey int

const principalKey ctxKey = 0

func withPrincipal(ctx context.Context, p *principal) context.Context {
	return context.WithValue(ctx, principalKey, p)
}

// principalFrom returns the request principal, or nil if the route is
// unauthenticated (only /api/v1/health and the auth endpoints).
func principalFrom(ctx context.Context) *principal {
	p, _ := ctx.Value(principalKey).(*principal)
	return p
}

// --------------------------------------------------------------- cookies ---

const (
	sessionCookie = "pf_session"
	csrfCookie    = "pf_csrf"
	csrfHeader    = "X-CSRF-Token"
)

func randToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("server: crypto/rand failed: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func (s *Server) setAuthCookies(w http.ResponseWriter, sessionID, csrf string, ttl time.Duration) {
	common := http.Cookie{Path: "/", Secure: s.cfg.CookieSecure, SameSite: http.SameSiteLaxMode}

	sc := common
	sc.Name, sc.Value, sc.HttpOnly = sessionCookie, sessionID, true
	sc.Expires = time.Now().Add(ttl)
	sc.MaxAge = int(ttl.Seconds())
	http.SetCookie(w, &sc)

	// Readable by the console JS so it can echo it back in the CSRF header.
	cc := common
	cc.Name, cc.Value = csrfCookie, csrf
	cc.Expires = time.Now().Add(ttl)
	cc.MaxAge = int(ttl.Seconds())
	http.SetCookie(w, &cc)
}

func (s *Server) clearAuthCookies(w http.ResponseWriter) {
	for _, name := range []string{sessionCookie, csrfCookie} {
		http.SetCookie(w, &http.Cookie{
			Name: name, Value: "", Path: "/", MaxAge: -1,
			Secure: s.cfg.CookieSecure, SameSite: http.SameSiteLaxMode,
		})
	}
}

// --------------------------------------------------------------- handlers ---

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	b.Email = strings.TrimSpace(b.Email)
	if b.Email == "" || b.Password == "" {
		writeErr(w, http.StatusBadRequest, errors.New("email and password are required"))
		return
	}

	// Per-(email+IP) brute-force guard, plus a flat delay on every attempt so a
	// wrong password is not observably faster than a missing account.
	throttleKey := strings.ToLower(b.Email) + "|" + s.clientIP(r)
	if locked, retry := s.loginThrottle.Locked(throttleKey); locked {
		w.Header().Set("Retry-After", secondsString(retry))
		writeErr(w, http.StatusTooManyRequests, errors.New("too many attempts, try again later"))
		return
	}
	time.Sleep(250 * time.Millisecond)

	user, hash, err := s.store.GetUserAuth(r.Context(), b.Email)
	ok := false
	if err == nil && user.Active {
		if match, verr := authn.VerifyPassword(hash, b.Password); verr == nil && match {
			ok = true
		}
	}
	if !ok {
		s.loginThrottle.Fail(throttleKey)
		writeErr(w, http.StatusUnauthorized, errors.New("invalid email or password"))
		return
	}
	s.loginThrottle.Reset(throttleKey)

	ttl := s.cfg.SessionTTL
	sess := &core.Session{
		ID: randToken(32), UserID: user.ID, ExpiresAt: time.Now().Add(ttl),
		IP: s.clientIP(r), UserAgent: truncate(r.UserAgent(), 400),
	}
	if err := s.store.CreateSession(r.Context(), sess); err != nil {
		fail(w, err)
		return
	}
	_ = s.store.TouchUserLogin(r.Context(), user.ID, time.Now().UTC())

	csrf := randToken(24)
	s.setAuthCookies(w, sess.ID, csrf, ttl)
	s.events.Emit(r.Context(), "operator.login", "user", user.ID, nil)
	writeJSON(w, http.StatusOK, map[string]any{"email": user.Email, "role": user.Role})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil && c.Value != "" {
		_ = s.store.DeleteSession(r.Context(), c.Value)
	}
	s.clearAuthCookies(w)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) me(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	if p == nil {
		writeErr(w, http.StatusUnauthorized, errors.New("not authenticated"))
		return
	}
	role := string(p.Role)
	if p.Machine {
		role = string(authn.RoleAdmin)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"email": p.Email, "role": role, "machine": p.Machine,
	})
}

// ----------------------------------------------------------- user admin ---

func (s *Server) listUsers(w http.ResponseWriter, r *http.Request) {
	us, err := s.store.ListUsers(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, us)
}

func (s *Server) createUser(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	b.Email, b.Role = strings.TrimSpace(b.Email), strings.TrimSpace(b.Role)
	if b.Email == "" || len(b.Password) < 8 {
		writeErr(w, http.StatusBadRequest, errors.New("email and a password of at least 8 characters are required"))
		return
	}
	if !authn.ValidRole(b.Role) {
		writeErr(w, http.StatusBadRequest, errors.New("role must be admin, operator or viewer"))
		return
	}
	u, err := s.store.CreateUser(r.Context(), store.UserInput{
		ID: newID(), Email: b.Email, PasswordHash: authn.HashPassword(b.Password),
		Role: b.Role, Active: true,
	})
	if err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "operator.user-created", "user", u.ID, nil)
	writeJSON(w, http.StatusOK, u)
}

func (s *Server) updateUser(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var b struct {
		Role     *string `json:"role,omitempty"`
		Active   *bool   `json:"active,omitempty"`
		Password string  `json:"password,omitempty"`
	}
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if b.Role != nil && !authn.ValidRole(*b.Role) {
		writeErr(w, http.StatusBadRequest, errors.New("role must be admin, operator or viewer"))
		return
	}
	if b.Password != "" && len(b.Password) < 8 {
		writeErr(w, http.StatusBadRequest, errors.New("password must be at least 8 characters"))
		return
	}
	// Guard against locking everyone out: never remove the last active admin.
	if (b.Active != nil && !*b.Active) || (b.Role != nil && *b.Role != string(authn.RoleAdmin)) {
		if last, err := s.isLastActiveAdmin(r.Context(), id); err == nil && last {
			writeErr(w, http.StatusConflict, errors.New("cannot demote or deactivate the last active admin"))
			return
		}
	}
	hash := ""
	if b.Password != "" {
		hash = authn.HashPassword(b.Password)
	}
	u, err := s.store.UpdateUser(r.Context(), id, b.Role, b.Active, hash)
	if err != nil {
		fail(w, err)
		return
	}
	// A role change or deactivation must not leave stale sessions with stale
	// authority; force the account to log back in.
	if b.Role != nil || (b.Active != nil && !*b.Active) || b.Password != "" {
		_ = s.store.DeleteUserSessions(r.Context(), id)
	}
	writeJSON(w, http.StatusOK, u)
}

func (s *Server) deleteUser(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if last, err := s.isLastActiveAdmin(r.Context(), id); err == nil && last {
		writeErr(w, http.StatusConflict, errors.New("cannot deactivate the last active admin"))
		return
	}
	no := false
	if _, err := s.store.UpdateUser(r.Context(), id, nil, &no, ""); err != nil {
		fail(w, err)
		return
	}
	_ = s.store.DeleteUserSessions(r.Context(), id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) isLastActiveAdmin(ctx context.Context, id string) (bool, error) {
	us, err := s.store.ListUsers(ctx)
	if err != nil {
		return false, err
	}
	admins, isTarget := 0, false
	for _, u := range us {
		if u.Active && u.Role == string(authn.RoleAdmin) {
			admins++
			if u.ID == id {
				isTarget = true
			}
		}
	}
	return isTarget && admins <= 1, nil
}

// --------------------------------------------------------------- helpers ---

func secondsString(d time.Duration) string {
	s := int(d.Seconds())
	if s < 1 {
		s = 1
	}
	return itoa(s)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
