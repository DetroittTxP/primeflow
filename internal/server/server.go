// Package server exposes the REST API, the live event stream and the operator
// UI.
//
// The API is the only way anything mutates state: workers, the CLI and the UI
// all speak it, so there is one place where the orchestration rules and the
// admin controls are enforced.
//
// Authentication has two independent surfaces:
//
//   - /api/v1/*    — the operator API. A human authenticates with a session
//     cookie (see auth.go) carrying a role; a machine authenticates with the
//     static PRIMEFLOW_API_TOKEN and is treated as an admin. /api/v1/health is
//     always open.
//   - /api/external/v1/*  — the External API. Authenticated by issued API keys
//     with per-key security controls and a global master switch (see external.go
//     and apikeys.go).
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/apiauth"
	"github.com/primex/primeflow/internal/authn"
	"github.com/primex/primeflow/internal/bus"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/metrics"
	"github.com/primex/primeflow/internal/oidcauth"
	"github.com/primex/primeflow/internal/ratelimit"
	"github.com/primex/primeflow/internal/store"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// DefaultSessionTTL is used when Config.SessionTTL is zero.
const DefaultSessionTTL = 7 * 24 * time.Hour

// Config configures the HTTP server.
type Config struct {
	Addr string
	// APIToken, when set, is accepted as "Authorization: Bearer <token>" on
	// /api/v1 routes for machine clients (workers, CLI). Human operators use
	// login sessions regardless. Leave empty only on a trusted network.
	APIToken string
	// UIEnabled serves the bundled operator console at /.
	UIEnabled bool
	// CORSOrigin allows a separately hosted front end (e.g. the PrimeX
	// console) to call this API.
	CORSOrigin string

	// TrustedProxyCIDRs lists the networks an edge proxy may connect from. Only
	// when the immediate peer is in this set are X-Forwarded-For and
	// X-SSL-Client-Verify believed.
	TrustedProxyCIDRs []*net.IPNet
	// SessionTTL is how long a login session lasts; it slides forward on use.
	SessionTTL time.Duration
	// CookieSecure marks the session cookies Secure. Set it when the console is
	// served over HTTPS (directly or via a terminating proxy).
	CookieSecure bool

	// Metrics, when non-nil, enables the /metrics endpoint and HTTP RED series.
	Metrics *metrics.Metrics

	// LoginThrottle / KeyRateLimiter override the in-process defaults with a
	// shared (Redis-backed) implementation. Nil falls back to per-replica limits.
	LoginThrottle  ratelimit.Throttle
	KeyRateLimiter ratelimit.Limiter

	// OIDC, when non-nil, enables "Sign in with SSO". OIDCRedirectURL may be
	// blank to derive it from each request. ResetTTL bounds admin-issued
	// password-reset links (default 1h).
	OIDC            *oidcauth.Provider
	OIDCRedirectURL string
	OIDCLabel       string
	ResetTTL        time.Duration
}

// Server holds the API dependencies.
type Server struct {
	store  store.Store
	bus    bus.Bus
	events *events.Emitter
	log    *slog.Logger
	cfg    Config

	loginThrottle ratelimit.Throttle
	rateLimiter   ratelimit.Limiter
}

// New builds a server.
func New(s store.Store, b bus.Bus, em *events.Emitter, log *slog.Logger, cfg Config) *Server {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = DefaultSessionTTL
	}
	lt := cfg.LoginThrottle
	if lt == nil {
		lt = authn.NewThrottle()
	}
	rl := cfg.KeyRateLimiter
	if rl == nil {
		rl = apiauth.NewRateLimiter()
	}
	return &Server{
		store: s, bus: b, events: em, log: log, cfg: cfg,
		loginThrottle: lt,
		rateLimiter:   rl,
	}
}

// Handler returns the fully wired HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// --- auth (operator login) ---
	mux.HandleFunc("POST /api/v1/auth/login", s.login)
	mux.HandleFunc("POST /api/v1/auth/logout", s.logout)
	mux.HandleFunc("GET /api/v1/auth/me", s.me)
	mux.HandleFunc("GET /api/v1/auth/config", s.authConfig)
	mux.HandleFunc("POST /api/v1/auth/reset", s.resetPassword)
	if s.cfg.OIDC != nil {
		mux.HandleFunc("GET /api/v1/auth/oidc/login", s.oidcLogin)
		mux.HandleFunc("GET /api/v1/auth/oidc/callback", s.oidcCallback)
	}

	// --- health & catalogue ---
	mux.HandleFunc("GET /api/v1/health", s.health)
	mux.HandleFunc("GET /api/v1/flows", s.listFlows)
	mux.HandleFunc("GET /api/v1/flows/{name}", s.getFlow)
	mux.HandleFunc("GET /api/v1/workers", s.listWorkers)
	mux.HandleFunc("GET /api/v1/summary", s.summary)
	mux.HandleFunc("GET /api/v1/stats", s.stats)

	// --- deployments ---
	mux.HandleFunc("GET /api/v1/deployments", s.listDeployments)
	mux.HandleFunc("POST /api/v1/deployments", s.upsertDeployment)
	mux.HandleFunc("GET /api/v1/deployments/{id}", s.getDeployment)
	mux.HandleFunc("DELETE /api/v1/deployments/{id}", s.deleteDeployment)
	mux.HandleFunc("POST /api/v1/deployments/{id}/pause", s.pauseDeployment(true))
	mux.HandleFunc("POST /api/v1/deployments/{id}/resume", s.pauseDeployment(false))
	mux.HandleFunc("POST /api/v1/deployments/{id}/run", s.runDeployment)

	// --- runs ---
	mux.HandleFunc("GET /api/v1/runs", s.listRuns)
	mux.HandleFunc("POST /api/v1/runs", s.createRun)
	mux.HandleFunc("GET /api/v1/runs/{id}", s.getRun)
	mux.HandleFunc("GET /api/v1/runs/{id}/tasks", s.runTasks)
	mux.HandleFunc("GET /api/v1/runs/{id}/logs", s.runLogs)
	mux.HandleFunc("GET /api/v1/runs/{id}/artifacts", s.runArtifacts)
	mux.HandleFunc("GET /api/v1/runs/{id}/children", s.runChildren)
	mux.HandleFunc("POST /api/v1/runs/{id}/cancel", s.cancelRun)
	mux.HandleFunc("POST /api/v1/runs/{id}/retry", s.retryRun)
	mux.HandleFunc("POST /api/v1/runs/{id}/reschedule", s.rescheduleRun)

	// --- operator queue controls ---
	mux.HandleFunc("POST /api/v1/runs/{id}/priority", s.setPriority)
	mux.HandleFunc("POST /api/v1/runs/{id}/front", s.moveFront)
	mux.HandleFunc("POST /api/v1/runs/{id}/back", s.moveBack)
	mux.HandleFunc("POST /api/v1/runs/{id}/unpin", s.unpin)
	mux.HandleFunc("POST /api/v1/runs/{id}/queue", s.moveQueue)

	// --- queues ---
	mux.HandleFunc("GET /api/v1/queues", s.listQueues)
	mux.HandleFunc("POST /api/v1/queues", s.upsertQueue)
	mux.HandleFunc("GET /api/v1/queues/{name}", s.getQueue)
	mux.HandleFunc("GET /api/v1/queues/{name}/pending", s.queuePending)
	mux.HandleFunc("POST /api/v1/queues/{name}/pause", s.pauseQueue(true))
	mux.HandleFunc("POST /api/v1/queues/{name}/resume", s.pauseQueue(false))

	// --- events, automations, webhooks ---
	mux.HandleFunc("GET /api/v1/events", s.listEvents)
	mux.HandleFunc("GET /api/v1/automations", s.listAutomations)
	mux.HandleFunc("POST /api/v1/automations", s.upsertAutomation)
	mux.HandleFunc("DELETE /api/v1/automations/{id}", s.deleteAutomation)
	mux.HandleFunc("POST /api/v1/webhooks/{deployment}", s.webhook)
	mux.HandleFunc("GET /api/v1/stream", s.stream)

	// --- admin: operator accounts ---
	mux.HandleFunc("GET /api/v1/users", s.listUsers)
	mux.HandleFunc("POST /api/v1/users", s.createUser)
	mux.HandleFunc("PATCH /api/v1/users/{id}", s.updateUser)
	mux.HandleFunc("DELETE /api/v1/users/{id}", s.deleteUser)
	mux.HandleFunc("POST /api/v1/users/{id}/reset-link", s.createResetLink)

	// --- worker API: the execution path over HTTP (see worker.go) ---
	s.registerWorkerRoutes(mux)

	// --- admin: External API settings & keys ---
	mux.HandleFunc("GET /api/v1/settings/external-api", s.getExternalAPISettings)
	mux.HandleFunc("PUT /api/v1/settings/external-api", s.putExternalAPISettings)
	mux.HandleFunc("GET /api/v1/settings/log-retention", s.getLogRetention)
	mux.HandleFunc("PUT /api/v1/settings/log-retention", s.putLogRetention)
	mux.HandleFunc("GET /api/v1/settings/git", s.getGitConnection)
	mux.HandleFunc("PUT /api/v1/settings/git", s.putGitConnection)
	mux.HandleFunc("GET /api/v1/api-roles", s.apiRoles)
	mux.HandleFunc("GET /api/v1/api-keys", s.listAPIKeys)
	mux.HandleFunc("POST /api/v1/api-keys", s.createAPIKey)
	mux.HandleFunc("GET /api/v1/api-keys/{id}", s.getAPIKey)
	mux.HandleFunc("PATCH /api/v1/api-keys/{id}", s.updateAPIKey)
	mux.HandleFunc("DELETE /api/v1/api-keys/{id}", s.deleteAPIKey)
	mux.HandleFunc("POST /api/v1/api-keys/{id}/rotate", s.rotateAPIKey)
	mux.HandleFunc("GET /api/v1/api-keys/{id}/history", s.apiKeyHistory)

	// Prometheus scrape target. Open like /api/v1/health — scrapers carry no
	// credential — and only mounted when metrics are enabled.
	if reg := s.cfg.Metrics.Registry(); reg != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	}

	if s.cfg.UIEnabled {
		mux.Handle("GET /", s.uiHandler())
	}

	// withMetrics sits directly around each mux so it observes the request
	// *after* the auth and otelhttp layers have taken their own copies — that is
	// the only place http.Request.Pattern (set by the mux on match) is visible.
	operator := s.withAuth(s.withMetrics(mux))
	external := s.withMetrics(s.externalMux())

	root := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/external/") {
			external.ServeHTTP(w, r)
			return
		}
		operator.ServeHTTP(w, r)
	})

	// Server spans + inbound trace-context extraction. otelhttp is a no-op
	// tracer when tracing is disabled, so this is always safe to install.
	return otelhttp.NewHandler(s.withCORS(root), "primeflow.http")
}

// withMetrics records the HTTP RED series. The route label is the ServeMux
// pattern (e.g. "GET /api/v1/runs/{id}"), never the raw path, so cardinality
// stays bounded.
func (s *Server) withMetrics(next http.Handler) http.Handler {
	if s.cfg.Metrics == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" {
			route = "other"
		}
		s.cfg.Metrics.ObserveHTTP(route, r.Method, codeClass(sw.code), time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(c int) { r.code = c; r.ResponseWriter.WriteHeader(c) }
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func codeClass(c int) string {
	switch {
	case c >= 500:
		return "5xx"
	case c >= 400:
		return "4xx"
	case c >= 300:
		return "3xx"
	case c >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

// ListenAndServe runs the HTTP server until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.cfg.Addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	s.log.Info("api listening", "addr", s.cfg.Addr, "ui", s.cfg.UIEnabled,
		"machine_token", s.cfg.APIToken != "", "cookie_secure", s.cfg.CookieSecure)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// ------------------------------------------------------------ middleware ---

func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.CORSOrigin != "" {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", s.cfg.CORSOrigin)
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-API-Key, X-CSRF-Token")
			h.Set("Access-Control-Allow-Methods", "GET, POST, PATCH, PUT, DELETE, OPTIONS")
			h.Set("Access-Control-Allow-Credentials", "true")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// unauthenticated routes on the operator surface: the health probe, the login
// and logout endpoints, and the login page itself.
func openOperatorPath(method, path string) bool {
	switch {
	case path == "/api/v1/health" || path == "/metrics":
		return true
	case path == "/api/v1/auth/login" && method == http.MethodPost:
		return true
	case path == "/api/v1/auth/logout" && method == http.MethodPost:
		return true
	case path == "/api/v1/auth/config" && method == http.MethodGet:
		return true
	case path == "/api/v1/auth/reset" && method == http.MethodPost:
		return true
	case strings.HasPrefix(path, "/api/v1/auth/oidc/"):
		return true
	case path == "/login.html" || path == "/reset.html" || path == "/primeflow.png" || path == "/favicon.ico":
		return true
	}
	return false
}

// adminOperatorPath reports whether a route requires an admin principal (user
// management, API-key management, instance settings).
func adminOperatorPath(path string) bool {
	return strings.HasPrefix(path, "/api/v1/users") ||
		strings.HasPrefix(path, "/api/v1/api-keys") ||
		strings.HasPrefix(path, "/api/v1/api-roles") ||
		strings.HasPrefix(path, "/api/v1/settings/")
}

func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		if openOperatorPath(r.Method, path) {
			next.ServeHTTP(w, r)
			return
		}

		p, csrfOK := s.resolvePrincipal(r)
		if p == nil {
			// A browser navigating to a page gets bounced to the login screen;
			// an API client gets a clean 401.
			if r.Method == http.MethodGet && !strings.HasPrefix(path, "/api/") {
				http.Redirect(w, r, "/login.html", http.StatusSeeOther)
				return
			}
			writeErr(w, http.StatusUnauthorized, errors.New("authentication required"))
			return
		}

		// Session-authenticated writes must carry a matching CSRF token. Machine
		// tokens are exempt: they are not ambient credentials.
		if !p.Machine && isWrite(r.Method) && !csrfOK {
			writeErr(w, http.StatusForbidden, errors.New("missing or invalid CSRF token"))
			return
		}

		if strings.HasPrefix(path, "/api/") {
			if adminOperatorPath(path) {
				if !p.canAdmin() {
					writeErr(w, http.StatusForbidden, errors.New("admin role required"))
					return
				}
			} else if isWrite(r.Method) && !p.canMutate() {
				writeErr(w, http.StatusForbidden, errors.New("this account is read-only"))
				return
			}
		}

		next.ServeHTTP(w, r.WithContext(withPrincipal(r.Context(), p)))
	})
}

func isWrite(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	return true
}

// resolvePrincipal identifies the caller. It returns (nil, false) when there is
// no valid credential. The bool is whether a CSRF check passed (always true for
// machine tokens, which do not need one).
func (s *Server) resolvePrincipal(r *http.Request) (*principal, bool) {
	// 1. Static machine token (workers, CLI). Also accepted as ?token= so the
	//    SSE EventSource, which cannot set headers, still works for scripts.
	if s.cfg.APIToken != "" {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got == s.cfg.APIToken || r.URL.Query().Get("token") == s.cfg.APIToken {
			return &principal{Machine: true, Email: "machine-token", Role: authn.RoleAdmin}, true
		}
	}

	// 2. Browser session cookie.
	c, err := r.Cookie(sessionCookie)
	if err != nil || c.Value == "" {
		return nil, false
	}
	_, user, err := s.store.GetSession(r.Context(), c.Value, time.Now().UTC())
	if err != nil {
		return nil, false
	}
	// Slide the expiry forward, at most once a minute worth of writes.
	_ = s.store.TouchSession(r.Context(), c.Value, time.Now().UTC(), time.Now().Add(s.cfg.SessionTTL))

	csrfOK := false
	if cc, err := r.Cookie(csrfCookie); err == nil && cc.Value != "" {
		csrfOK = subtleEqual(cc.Value, r.Header.Get(csrfHeader))
	}
	return &principal{
		UserID: user.ID, Email: user.Email, Role: authn.Role(user.Role),
	}, csrfOK
}

func subtleEqual(a, b string) bool {
	if len(a) != len(b) || a == "" {
		return false
	}
	var v byte
	for i := 0; i < len(a); i++ {
		v |= a[i] ^ b[i]
	}
	return v == 0
}

// ----------------------------------------------------------- client IP ---

func (s *Server) clientIPAddr(r *http.Request) net.IP {
	return apiauth.ClientIP(r, s.cfg.TrustedProxyCIDRs)
}

func (s *Server) clientIP(r *http.Request) string {
	if ip := s.clientIPAddr(r); ip != nil {
		return ip.String()
	}
	return ""
}

// -------------------------------------------------------------- helpers ---

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

// fail maps store errors onto status codes so clients can react sensibly.
func fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, err)
	case errors.Is(err, store.ErrConflict):
		writeErr(w, http.StatusConflict, err)
	default:
		var it core.ErrInvalidTransition
		if errors.As(err, &it) {
			writeErr(w, http.StatusConflict, err)
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
	}
}

func decode(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && err.Error() != "EOF" {
		return err
	}
	return nil
}

func intParam(r *http.Request, name string, def int) int {
	if v := r.URL.Query().Get(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func csvParam(r *http.Request, name string) []string {
	v := r.URL.Query().Get(name)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func newID() string { return uuid.NewString() }
