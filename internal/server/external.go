package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/primex/primeflow/internal/apiauth"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

// The External API is a role-gated, key-authenticated projection of PrimeFlow's
// own resources, mounted at /api/external/v1. It shares the store with the
// operator API but nothing else: its own auth, its own rate limiting, its own
// response shaping.

type extKeyCtxKey int

const extKeyKey extKeyCtxKey = 0

func extKeyFrom(ctx context.Context) *core.APIKey {
	k, _ := ctx.Value(extKeyKey).(*core.APIKey)
	return k
}

// externalMux builds the External API router. Every route is wrapped by ext(),
// which runs the full auth chain and the per-route scope check.
func (s *Server) externalMux() http.Handler {
	m := http.NewServeMux()
	get := func(path string, scope apiauth.Scope, h http.HandlerFunc) {
		m.HandleFunc("GET /api/external/v1"+path, s.ext(scope, h))
	}
	post := func(path string, scope apiauth.Scope, h http.HandlerFunc) {
		m.HandleFunc("POST /api/external/v1"+path, s.ext(scope, h))
	}

	get("/health", "", s.extHealth)
	get("/runs", apiauth.ScopeReadRuns, s.extListRuns)
	post("/runs", apiauth.ScopeWriteRuns, s.extCreateRun)
	get("/runs/{id}", apiauth.ScopeReadRuns, s.extGetRun)
	get("/runs/{id}/tasks", apiauth.ScopeReadRuns, s.extRunTasks)
	get("/runs/{id}/logs", apiauth.ScopeReadRuns, s.extRunLogs)
	get("/runs/{id}/artifacts", apiauth.ScopeReadRuns, s.extRunArtifacts)
	get("/deployments", apiauth.ScopeReadDeployments, s.extListDeployments)
	get("/deployments/{id}", apiauth.ScopeReadDeployments, s.extGetDeployment)
	post("/deployments/{id}/run", apiauth.ScopeWriteRuns, s.extRunDeployment)
	get("/queues", apiauth.ScopeReadQueues, s.extListQueues)
	get("/queues/{name}/pending", apiauth.ScopeReadQueues, s.extQueuePending)
	get("/events", apiauth.ScopeReadEvents, s.extListEvents)
	return m
}

// ext wraps an External API handler with the whole enforcement chain:
// master switch, key resolution, IP allowlist, mutual-TLS requirement, scope,
// and rate limit. The distinction the mockup calls for is preserved: a bad or
// disabled key is 401, a valid key whose role lacks the scope is 403.
func (s *Server) ext(scope apiauth.Scope, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, err := s.store.GetExternalAPISettings(r.Context())
		if err != nil {
			fail(w, err)
			return
		}
		if !cfg.Enabled {
			writeErr(w, http.StatusUnauthorized, errors.New("external API is disabled"))
			return
		}

		secret := bearerOrHeader(r)
		if secret == "" {
			writeErr(w, http.StatusUnauthorized, errors.New("missing API key"))
			return
		}
		prefix := apiauth.PrefixOf(secret)
		if prefix == "" {
			writeErr(w, http.StatusUnauthorized, errors.New("malformed API key"))
			return
		}
		key, err := s.store.GetAPIKeyByPrefix(r.Context(), prefix)
		if err != nil || !apiauth.SecretMatches(key.SecretHash, secret) {
			writeErr(w, http.StatusUnauthorized, errors.New("invalid API key"))
			return
		}
		now := time.Now().UTC()
		if !key.Active || key.Expired(now) {
			writeErr(w, http.StatusUnauthorized, errors.New("API key is inactive or expired"))
			return
		}

		clientIP := s.clientIPAddr(r)
		if len(key.IPAllowlist) > 0 {
			nets := apiauth.ParseCIDRs(strings.Join(key.IPAllowlist, ","))
			if clientIP == nil || !apiauth.IPInAny(clientIP, nets) {
				s.denyKey(r, key.ID, "ip-not-allowlisted", clientIP.String())
				writeErr(w, http.StatusUnauthorized, errors.New("client address is not allow-listed for this key"))
				return
			}
		}
		if key.RequireMTLS {
			verified := apiauth.ViaTrustedProxy(r, s.cfg.TrustedProxyCIDRs) &&
				strings.EqualFold(r.Header.Get("X-SSL-Client-Verify"), "SUCCESS")
			if !verified {
				s.denyKey(r, key.ID, "mtls-not-verified", clientIP.String())
				writeErr(w, http.StatusUnauthorized, errors.New("this key requires a verified client certificate"))
				return
			}
		}

		role, _ := apiauth.LookupRole(key.Role)
		if scope != "" && !role.Has(scope) {
			s.denyKey(r, key.ID, "scope-denied:"+string(scope), clientIP.String())
			writeErr(w, http.StatusForbidden,
				errors.New("this key's role ("+role.Label+") lacks the "+string(scope)+" scope"))
			return
		}

		limit := cfg.DefaultRateLimitPerMin
		if key.RateLimitPerMin != nil {
			limit = *key.RateLimitPerMin
		}
		if ok, retry := s.rateLimiter.Allow(key.ID, limit); !ok {
			w.Header().Set("Retry-After", secondsString(retry))
			writeErr(w, http.StatusTooManyRequests, errors.New("rate limit exceeded"))
			return
		}

		_ = s.store.TouchAPIKey(r.Context(), key.ID, now)
		next(w, r.WithContext(context.WithValue(r.Context(), extKeyKey, key)))
	}
}

func bearerOrHeader(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("X-API-Key")); v != "" {
		return v
	}
	if v := r.Header.Get("Authorization"); strings.HasPrefix(v, "Bearer ") {
		return strings.TrimSpace(strings.TrimPrefix(v, "Bearer "))
	}
	return ""
}

func (s *Server) denyKey(r *http.Request, keyID, reason, ip string) {
	_ = s.store.AppendAPIKeyEvent(r.Context(), &core.APIKeyEvent{
		APIKeyID: keyID, Actor: "external-caller", Action: "auth-denied",
		Detail: mustJSON(map[string]string{"reason": reason, "ip": ip, "path": r.URL.Path}),
	})
}

// --------------------------------------------------------------- handlers ---

func (s *Server) extHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": time.Now().UTC()})
}

func (s *Server) extListRuns(w http.ResponseWriter, r *http.Request) {
	f := store.FlowRunFilter{
		WorkQueues:   csvParam(r, "queue"),
		FlowNames:    csvParam(r, "flow"),
		DeploymentID: r.URL.Query().Get("deployment_id"),
		Search:       r.URL.Query().Get("search"),
		Limit:        intParam(r, "limit", 50),
		Offset:       intParam(r, "offset", 0),
	}
	for _, st := range csvParam(r, "state") {
		t := core.StateType(st)
		if !t.Valid() {
			writeErr(w, http.StatusBadRequest, errors.New("unknown state "+st))
			return
		}
		f.States = append(f.States, t)
	}
	runs, err := s.store.ListFlowRuns(r.Context(), f)
	if err != nil {
		fail(w, err)
		return
	}
	total, err := s.store.CountFlowRuns(r.Context(), f)
	if err != nil {
		fail(w, err)
		return
	}
	if extKeyFrom(r.Context()).RedactPII {
		for i := range runs {
			redactRun(&runs[i])
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": total, "runs": runs})
}

func (s *Server) extGetRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.GetFlowRun(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	if extKeyFrom(r.Context()).RedactPII {
		redactRun(run)
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) extCreateRun(w http.ResponseWriter, r *http.Request) {
	s.createRun(w, r) // identical semantics; role scope already checked by ext()
}

func (s *Server) extRunDeployment(w http.ResponseWriter, r *http.Request) {
	s.runDeployment(w, r)
}

func (s *Server) extRunTasks(w http.ResponseWriter, r *http.Request) {
	ts, err := s.store.ListTaskRuns(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	if extKeyFrom(r.Context()).RedactPII {
		for i := range ts {
			ts[i].Result = nil
		}
	}
	writeJSON(w, http.StatusOK, ts)
}

func (s *Server) extRunLogs(w http.ResponseWriter, r *http.Request) {
	logs, err := s.store.ListLogs(r.Context(), r.PathValue("id"),
		int64(intParam(r, "after", 0)), intParam(r, "limit", 500))
	if err != nil {
		fail(w, err)
		return
	}
	if extKeyFrom(r.Context()).RedactPII {
		for i := range logs {
			logs[i].Fields = nil
		}
	}
	writeJSON(w, http.StatusOK, logs)
}

func (s *Server) extRunArtifacts(w http.ResponseWriter, r *http.Request) {
	as, err := s.store.ListArtifacts(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	if extKeyFrom(r.Context()).RedactPII {
		for i := range as {
			as[i].Data = json.RawMessage("null")
		}
	}
	writeJSON(w, http.StatusOK, as)
}

func (s *Server) extListDeployments(w http.ResponseWriter, r *http.Request) {
	ds, err := s.store.ListDeployments(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	if extKeyFrom(r.Context()).RedactPII {
		for i := range ds {
			ds[i].Parameters = nil
		}
	}
	writeJSON(w, http.StatusOK, ds)
}

func (s *Server) extGetDeployment(w http.ResponseWriter, r *http.Request) {
	d, err := s.store.GetDeployment(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	if extKeyFrom(r.Context()).RedactPII {
		d.Parameters = nil
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) extListQueues(w http.ResponseWriter, r *http.Request) {
	qs, err := s.store.QueueStats(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, qs)
}

func (s *Server) extQueuePending(w http.ResponseWriter, r *http.Request) {
	runs, err := s.store.PendingInQueue(r.Context(), r.PathValue("name"), intParam(r, "limit", 100))
	if err != nil {
		fail(w, err)
		return
	}
	if extKeyFrom(r.Context()).RedactPII {
		for i := range runs {
			redactRun(&runs[i])
		}
	}
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) extListEvents(w http.ResponseWriter, r *http.Request) {
	evs, err := s.store.ListEvents(r.Context(), intParam(r, "limit", 100))
	if err != nil {
		fail(w, err)
		return
	}
	if extKeyFrom(r.Context()).RedactPII {
		for i := range evs {
			evs[i].Payload = nil
		}
	}
	writeJSON(w, http.StatusOK, evs)
}

// redactRun nulls the fields that can carry customer data: run parameters and
// the flow's return value. Counts, states, timings, names, queue and priority
// are left intact so analytics still work.
func redactRun(r *core.FlowRun) {
	r.Parameters = nil
	r.Result = nil
}
