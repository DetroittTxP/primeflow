// Package server exposes the REST API, the live event stream and the operator
// UI.
//
// The API is the only way anything mutates state: workers, the CLI and the UI
// all speak it, so there is one place where the orchestration rules and the
// admin controls are enforced.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/bus"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/store"
)

// Config configures the HTTP server.
type Config struct {
	Addr string
	// APIToken, when set, is required as "Authorization: Bearer <token>" on
	// every /api route. Leave empty only on a trusted network.
	APIToken string
	// UIEnabled serves the bundled operator console at /.
	UIEnabled bool
	// CORSOrigin allows a separately hosted front end (e.g. the PrimeX
	// console) to call this API.
	CORSOrigin string
}

// Server holds the API dependencies.
type Server struct {
	store  store.Store
	bus    bus.Bus
	events *events.Emitter
	log    *slog.Logger
	cfg    Config
}

// New builds a server.
func New(s store.Store, b bus.Bus, em *events.Emitter, log *slog.Logger, cfg Config) *Server {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	return &Server{store: s, bus: b, events: em, log: log, cfg: cfg}
}

// Handler returns the fully wired HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// --- health & catalogue ---
	mux.HandleFunc("GET /api/v1/health", s.health)
	mux.HandleFunc("GET /api/v1/flows", s.listFlows)
	mux.HandleFunc("GET /api/v1/workers", s.listWorkers)
	mux.HandleFunc("GET /api/v1/summary", s.summary)

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

	if s.cfg.UIEnabled {
		mux.Handle("GET /", s.uiHandler())
	}

	return s.withMiddleware(mux)
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
	s.log.Info("api listening", "addr", s.cfg.Addr, "ui", s.cfg.UIEnabled, "auth", s.cfg.APIToken != "")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// ------------------------------------------------------------ middleware ---

func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.CORSOrigin != "" {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", s.cfg.CORSOrigin)
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			h.Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		// The health endpoint stays open so load balancers do not need a token.
		if s.cfg.APIToken != "" && strings.HasPrefix(r.URL.Path, "/api/") &&
			r.URL.Path != "/api/v1/health" {
			if !s.authorised(r) {
				writeErr(w, http.StatusUnauthorized, errors.New("missing or invalid bearer token"))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) authorised(r *http.Request) bool {
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if got == s.cfg.APIToken {
		return true
	}
	// The SSE stream is opened by EventSource, which cannot set headers.
	return r.URL.Query().Get("token") == s.cfg.APIToken
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
