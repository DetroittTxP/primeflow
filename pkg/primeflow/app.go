// Package primeflow wires the pieces together so an application that just
// wants to run flows needs a short main function.
//
//	func main() {
//	    sdk.Flow("provision-vm", provisionVM)
//	    log.Fatal(primeflow.RunWorker(context.Background(), primeflow.Options{}))
//	}
//
// Options are read from the environment by default, so the same binary works in
// docker-compose and on Kubernetes without code changes.
package primeflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/apiauth"
	"github.com/primex/primeflow/internal/authn"
	"github.com/primex/primeflow/internal/automations"
	"github.com/primex/primeflow/internal/bus"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/engine"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/metrics"
	"github.com/primex/primeflow/internal/oidcauth"
	"github.com/primex/primeflow/internal/otelinit"
	"github.com/primex/primeflow/internal/ratelimit"
	"github.com/primex/primeflow/internal/scheduler"
	"github.com/primex/primeflow/internal/server"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/internal/store/postgres"
	"github.com/primex/primeflow/internal/store/remote"
	"github.com/primex/primeflow/internal/worker"
	"github.com/primex/primeflow/pkg/sdk"
)

// Options configure a PrimeFlow process. Any zero field falls back to the
// matching PRIMEFLOW_* environment variable, then to a sensible default.
type Options struct {
	// DatabaseURL is the Postgres DSN. Required.
	DatabaseURL string
	// RedisURL enables the low-latency notification bus. Optional: without it
	// the process falls back to polling, which is slower but fully correct.
	RedisURL string
	// NatsURL selects NATS as the notification bus. Takes precedence over
	// RedisURL when both are set. Same "accelerator, never a dependency"
	// contract: a dial failure degrades to polling, it does not stop start-up.
	NatsURL string

	// APIURL and WorkerToken put a worker in remote mode: it reaches the
	// orchestrator through the worker API instead of a database connection,
	// which is what a site allowed nothing but outbound 443 needs. Set both, and
	// DatabaseURL is neither required nor used. The token is a pool-scoped
	// api-worker key.
	APIURL      string
	WorkerToken string

	// Server options.
	HTTPAddr   string
	APIToken   string
	DisableUI  bool
	CORSOrigin string

	// TrustedProxyCIDRs is a comma-separated list of networks an edge proxy may
	// connect from. Only then are X-Forwarded-For and forwarded client-cert
	// state believed (per-key IP allowlists and mutual-TLS enforcement).
	TrustedProxyCIDRs string
	// SessionTTL is how long an operator login lasts; it slides on use.
	SessionTTL time.Duration
	// CookieSecure marks session cookies Secure. Defaults to true when
	// TrustedProxyCIDRs is set (i.e. there is a TLS-terminating proxy in front).
	CookieSecure *bool

	// AdminEmail / AdminPassword seed the first operator account on a fresh
	// database. Ignored once any user exists.
	AdminEmail    string
	AdminPassword string

	// OIDC single sign-on. Enabled when OIDCIssuer and OIDCClientID are set.
	// OIDCRoleMap is "claimValue=role,..." keyed on OIDCRoleClaim.
	OIDCIssuer       string
	OIDCClientID     string
	OIDCClientSecret string
	OIDCRedirectURL  string
	OIDCScopes       string
	OIDCDefaultRole  string
	OIDCRoleClaim    string
	OIDCRoleMap      string
	OIDCLabel        string
	// ResetTTL bounds admin-issued password-reset links (default 1h).
	ResetTTL time.Duration

	// LogRetention, when > 0, sets the pf_logs cleanup horizon on a
	// migrate-on-start process (PRIMEFLOW_LOG_RETENTION, e.g. "720h").
	LogRetention time.Duration

	// Worker options.
	WorkerName        string
	Queues            []string
	Concurrency       int
	LeaseDuration     time.Duration
	PollInterval      time.Duration
	MaxSubflowDepth   int
	WorkerMetricsAddr string

	// Push-worker options (RunPushWorker): the address the receiver listens on
	// for POST /run, and the shared secret the server signs dispatches with.
	PushAddr   string
	PushSecret string

	// Registry holds the flows this process can execute. Defaults to sdk.Default.
	Registry *sdk.Registry

	// Logger; defaults to a JSON slog logger at info level.
	Logger *slog.Logger

	// MigrateOnStart applies the schema before serving. Safe and idempotent.
	MigrateOnStart bool
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (o *Options) applyEnv() {
	o.DatabaseURL = firstNonEmpty(o.DatabaseURL, os.Getenv("PRIMEFLOW_DATABASE_URL"), os.Getenv("DATABASE_URL"))
	o.RedisURL = firstNonEmpty(o.RedisURL, os.Getenv("PRIMEFLOW_REDIS_URL"), os.Getenv("REDIS_URL"))
	o.NatsURL = firstNonEmpty(o.NatsURL, os.Getenv("PRIMEFLOW_NATS_URL"), os.Getenv("NATS_URL"))
	o.APIURL = firstNonEmpty(o.APIURL, os.Getenv("PRIMEFLOW_API_URL"))
	o.WorkerToken = firstNonEmpty(o.WorkerToken, os.Getenv("PRIMEFLOW_WORKER_TOKEN"))
	o.HTTPAddr = firstNonEmpty(o.HTTPAddr, os.Getenv("PRIMEFLOW_HTTP_ADDR"), ":8080")
	o.APIToken = firstNonEmpty(o.APIToken, os.Getenv("PRIMEFLOW_API_TOKEN"))
	o.CORSOrigin = firstNonEmpty(o.CORSOrigin, os.Getenv("PRIMEFLOW_CORS_ORIGIN"))
	o.WorkerName = firstNonEmpty(o.WorkerName, os.Getenv("PRIMEFLOW_WORKER_NAME"))
	o.TrustedProxyCIDRs = firstNonEmpty(o.TrustedProxyCIDRs, os.Getenv("PRIMEFLOW_TRUSTED_PROXY_CIDRS"))
	o.AdminEmail = firstNonEmpty(o.AdminEmail, os.Getenv("PRIMEFLOW_ADMIN_EMAIL"))
	o.AdminPassword = firstNonEmpty(o.AdminPassword, os.Getenv("PRIMEFLOW_ADMIN_PASSWORD"))
	o.OIDCIssuer = firstNonEmpty(o.OIDCIssuer, os.Getenv("PRIMEFLOW_OIDC_ISSUER"))
	o.OIDCClientID = firstNonEmpty(o.OIDCClientID, os.Getenv("PRIMEFLOW_OIDC_CLIENT_ID"))
	o.OIDCClientSecret = firstNonEmpty(o.OIDCClientSecret, os.Getenv("PRIMEFLOW_OIDC_CLIENT_SECRET"))
	o.OIDCRedirectURL = firstNonEmpty(o.OIDCRedirectURL, os.Getenv("PRIMEFLOW_OIDC_REDIRECT_URL"))
	o.OIDCScopes = firstNonEmpty(o.OIDCScopes, os.Getenv("PRIMEFLOW_OIDC_SCOPES"))
	o.OIDCDefaultRole = firstNonEmpty(o.OIDCDefaultRole, envOr("PRIMEFLOW_OIDC_DEFAULT_ROLE", "viewer"))
	o.OIDCRoleClaim = firstNonEmpty(o.OIDCRoleClaim, os.Getenv("PRIMEFLOW_OIDC_ROLE_CLAIM"))
	o.OIDCRoleMap = firstNonEmpty(o.OIDCRoleMap, os.Getenv("PRIMEFLOW_OIDC_ROLE_MAP"))
	o.OIDCLabel = firstNonEmpty(o.OIDCLabel, envOr("PRIMEFLOW_OIDC_LABEL", "SSO"))
	if o.ResetTTL == 0 {
		if d, err := time.ParseDuration(envOr("PRIMEFLOW_RESET_TTL", "1h")); err == nil {
			o.ResetTTL = d
		}
	}
	if o.LogRetention == 0 {
		if d, err := time.ParseDuration(os.Getenv("PRIMEFLOW_LOG_RETENTION")); err == nil {
			o.LogRetention = d
		}
	}

	if o.SessionTTL == 0 {
		if d, err := time.ParseDuration(envOr("PRIMEFLOW_SESSION_TTL", "168h")); err == nil {
			o.SessionTTL = d
		}
	}
	if o.CookieSecure == nil {
		v := strings.EqualFold(os.Getenv("PRIMEFLOW_COOKIE_SECURE"), "true") || o.TrustedProxyCIDRs != ""
		if raw := os.Getenv("PRIMEFLOW_COOKIE_SECURE"); raw != "" {
			v = strings.EqualFold(raw, "true") || raw == "1"
		}
		o.CookieSecure = &v
	}

	if len(o.Queues) == 0 {
		if v := envOr("PRIMEFLOW_QUEUES", "default"); v != "" {
			for _, q := range strings.Split(v, ",") {
				if q = strings.TrimSpace(q); q != "" {
					o.Queues = append(o.Queues, q)
				}
			}
		}
	}
	if o.Concurrency == 0 {
		if n, err := strconv.Atoi(envOr("PRIMEFLOW_CONCURRENCY", "4")); err == nil {
			o.Concurrency = n
		}
	}
	if o.LeaseDuration == 0 {
		if d, err := time.ParseDuration(envOr("PRIMEFLOW_LEASE", "60s")); err == nil {
			o.LeaseDuration = d
		}
	}
	if o.PollInterval == 0 {
		// Two seconds suits a worker sitting beside the database. A worker
		// reaching the orchestrator over a WAN multiplies that by every site,
		// and the wake-up stream is what keeps latency low there, so the poll
		// becomes a backstop rather than the mechanism. Fifteen seconds is what
		// Prefect polls at, for the same reason.
		def := "2s"
		if o.Remote() {
			def = "15s"
		}
		if d, err := time.ParseDuration(envOr("PRIMEFLOW_POLL", def)); err == nil {
			o.PollInterval = d
		}
	}
	if o.MaxSubflowDepth == 0 {
		if n, err := strconv.Atoi(envOr("PRIMEFLOW_MAX_SUBFLOW_DEPTH", "8")); err == nil && n > 0 {
			o.MaxSubflowDepth = n
		}
	}
	o.WorkerMetricsAddr = firstNonEmpty(o.WorkerMetricsAddr, envOr("PRIMEFLOW_METRICS_ADDR", ":9090"))
	o.PushAddr = firstNonEmpty(o.PushAddr, envOr("PRIMEFLOW_PUSH_ADDR", ":8090"))
	o.PushSecret = firstNonEmpty(o.PushSecret, os.Getenv("PRIMEFLOW_PUSH_SECRET"))
	if o.Registry == nil {
		o.Registry = sdk.Default
	}
	if o.Logger == nil {
		level := slog.LevelInfo
		if strings.EqualFold(os.Getenv("PRIMEFLOW_LOG_LEVEL"), "debug") {
			level = slog.LevelDebug
		}
		o.Logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	}
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

// App holds initialised infrastructure so the run modes can share it.
type App struct {
	Options Options
	// Store is the full persistence surface, and is nil in remote mode: a
	// worker reaching the API has no database and nothing that needs one.
	Store store.Store
	// WorkerStore is what the engine and the worker run against — either the
	// Postgres store or the API client. Always set.
	WorkerStore store.WorkerStore
	Bus         bus.Bus
	Events      *events.Emitter
	Metrics     *metrics.Metrics
	Log         *slog.Logger

	loginThrottle ratelimit.Throttle
	keyLimiter    ratelimit.Limiter
	rlClose       func() error
	oidc          *oidcauth.Provider

	holder       string
	otelShutdown otelinit.ShutdownFunc
}

// Remote reports whether this process should reach the orchestrator through its
// API instead of a database connection.
func (o Options) Remote() bool { return o.APIURL != "" && o.WorkerToken != "" }

// openRemote builds a worker that holds no database credential.
//
// Everything the local path sets up around the store is deliberately absent.
// There is no emitter: the server records transitions on the worker's behalf,
// which is what keeps a site from being able to write the event log automations
// act on. There is no admin bootstrap, no migration, no leader election — a
// worker owns none of that. The bus is whatever the environment offers, which
// at a site reachable only over 443 means the in-process one, so dispatch falls
// back to polling until the wake-up channel lands.
func openRemote(ctx context.Context, o Options) (*App, error) {
	if o.DatabaseURL != "" {
		o.Logger.Warn("PRIMEFLOW_DATABASE_URL is set but ignored: this worker runs against the API",
			"api", o.APIURL)
	}
	name := o.WorkerName
	if name == "" {
		name, _ = os.Hostname()
	}
	rs, err := remote.New(remote.Config{
		BaseURL:  o.APIURL,
		Token:    o.WorkerToken,
		WorkerID: name + "-" + uuid.NewString()[:8],
		Timeout:  o.LeaseDuration,
		Logger:   o.Logger,
	})
	if err != nil {
		return nil, err
	}
	o.Logger.Info("worker store: primeflow api", "url", o.APIURL, "queues", o.Queues)

	// The wake-up channel rides the same 443. It is an accelerator: a failure
	// to build it leaves the worker polling, which is slower and just as
	// correct, so it never stops start-up.
	var b bus.Bus = bus.NewInMemory()
	if rb, berr := remote.NewBus(remote.Config{
		BaseURL: o.APIURL, Token: o.WorkerToken, WorkerID: name, Logger: o.Logger,
	}); berr != nil {
		o.Logger.Warn("wake-up stream unavailable; falling back to polling", "err", berr)
	} else {
		b = rb
	}

	shutdown, err := otelinit.Setup(ctx, "primeflow-worker", version())
	if err != nil {
		shutdown = func(context.Context) error { return nil }
	}
	return &App{
		Options:      o,
		WorkerStore:  rs,
		Bus:          b,
		Metrics:      metrics.New(nil),
		Log:          o.Logger,
		holder:       name + "-" + uuid.NewString()[:8],
		otelShutdown: shutdown,
	}, nil
}

// Open connects to Postgres and (if configured) Redis.
func Open(ctx context.Context, o Options) (*App, error) {
	o.applyEnv()
	if o.Remote() {
		return openRemote(ctx, o)
	}
	if o.DatabaseURL == "" {
		return nil, errors.New("primeflow: set PRIMEFLOW_DATABASE_URL, or PRIMEFLOW_API_URL and PRIMEFLOW_WORKER_TOKEN to run against the API")
	}
	st, err := postgres.Open(ctx, o.DatabaseURL, 0)
	if err != nil {
		return nil, err
	}
	if o.MigrateOnStart {
		if err := st.Migrate(ctx); err != nil {
			_ = st.Close()
			return nil, fmt.Errorf("migrate: %w", err)
		}
	}

	// Transport precedence: NATS, then Redis, then the in-process bus. Every
	// path degrades to polling on a dial failure — the bus is never a
	// dependency, only a latency optimisation.
	var b bus.Bus
	switch {
	case o.NatsURL != "":
		nb, err := bus.NewNATS(ctx, o.NatsURL, o.Logger)
		if err != nil {
			o.Logger.Warn("nats unavailable; falling back to polling", "err", err)
			b = bus.NewInMemory()
		} else {
			o.Logger.Info("notification bus: nats", "url", o.NatsURL)
			b = nb
		}
	case o.RedisURL != "":
		rb, err := bus.NewRedis(ctx, o.RedisURL, os.Getenv("PRIMEFLOW_REDIS_PASSWORD"), 0, o.Logger)
		if err != nil {
			o.Logger.Warn("redis unavailable; falling back to polling", "err", err)
			b = bus.NewInMemory()
		} else {
			o.Logger.Info("notification bus: redis")
			b = rb
		}
	default:
		o.Logger.Info("notification bus: in-process (no PRIMEFLOW_NATS_URL / PRIMEFLOW_REDIS_URL)")
		b = bus.NewInMemory()
	}

	if o.MigrateOnStart {
		bootstrapAdmin(ctx, st, o)
		bootstrapSettings(ctx, st, o)
	}

	// Tracing: a no-op (and free) unless an OTLP endpoint is configured.
	shutdown, err := otelinit.Setup(ctx, "primeflow", version())
	if err != nil {
		o.Logger.Warn("otel setup failed; continuing without tracing", "err", err)
		shutdown = func(context.Context) error { return nil }
	} else if otelinit.Enabled() {
		o.Logger.Info("tracing enabled (OTLP)")
	}

	// Rate limiting: shared via Redis when a Redis URL is set (independent of the
	// bus choice), in-process otherwise.
	lt, rl, rlClose := ratelimit.New(ctx, ratelimit.Config{
		RedisURL:      o.RedisURL,
		RedisPassword: os.Getenv("PRIMEFLOW_REDIS_PASSWORD"),
		Logger:        o.Logger,
	}, authn.NewThrottle(), apiauth.NewRateLimiter())

	// OIDC discovery, when configured. A failure disables SSO but does not stop
	// the server — local password login still works.
	var oidcProvider *oidcauth.Provider
	if o.OIDCIssuer != "" && o.OIDCClientID != "" {
		var scopes []string
		if o.OIDCScopes != "" {
			scopes = strings.Fields(strings.ReplaceAll(o.OIDCScopes, ",", " "))
		}
		p, oerr := oidcauth.New(ctx, oidcauth.Config{
			Issuer: o.OIDCIssuer, ClientID: o.OIDCClientID, ClientSecret: o.OIDCClientSecret,
			RedirectURL: o.OIDCRedirectURL, Scopes: scopes, DefaultRole: o.OIDCDefaultRole,
			RoleClaim: o.OIDCRoleClaim, RoleMap: oidcauth.ParseRoleMap(o.OIDCRoleMap),
		})
		if oerr != nil {
			o.Logger.Warn("OIDC disabled: discovery failed", "issuer", o.OIDCIssuer, "err", oerr)
		} else {
			o.Logger.Info("OIDC single sign-on enabled", "issuer", o.OIDCIssuer)
			oidcProvider = p
		}
	}

	return &App{
		Options: o, Store: st, WorkerStore: st, Bus: b,
		Events:        events.New(st, b, o.Logger),
		Metrics:       metrics.New(st),
		Log:           o.Logger,
		loginThrottle: lt, keyLimiter: rl, rlClose: rlClose, oidc: oidcProvider,
		holder: uuid.NewString(), otelShutdown: shutdown,
	}, nil
}

// version is a best-effort build version for telemetry resource attributes.
func version() string {
	if v := os.Getenv("PRIMEFLOW_VERSION"); v != "" {
		return v
	}
	return "dev"
}

// bootstrapSettings applies env overrides to the pf_settings rows once, on a
// migrate-on-start process.
func bootstrapSettings(ctx context.Context, st store.Store, o Options) {
	if o.LogRetention <= 0 {
		return
	}
	cur, err := st.GetLogRetention(ctx)
	if err != nil {
		return
	}
	cur.Enabled = true
	cur.MaxAgeHours = int(o.LogRetention.Hours())
	if err := st.PutLogRetention(ctx, cur); err != nil {
		o.Logger.Warn("could not apply PRIMEFLOW_LOG_RETENTION", "err", err)
		return
	}
	o.Logger.Info("log retention set from environment", "max_age_hours", cur.MaxAgeHours)
}

// bootstrapAdmin seeds the first operator account from PRIMEFLOW_ADMIN_EMAIL /
// PRIMEFLOW_ADMIN_PASSWORD on an empty user table, and otherwise warns if the
// console would be unreachable.
func bootstrapAdmin(ctx context.Context, st store.Store, o Options) {
	n, err := st.CountUsers(ctx)
	if err != nil || n > 0 {
		return
	}
	if o.AdminEmail == "" || o.AdminPassword == "" {
		o.Logger.Warn("no operator accounts exist and PRIMEFLOW_ADMIN_EMAIL/PRIMEFLOW_ADMIN_PASSWORD are unset; " +
			"the console cannot be used until you run `primeflow user add`")
		return
	}
	if len(o.AdminPassword) < 8 {
		o.Logger.Error("PRIMEFLOW_ADMIN_PASSWORD must be at least 8 characters; skipping admin bootstrap")
		return
	}
	if _, err := st.CreateUser(ctx, store.UserInput{
		ID: uuid.NewString(), Email: o.AdminEmail,
		PasswordHash: authn.HashPassword(o.AdminPassword),
		Role:         string(authn.RoleAdmin), Active: true,
	}); err != nil {
		o.Logger.Error("admin bootstrap failed", "err", err)
		return
	}
	o.Logger.Info("seeded bootstrap admin account", "email", o.AdminEmail)
}

// Close releases resources.
func (a *App) Close() error {
	if a.rlClose != nil {
		_ = a.rlClose()
	}
	if a.otelShutdown != nil {
		_ = a.otelShutdown(context.Background())
	}
	if a.Bus != nil {
		_ = a.Bus.Close()
	}
	if a.Store == nil {
		if c, ok := a.WorkerStore.(interface{ Close() error }); ok {
			return c.Close()
		}
		return nil
	}
	return a.Store.Close()
}

// Migrate applies the schema.
func (a *App) Migrate(ctx context.Context) error {
	if a.Store == nil {
		return errors.New("primeflow: migrations require a database; remote mode runs workers only")
	}
	pg, ok := a.Store.(*postgres.Store)
	if !ok {
		return errors.New("primeflow: migrations require the postgres store")
	}
	return pg.Migrate(ctx)
}

// ServeAPI runs the API, UI, scheduler and automation evaluator.
func (a *App) ServeAPI(ctx context.Context) error {
	if a.Store == nil {
		return errors.New("primeflow: the API server needs a database; remote mode runs workers only")
	}
	cookieSecure := false
	if a.Options.CookieSecure != nil {
		cookieSecure = *a.Options.CookieSecure
	}
	trusted := apiauth.ParseCIDRs(a.Options.TrustedProxyCIDRs)
	if a.Options.TrustedProxyCIDRs != "" {
		a.Log.Info("trusting forwarded headers from proxy networks", "count", len(trusted))
	}
	srv := server.New(a.Store, a.Bus, a.Events, a.Log, server.Config{
		Addr:              a.Options.HTTPAddr,
		APIToken:          a.Options.APIToken,
		UIEnabled:         !a.Options.DisableUI,
		CORSOrigin:        a.Options.CORSOrigin,
		TrustedProxyCIDRs: trusted,
		SessionTTL:        a.Options.SessionTTL,
		CookieSecure:      cookieSecure,
		Metrics:           a.Metrics,
		LoginThrottle:     a.loginThrottle,
		KeyRateLimiter:    a.keyLimiter,
		OIDC:              a.oidc,
		OIDCRedirectURL:   a.Options.OIDCRedirectURL,
		OIDCLabel:         a.Options.OIDCLabel,
		ResetTTL:          a.Options.ResetTTL,
	})
	if a.Store == nil {
		return errors.New("primeflow: the scheduler needs a database; remote mode runs workers only")
	}
	sch := scheduler.New(a.Store, a.Events, a.Log, scheduler.Config{Holder: a.holder})
	autos := automations.New(a.Store, a.Events, a.Log, automations.Config{Holder: a.holder})

	return runAll(ctx,
		func(c context.Context) error { return srv.ListenAndServe(c) },
		func(c context.Context) error { return sch.Run(c) },
		func(c context.Context) error { return autos.Run(c) },
	)
}

// ServeWorker runs a worker for the registered flows.
func (a *App) ServeWorker(ctx context.Context) error {
	w := worker.New(a.workerStore(), a.Bus, a.Options.Registry, a.Events, a.Log, worker.Config{
		Name:            a.Options.WorkerName,
		Queues:          a.Options.Queues,
		Concurrency:     a.Options.Concurrency,
		PollInterval:    a.Options.PollInterval,
		LeaseDuration:   a.Options.LeaseDuration,
		Metrics:         a.Metrics,
		MetricsAddr:     a.Options.WorkerMetricsAddr,
		MaxSubflowDepth: a.Options.MaxSubflowDepth,
		// A worker on the API hears about cancellation twice already — on the
		// wake-up stream and in every heartbeat — so the engine's per-run
		// backstop poll would only add WAN traffic proportional to how busy
		// the site is.
		CancelPollInterval: cancelPollFor(a.Options),
	})
	return w.Run(ctx)
}

// ServeAll runs the API and a worker in one process — the mode to use for
// development, a small deployment, or when PrimeFlow is embedded directly in
// the PrimeX backend.
func (a *App) ServeAll(ctx context.Context) error {
	return runAll(ctx, a.ServeAPI, a.ServeWorker)
}

// pushHost is a stable-ish identity for this receiver's leases.
func (a *App) pushHost() string {
	host := a.Options.WorkerName
	if host == "" {
		host, _ = os.Hostname()
		if host == "" {
			host = "push-worker"
		}
	}
	return host + "-" + uuid.NewString()[:8]
}

// RunOne claims a single scheduled run and executes it with the engine,
// synchronously. It is the unit of work a push-pool receiver performs per
// dispatch (and a handy entry point for embedders / tests).
func (a *App) RunOne(ctx context.Context, runID string) error {
	claimed, err := a.claimForPush(ctx, runID)
	if err != nil {
		return err
	}
	a.executePushRun(context.WithoutCancel(ctx), claimed)
	return nil
}

func (a *App) claimForPush(ctx context.Context, runID string) (*core.FlowRun, error) {
	// SCHEDULED -> PENDING under our lease. The engine emits flow-run.RUNNING
	// when it starts, exactly as for a leased run, so we deliberately do not
	// emit a state-change event here.
	claimed, err := a.workerStore().ClaimPushRun(ctx, runID, a.pushHost(), a.Options.LeaseDuration)
	if err != nil {
		return nil, err // ErrConflict => already claimed by another receiver
	}
	return claimed, nil
}

func (a *App) executePushRun(ctx context.Context, run *core.FlowRun) {
	lease := a.Options.LeaseDuration
	if lease <= 0 {
		lease = 60 * time.Second
	}
	// Keep the lease alive for the duration, like a pull worker's renew loop, so
	// a long flow is not reclaimed as CRASHED while it is still running here.
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(lease / 3)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = a.workerStore().RenewLease(context.WithoutCancel(ctx), run.ID, *run.WorkerID, lease)
			}
		}
	}()
	defer close(done)

	eng := engine.New(a.workerStore(), a.Options.Registry, a.Events, a.Log, engine.Config{
		WorkerID: *run.WorkerID, Metrics: a.Metrics, MaxSubflowDepth: a.Options.MaxSubflowDepth,
	})
	eng.Execute(ctx, run)
}

// ServePushWorker listens on Options.PushAddr for signed dispatch notifications
// and executes each run synchronously (so a scale-to-zero platform keeps the
// instance alive for the duration).
func (a *App) ServePushWorker(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /run", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if !scheduler.VerifyPushSignature(a.Options.PushSecret, r.Header.Get("X-PrimeFlow-Signature"), body) {
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return
		}
		var b struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(body, &b); err != nil || b.RunID == "" {
			http.Error(w, "run_id required", http.StatusBadRequest)
			return
		}
		// Claim synchronously so the dispatcher's POST returns fast, then execute
		// in the background. A pod killed mid-flow lets the lease lapse and the
		// run is re-dispatched / reclaimed as CRASHED — the pull-worker recovery.
		claimed, err := a.claimForPush(r.Context(), b.RunID)
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				w.WriteHeader(http.StatusConflict) // already claimed elsewhere
				return
			}
			a.Log.Warn("push claim failed", "run", b.RunID, "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		go a.executePushRun(context.WithoutCancel(ctx), claimed)
		w.WriteHeader(http.StatusAccepted)
	})

	srv := &http.Server{Addr: a.Options.PushAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sc, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(sc)
	}()
	a.Log.Info("push worker listening", "addr", a.Options.PushAddr,
		"flows", a.Options.Registry.Names(), "signed", a.Options.PushSecret != "")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// RunPushWorker is the one-call entry point for a push-pool receiver: it opens
// the app and serves the dispatch endpoint.
func RunPushWorker(ctx context.Context, o Options) error {
	app, err := Open(ctx, o)
	if err != nil {
		return err
	}
	defer app.Close()
	return app.ServePushWorker(WithSignals(ctx))
}

func runAll(ctx context.Context, fns ...func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(fns))
	for _, fn := range fns {
		wg.Add(1)
		go func(f func(context.Context) error) {
			defer wg.Done()
			if err := f(ctx); err != nil {
				errCh <- err
				cancel()
			}
		}(fn)
	}
	wg.Wait()
	close(errCh)
	return <-errCh
}

// RunWorker is the one-call entry point for an application whose only job is to
// execute flows.
func RunWorker(ctx context.Context, o Options) error {
	app, err := Open(ctx, o)
	if err != nil {
		return err
	}
	defer app.Close()
	return app.ServeWorker(WithSignals(ctx))
}

// WithSignals returns a context cancelled on SIGINT or SIGTERM, which is what
// gives workers their graceful drain on a Kubernetes rollout.
func WithSignals(ctx context.Context) context.Context {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx
}

// cancelPollFor turns the engine's per-run cancellation poll off at a site and
// leaves it at its default beside a database.
func cancelPollFor(o Options) time.Duration {
	if o.Remote() {
		return -1
	}
	return 0
}

// workerStore is what the engine and the worker run against. On the database
// path the full store is the worker store by definition, so falling back to it
// keeps the two from drifting apart — a nil here is a panic on the first flow
// a worker registers, which is a long way from where the mistake was made.
func (a *App) workerStore() store.WorkerStore {
	if a.WorkerStore != nil {
		return a.WorkerStore
	}
	return a.Store
}
