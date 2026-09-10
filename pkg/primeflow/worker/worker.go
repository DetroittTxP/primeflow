// Package worker runs flows, and links nothing that serves them.
//
//	func main() {
//	    sdk.Flow("provision-vm", provisionVM)
//	    log.Fatal(worker.Run(context.Background(), worker.Options{}))
//	}
//
// It is the package to import for a site VM. The parent package,
// [github.com/DetroittTxP/primeflow/pkg/primeflow], can run a worker too, but it
// offers every mode from one package, so importing it links the API server, the
// console, OIDC, the scheduler and the automation evaluator whether the process
// reaches them or not.
//
// The size that saves is modest — about 0.7 MB on a stripped build, because the
// linker was already dropping most of the unreachable code. What it removes
// outright is the more interesting part: go-oidc, go-jose, the cron parser and
// the embedded time-zone database are no longer in the binary at all, so they
// no longer appear in a scan of one.
//
// What is given up: this package cannot serve the API, migrate a schema or seed
// an operator. A process that needs any of those wants the parent package.
//
// Options are read from the environment, so the same binary works in
// docker-compose and on Kubernetes with no code change.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/DetroittTxP/primeflow/internal/bus"
	"github.com/DetroittTxP/primeflow/internal/events"
	"github.com/DetroittTxP/primeflow/internal/metrics"
	"github.com/DetroittTxP/primeflow/internal/otelinit"
	"github.com/DetroittTxP/primeflow/internal/runner"
	"github.com/DetroittTxP/primeflow/internal/store"
	"github.com/DetroittTxP/primeflow/internal/store/postgres"
	"github.com/DetroittTxP/primeflow/internal/store/remote"
	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

// Options configure a worker process. Any zero field falls back to the matching
// PRIMEFLOW_* environment variable, then to a default.
type Options struct {
	// APIURL and WorkerToken put the worker in remote mode: it reaches the
	// orchestrator through the worker API instead of a database connection,
	// which is what a site allowed nothing but outbound 443 needs. The token is
	// a pool-scoped api-worker key.
	APIURL      string
	WorkerToken string

	// DatabaseURL is the Postgres DSN, for a worker running beside the
	// database. Ignored, with a warning, when APIURL and WorkerToken are set.
	DatabaseURL string

	// RedisURL / NatsURL select a notification bus for a worker on the database
	// path. Both are accelerators, never dependencies: a dial failure degrades
	// to polling. In remote mode the wake-up stream replaces them and both are
	// ignored.
	RedisURL string
	NatsURL  string

	Name        string
	Queues      []string
	Concurrency int

	LeaseDuration time.Duration
	PollInterval  time.Duration

	MaxSubflowDepth int
	MetricsAddr     string

	// PushAddr and PushSecret configure [App.ServePush]: the address the
	// receiver listens on for POST /run, and the secret the server signs
	// dispatches with.
	PushAddr   string
	PushSecret string

	// ExecMode is "inline" (the default: every run is a goroutine of this
	// process) or "process" (each run gets a child process of this same binary,
	// so a panic, a leak or an OOM kill reaches one run rather than all of
	// them). PRIMEFLOW_EXEC_MODE sets it. Pull pools only — a push receiver
	// executes in process whatever this says.
	ExecMode string

	// LogPrints records what a flow prints on os.Stdout or os.Stderr —
	// fmt.Println, log.Println, a command's output — in that run's own log, as
	// well as on this process's stdout where it already went. On by default;
	// PRIMEFLOW_LOG_PRINTS=false turns it off.
	//
	// A worker executing several runs in one process (ExecMode "inline" with
	// Concurrency above 1) cannot tell which of them printed a given line: Go
	// hands no writer identity to the other end of a pipe. Such a line is
	// recorded against every run in flight and marked ambiguous. Give each run
	// its own process — ExecMode "process", "kubernetes", or Concurrency 1 — and
	// every line is attributed exactly.
	LogPrints *bool

	// Registry holds the flows this process can execute. Defaults to sdk.Default.
	Registry *sdk.Registry

	// Logger; defaults to a JSON slog logger at info level.
	Logger *slog.Logger
}

// Remote reports whether this worker reaches the orchestrator through its API
// rather than a database connection.
func (o Options) Remote() bool { return o.APIURL != "" && o.WorkerToken != "" }

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envTrue reads a boolean variable, falling back to def when it is unset or
// unreadable. "1" and "true" are on; anything else set is off.
func envTrue(key string, def bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	return raw == "1" || strings.EqualFold(raw, "true")
}

func ptr[T any](v T) *T { return &v }

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

// applyEnv fills the zero fields from the environment. It is the same variable
// set the parent package reads, deliberately: an operator moving a site from
// one entry point to the other should not have to rewrite worker.env.
func (o *Options) applyEnv() {
	o.APIURL = firstNonEmpty(o.APIURL, os.Getenv("PRIMEFLOW_API_URL"))
	o.WorkerToken = firstNonEmpty(o.WorkerToken, os.Getenv("PRIMEFLOW_WORKER_TOKEN"))
	o.DatabaseURL = firstNonEmpty(o.DatabaseURL, os.Getenv("PRIMEFLOW_DATABASE_URL"), os.Getenv("DATABASE_URL"))
	o.RedisURL = firstNonEmpty(o.RedisURL, os.Getenv("PRIMEFLOW_REDIS_URL"), os.Getenv("REDIS_URL"))
	o.NatsURL = firstNonEmpty(o.NatsURL, os.Getenv("PRIMEFLOW_NATS_URL"), os.Getenv("NATS_URL"))
	o.Name = firstNonEmpty(o.Name, os.Getenv("PRIMEFLOW_WORKER_NAME"))

	if len(o.Queues) == 0 {
		for _, q := range strings.Split(envOr("PRIMEFLOW_QUEUES", "default"), ",") {
			if q = strings.TrimSpace(q); q != "" {
				o.Queues = append(o.Queues, q)
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
		// becomes a backstop rather than the mechanism.
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
	o.MetricsAddr = firstNonEmpty(o.MetricsAddr, envOr("PRIMEFLOW_METRICS_ADDR", ":9090"))
	o.PushAddr = firstNonEmpty(o.PushAddr, envOr("PRIMEFLOW_PUSH_ADDR", ":8090"))
	o.PushSecret = firstNonEmpty(o.PushSecret, os.Getenv("PRIMEFLOW_PUSH_SECRET"))
	o.ExecMode = firstNonEmpty(o.ExecMode, envOr(runner.EnvExecMode, runner.ExecInline))
	if o.LogPrints == nil {
		o.LogPrints = ptr(envTrue("PRIMEFLOW_LOG_PRINTS", true))
	}
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

// App is an opened worker: a store, a bus, and the options they came from.
type App struct {
	Options Options

	store store.WorkerStore
	bus   bus.Bus
	// events is nil in remote mode. A site does not write the event log
	// automations act on; the server records transitions on its behalf.
	events       *events.Emitter
	metrics      *metrics.Metrics
	log          *slog.Logger
	otelShutdown otelinit.ShutdownFunc
}

// Open connects to whichever route the options describe.
func Open(ctx context.Context, o Options) (*App, error) {
	o.applyEnv()
	if o.Remote() {
		return openRemote(ctx, o)
	}
	if o.DatabaseURL == "" {
		return nil, errors.New("primeflow/worker: set PRIMEFLOW_DATABASE_URL, or PRIMEFLOW_API_URL and PRIMEFLOW_WORKER_TOKEN to run against the API")
	}
	return openLocal(ctx, o)
}

// openRemote builds a worker that holds no database credential. Everything the
// local path sets up around a store is deliberately absent: no emitter, no
// migration, no leader election — a worker owns none of that.
func openRemote(ctx context.Context, o Options) (*App, error) {
	if o.DatabaseURL != "" {
		o.Logger.Warn("PRIMEFLOW_DATABASE_URL is set but ignored: this worker runs against the API",
			"api", o.APIURL)
	}
	name := o.Name
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

	return &App{
		Options: o, store: rs, bus: b,
		metrics: metrics.New(nil), log: o.Logger,
		otelShutdown: tracing(ctx, "primeflow-worker", o.Logger),
	}, nil
}

// openLocal builds a worker beside the database. It does not migrate: a worker
// is not the process that owns the schema.
func openLocal(ctx context.Context, o Options) (*App, error) {
	st, err := postgres.Open(ctx, o.DatabaseURL, 0)
	if err != nil {
		return nil, err
	}
	b := openBus(ctx, o)
	return &App{
		Options: o, store: st, bus: b,
		events: events.New(st, b, o.Logger), metrics: metrics.New(st), log: o.Logger,
		otelShutdown: tracing(ctx, "primeflow-worker", o.Logger),
	}, nil
}

// openBus follows the parent package's precedence: NATS, then Redis, then the
// in-process bus. Every path degrades to polling on a dial failure.
func openBus(ctx context.Context, o Options) bus.Bus {
	switch {
	case o.NatsURL != "":
		nb, err := bus.NewNATS(ctx, o.NatsURL, o.Logger)
		if err != nil {
			o.Logger.Warn("nats unavailable; falling back to polling", "err", err)
			return bus.NewInMemory()
		}
		o.Logger.Info("notification bus: nats", "url", o.NatsURL)
		return nb
	case o.RedisURL != "":
		rb, err := bus.NewRedis(ctx, o.RedisURL, os.Getenv("PRIMEFLOW_REDIS_PASSWORD"), 0, o.Logger)
		if err != nil {
			o.Logger.Warn("redis unavailable; falling back to polling", "err", err)
			return bus.NewInMemory()
		}
		o.Logger.Info("notification bus: redis")
		return rb
	default:
		o.Logger.Info("notification bus: in-process (no PRIMEFLOW_NATS_URL / PRIMEFLOW_REDIS_URL)")
		return bus.NewInMemory()
	}
}

// tracing is a no-op, and free, unless an OTLP endpoint is configured.
func tracing(ctx context.Context, service string, log *slog.Logger) otelinit.ShutdownFunc {
	shutdown, err := otelinit.Setup(ctx, service, version())
	if err != nil {
		log.Warn("otel setup failed; continuing without tracing", "err", err)
		return func(context.Context) error { return nil }
	}
	return shutdown
}

func version() string {
	if v := os.Getenv("PRIMEFLOW_VERSION"); v != "" {
		return v
	}
	return "dev"
}

// Close releases the store, the bus and the tracer.
func (a *App) Close() error {
	if a.otelShutdown != nil {
		_ = a.otelShutdown(context.Background())
	}
	if a.bus != nil {
		_ = a.bus.Close()
	}
	if c, ok := a.store.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

func (a *App) deps() runner.Deps {
	return runner.Deps{Store: a.store, Bus: a.bus, Events: a.events, Metrics: a.metrics, Log: a.log}
}

func (a *App) config() runner.Config {
	o := a.Options
	// A worker on the API hears about cancellation twice already — on the
	// wake-up stream and in every heartbeat — so the engine's per-run backstop
	// poll would only add WAN traffic proportional to how busy the site is.
	cancelPoll := time.Duration(0)
	if o.Remote() {
		cancelPoll = -1
	}
	return runner.Config{
		Name: o.Name, Queues: o.Queues, Concurrency: o.Concurrency,
		LeaseDuration: o.LeaseDuration, PollInterval: o.PollInterval,
		CancelPollInterval: cancelPoll,
		MaxSubflowDepth:    o.MaxSubflowDepth, MetricsAddr: o.MetricsAddr,
		PushAddr: o.PushAddr, PushSecret: o.PushSecret, Registry: o.Registry,
		ExecMode: o.ExecMode, ExecEnv: execEnv(o), LogPrints: o.LogPrints,
	}
}

// execEnv is the route a process-mode child is given on top of the environment
// it inherits. A worker configured entirely through PRIMEFLOW_* variables would
// pass these anyway; one configured in Go would otherwise fork a child that has
// no idea how to reach the orchestrator.
func execEnv(o Options) []string {
	var env []string
	add := func(k, v string) {
		if v != "" {
			env = append(env, k+"="+v)
		}
	}
	if o.Remote() {
		add("PRIMEFLOW_API_URL", o.APIURL)
		add("PRIMEFLOW_WORKER_TOKEN", o.WorkerToken)
	} else {
		add("PRIMEFLOW_DATABASE_URL", o.DatabaseURL)
	}
	// The bus is what carries a child's state changes to the console's live
	// feed, so a child opens the same one its parent did.
	add("PRIMEFLOW_NATS_URL", o.NatsURL)
	add("PRIMEFLOW_REDIS_URL", o.RedisURL)
	add("PRIMEFLOW_MAX_SUBFLOW_DEPTH", strconv.Itoa(o.MaxSubflowDepth))
	if o.LogPrints != nil {
		add("PRIMEFLOW_LOG_PRINTS", strconv.FormatBool(*o.LogPrints))
	}
	return env
}

// Serve runs the pull worker until ctx is cancelled — or, when this process is
// itself a process-mode child, executes the one run it was started for.
func (a *App) Serve(ctx context.Context) error {
	if runID, workerID, ok := runner.Leased(); ok {
		return runner.RunLeased(ctx, a.deps(), a.config(), runID, workerID)
	}
	return runner.ServeWorker(ctx, a.deps(), a.config())
}

// ServePush runs the push-pool receiver until ctx is cancelled.
func (a *App) ServePush(ctx context.Context) error {
	return runner.ServePush(ctx, a.deps(), a.config())
}

// RunOne claims a single scheduled run and executes it synchronously.
func (a *App) RunOne(ctx context.Context, runID string) error {
	return runner.RunOne(ctx, a.deps(), a.config(), runID)
}

// Run is the one-call entry point: open, serve, and drain on SIGTERM.
func Run(ctx context.Context, o Options) error {
	app, err := Open(ctx, o)
	if err != nil {
		return err
	}
	defer app.Close()
	return app.Serve(WithSignals(ctx))
}

// RunPush is [Run] for a push-pool receiver.
func RunPush(ctx context.Context, o Options) error {
	app, err := Open(ctx, o)
	if err != nil {
		return err
	}
	defer app.Close()
	return app.ServePush(WithSignals(ctx))
}

// WithSignals returns a context cancelled on SIGINT or SIGTERM, which is what
// gives workers their graceful drain on a rollout.
func WithSignals(ctx context.Context) context.Context {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	return ctx
}
