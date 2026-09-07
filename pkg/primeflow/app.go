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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/automations"
	"github.com/primex/primeflow/internal/bus"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/scheduler"
	"github.com/primex/primeflow/internal/server"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/internal/store/postgres"
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

	// Server options.
	HTTPAddr   string
	APIToken   string
	DisableUI  bool
	CORSOrigin string

	// Worker options.
	WorkerName    string
	Queues        []string
	Concurrency   int
	LeaseDuration time.Duration
	PollInterval  time.Duration

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
	o.HTTPAddr = firstNonEmpty(o.HTTPAddr, os.Getenv("PRIMEFLOW_HTTP_ADDR"), ":8080")
	o.APIToken = firstNonEmpty(o.APIToken, os.Getenv("PRIMEFLOW_API_TOKEN"))
	o.CORSOrigin = firstNonEmpty(o.CORSOrigin, os.Getenv("PRIMEFLOW_CORS_ORIGIN"))
	o.WorkerName = firstNonEmpty(o.WorkerName, os.Getenv("PRIMEFLOW_WORKER_NAME"))

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
		if d, err := time.ParseDuration(envOr("PRIMEFLOW_POLL", "2s")); err == nil {
			o.PollInterval = d
		}
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
	Store   store.Store
	Bus     bus.Bus
	Events  *events.Emitter
	Log     *slog.Logger
	holder  string
}

// Open connects to Postgres and (if configured) Redis.
func Open(ctx context.Context, o Options) (*App, error) {
	o.applyEnv()
	if o.DatabaseURL == "" {
		return nil, errors.New("primeflow: DatabaseURL (PRIMEFLOW_DATABASE_URL) is required")
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

	var b bus.Bus
	if o.RedisURL != "" {
		rb, err := bus.NewRedis(ctx, o.RedisURL, os.Getenv("PRIMEFLOW_REDIS_PASSWORD"), 0, o.Logger)
		if err != nil {
			// Redis only reduces latency, so a failure here is a warning, not
			// a reason to refuse to start.
			o.Logger.Warn("redis unavailable; falling back to polling", "err", err)
			b = bus.NewInMemory()
		} else {
			b = rb
		}
	} else {
		b = bus.NewInMemory()
	}

	return &App{
		Options: o, Store: st, Bus: b,
		Events: events.New(st, b, o.Logger), Log: o.Logger,
		holder: uuid.NewString(),
	}, nil
}

// Close releases resources.
func (a *App) Close() error {
	if a.Bus != nil {
		_ = a.Bus.Close()
	}
	return a.Store.Close()
}

// Migrate applies the schema.
func (a *App) Migrate(ctx context.Context) error {
	pg, ok := a.Store.(*postgres.Store)
	if !ok {
		return errors.New("primeflow: migrations require the postgres store")
	}
	return pg.Migrate(ctx)
}

// ServeAPI runs the API, UI, scheduler and automation evaluator.
func (a *App) ServeAPI(ctx context.Context) error {
	srv := server.New(a.Store, a.Bus, a.Events, a.Log, server.Config{
		Addr:       a.Options.HTTPAddr,
		APIToken:   a.Options.APIToken,
		UIEnabled:  !a.Options.DisableUI,
		CORSOrigin: a.Options.CORSOrigin,
	})
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
	w := worker.New(a.Store, a.Bus, a.Options.Registry, a.Events, a.Log, worker.Config{
		Name:          a.Options.WorkerName,
		Queues:        a.Options.Queues,
		Concurrency:   a.Options.Concurrency,
		PollInterval:  a.Options.PollInterval,
		LeaseDuration: a.Options.LeaseDuration,
	})
	return w.Run(ctx)
}

// ServeAll runs the API and a worker in one process — the mode to use for
// development, a small deployment, or when PrimeFlow is embedded directly in
// the PrimeX backend.
func (a *App) ServeAll(ctx context.Context) error {
	return runAll(ctx, a.ServeAPI, a.ServeWorker)
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
