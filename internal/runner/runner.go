// Package runner is the execution half of a PrimeFlow process: the pull worker
// and the push receiver, wired against a store and bus somebody else opened.
//
// It exists so the two public entry points can share one implementation.
// pkg/primeflow serves the API as well, and therefore links the server, the
// scheduler and the console; pkg/primeflow/worker must link none of those, so
// that a site VM's binary does not carry the OIDC and cron dependencies behind
// them. Both call in here, so neither has to keep its own copy of the wiring in
// step.
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/DetroittTxP/primeflow/internal/bus"
	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/engine"
	"github.com/DetroittTxP/primeflow/internal/events"
	"github.com/DetroittTxP/primeflow/internal/metrics"
	"github.com/DetroittTxP/primeflow/internal/pushsig"
	"github.com/DetroittTxP/primeflow/internal/store"
	"github.com/DetroittTxP/primeflow/internal/worker"
	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

// Execution modes and the variable that selects one, re-exported so the two
// public entry points can name them without importing internal/worker under an
// alias — both are themselves called "worker".
const (
	ExecInline  = worker.ExecInline
	ExecProcess = worker.ExecProcess
	EnvExecMode = worker.EnvExecMode
)

// Deps is the infrastructure a runner borrows. Events may be nil: a remote
// worker does not write the event log, the server records transitions on its
// behalf.
type Deps struct {
	Store   store.WorkerStore
	Bus     bus.Bus
	Events  *events.Emitter
	Metrics *metrics.Metrics
	Log     *slog.Logger
}

// Config is the worker-shaped subset of a process's options.
type Config struct {
	Name        string
	Queues      []string
	Concurrency int

	LeaseDuration time.Duration
	PollInterval  time.Duration
	// CancelPollInterval is the engine's per-run cancellation backstop. Zero
	// takes the engine default; negative turns it off, which is what a worker
	// reaching the orchestrator over a WAN wants — the heartbeat already
	// carries cancellations.
	CancelPollInterval time.Duration

	MaxSubflowDepth int
	MetricsAddr     string

	// PushAddr and PushSecret configure the push receiver.
	PushAddr   string
	PushSecret string

	// ExecMode is worker.ExecInline (the default) or worker.ExecProcess, which
	// gives every run a child process of this binary. ExecEnv is what such a
	// child needs to reach the orchestrator when the parent was configured in
	// Go rather than through the environment.
	ExecMode string
	ExecEnv  []string

	Registry *sdk.Registry
}

// ServeWorker runs a pull worker until ctx is cancelled.
func ServeWorker(ctx context.Context, d Deps, c Config) error {
	cfg := worker.Config{
		Name:               c.Name,
		Queues:             c.Queues,
		Concurrency:        c.Concurrency,
		PollInterval:       c.PollInterval,
		LeaseDuration:      c.LeaseDuration,
		Metrics:            d.Metrics,
		MetricsAddr:        c.MetricsAddr,
		MaxSubflowDepth:    c.MaxSubflowDepth,
		CancelPollInterval: c.CancelPollInterval,
		ExecMode:           c.ExecMode,
		ExecEnv:            c.ExecEnv,
	}
	switch c.ExecMode {
	case "", worker.ExecInline:
		cfg.ExecMode = worker.ExecInline
	case worker.ExecProcess:
		// Resolved once, here, so a binary that cannot find itself fails at
		// start-up rather than once per leased run.
		path, err := os.Executable()
		if err != nil {
			return fmt.Errorf("%s=%s needs this binary's own path: %w",
				worker.EnvExecMode, worker.ExecProcess, err)
		}
		cfg.ExecPath = path
	default:
		return fmt.Errorf("%s=%q is not a mode: use %q or %q",
			worker.EnvExecMode, c.ExecMode, worker.ExecInline, worker.ExecProcess)
	}
	return worker.New(d.Store, d.Bus, c.Registry, d.Events, d.Log, cfg).Run(ctx)
}

// Leased reports the run a process-mode child was started for, and the lease it
// is to execute under. Both public entry points check it before serving: a
// child is the same binary as its parent, and this is what tells it so.
func Leased() (runID, workerID string, ok bool) {
	runID = os.Getenv(worker.EnvRunID)
	workerID = os.Getenv(worker.EnvLeaseWorkerID)
	return runID, workerID, runID != "" && workerID != ""
}

// RunLeased executes a run the parent process has already leased, and is the
// whole of what a process-mode child does.
//
// It deliberately does none of what a worker does around a run: no lease of its
// own (the parent's heartbeat renews the one it was handed), no catalogue, no
// metrics listener, no heartbeat. Cancelling ctx — which is what the parent's
// SIGTERM does — cancels the run exactly as Engine.Cancel would in the parent,
// and the engine settles it either way.
func RunLeased(ctx context.Context, d Deps, c Config, runID, workerID string) error {
	run, err := d.Store.GetFlowRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("read leased run %s: %w", runID, err)
	}
	// The lease can have moved on between the parent's fork and this read — a
	// slow start against a short lease. The new holder is executing it now.
	if run.WorkerID == nil || *run.WorkerID != workerID {
		return fmt.Errorf("run %s is no longer leased by %s", runID, workerID)
	}
	engine.New(d.Store, c.Registry, d.Events, d.Log, engine.Config{
		WorkerID: workerID, Metrics: d.Metrics, MaxSubflowDepth: c.MaxSubflowDepth,
		CancelPollInterval: c.CancelPollInterval,
	}).Execute(ctx, run)
	return nil
}

// RunOne claims a single scheduled run and executes it synchronously. It is the
// unit of work a push receiver performs per dispatch, and a handy entry point
// for embedders and tests.
func RunOne(ctx context.Context, d Deps, c Config, runID string) error {
	claimed, err := claim(ctx, d, c, runID)
	if err != nil {
		return err
	}
	execute(context.WithoutCancel(ctx), d, c, claimed)
	return nil
}

// host is a stable-ish identity for this receiver's leases.
func host(c Config) string {
	h := c.Name
	if h == "" {
		h, _ = os.Hostname()
		if h == "" {
			h = "push-worker"
		}
	}
	return h + "-" + uuid.NewString()[:8]
}

func claim(ctx context.Context, d Deps, c Config, runID string) (*core.FlowRun, error) {
	// SCHEDULED -> PENDING under our lease. The engine emits flow-run.RUNNING
	// when it starts, exactly as for a leased run, so we deliberately do not
	// emit a state-change event here.
	return d.Store.ClaimPushRun(ctx, runID, host(c), c.LeaseDuration)
}

func execute(ctx context.Context, d Deps, c Config, run *core.FlowRun) {
	lease := c.LeaseDuration
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
				_ = d.Store.RenewLease(context.WithoutCancel(ctx), run.ID, *run.WorkerID, lease)
			}
		}
	}()
	defer close(done)

	eng := engine.New(d.Store, c.Registry, d.Events, d.Log, engine.Config{
		WorkerID: *run.WorkerID, Metrics: d.Metrics, MaxSubflowDepth: c.MaxSubflowDepth,
	})
	eng.Execute(ctx, run)
}

// ServePush listens on Config.PushAddr for signed dispatch notifications and
// executes each run, so a scale-to-zero platform keeps the instance alive for
// the duration.
func ServePush(ctx context.Context, d Deps, c Config) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /run", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if !pushsig.Verify(c.PushSecret, r.Header.Get(pushsig.Header), body) {
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
		claimed, err := claim(r.Context(), d, c, b.RunID)
		if err != nil {
			if errors.Is(err, store.ErrConflict) {
				w.WriteHeader(http.StatusConflict) // already claimed elsewhere
				return
			}
			d.Log.Warn("push claim failed", "run", b.RunID, "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		go execute(context.WithoutCancel(ctx), d, c, claimed)
		w.WriteHeader(http.StatusAccepted)
	})

	srv := &http.Server{Addr: c.PushAddr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sc, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(sc)
	}()
	d.Log.Info("push worker listening", "addr", c.PushAddr,
		"flows", c.Registry.Names(), "signed", c.PushSecret != "")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
