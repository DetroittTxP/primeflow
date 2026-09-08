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
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/bus"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/engine"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/metrics"
	"github.com/primex/primeflow/internal/pushsig"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/internal/worker"
	"github.com/primex/primeflow/pkg/sdk"
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

	Registry *sdk.Registry
}

// ServeWorker runs a pull worker until ctx is cancelled.
func ServeWorker(ctx context.Context, d Deps, c Config) error {
	w := worker.New(d.Store, d.Bus, c.Registry, d.Events, d.Log, worker.Config{
		Name:               c.Name,
		Queues:             c.Queues,
		Concurrency:        c.Concurrency,
		PollInterval:       c.PollInterval,
		LeaseDuration:      c.LeaseDuration,
		Metrics:            d.Metrics,
		MetricsAddr:        c.MetricsAddr,
		MaxSubflowDepth:    c.MaxSubflowDepth,
		CancelPollInterval: c.CancelPollInterval,
	})
	return w.Run(ctx)
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
