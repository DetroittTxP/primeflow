// Package worker polls work queues, holds leases, and hands runs to the engine.
//
// A worker owns no state. Everything it knows is re-derivable from Postgres, so
// killing one mid-run loses nothing: the lease expires, the janitor marks the
// run crashed, and another worker resumes it from its last checkpoint.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/primex/primeflow/internal/bus"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/engine"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/metrics"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/pkg/sdk"
)

// Config describes one worker process.
type Config struct {
	Name   string
	Queues []string
	// Concurrency is how many runs this worker executes at once.
	Concurrency int
	// PollInterval is the fallback poll cadence. The bus normally wakes the
	// worker sooner; this bounds latency when the bus is unavailable.
	PollInterval time.Duration
	// LeaseDuration is how long a claim survives without a heartbeat. Keep it
	// comfortably above HeartbeatInterval — too short and a busy worker loses
	// its own runs; too long and a dead worker's runs stall for that long.
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	// Metrics, when non-nil, is threaded into the engine for flow/task timings.
	Metrics *metrics.Metrics
	// MetricsAddr, when set and Metrics is non-nil, serves this worker's
	// Prometheus registry (flow/task execution timings live in the worker
	// process, not the server). Each worker pod is scraped separately.
	MetricsAddr string
	// MaxSubflowDepth bounds RunDeployment recursion (0 = engine default of 8).
	MaxSubflowDepth int
}

func (c *Config) applyDefaults() {
	if c.Name == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "worker"
		}
		c.Name = host
	}
	if len(c.Queues) == 0 {
		c.Queues = []string{"default"}
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 4
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 2 * time.Second
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 60 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = c.LeaseDuration / 3
	}
}

// Worker is a polling executor.
type Worker struct {
	id     string
	cfg    Config
	store  store.WorkerStore
	bus    bus.Bus
	engine *engine.Engine
	reg    *sdk.Registry
	log    *slog.Logger

	slots   chan struct{}
	active  atomic.Int64
	wake    chan struct{}
	started time.Time

	leaseMu sync.Mutex
	leases  map[string]struct{}
}

// New builds a worker. The engine it is given must share the same registry.
func New(s store.WorkerStore, b bus.Bus, reg *sdk.Registry, em *events.Emitter, log *slog.Logger, cfg Config) *Worker {
	cfg.applyDefaults()
	if log == nil {
		log = slog.Default()
	}
	id := uuid.NewString()
	eng := engine.New(s, reg, em, log, engine.Config{
		WorkerID: id, Metrics: cfg.Metrics, MaxSubflowDepth: cfg.MaxSubflowDepth,
	})
	return &Worker{
		id: id, cfg: cfg, store: s, bus: b, engine: eng, reg: reg, log: log,
		slots:   make(chan struct{}, cfg.Concurrency),
		wake:    make(chan struct{}, 1),
		leases:  map[string]struct{}{},
		started: time.Now().UTC(),
	}
}

// ID returns the worker's identity, used in lease records.
func (w *Worker) ID() string { return w.id }

// Run blocks until ctx is cancelled, then drains in-flight runs.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker starting",
		"id", w.id, "name", w.cfg.Name, "queues", w.cfg.Queues,
		"concurrency", w.cfg.Concurrency, "flows", w.reg.Names())

	w.publishCatalogue(ctx)
	w.subscribe(ctx)
	w.serveMetrics(ctx)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); w.heartbeatLoop(ctx) }()
	go func() { defer wg.Done(); w.leaseRenewLoop(ctx) }()

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		w.dispatch(ctx)

		select {
		case <-ctx.Done():
			w.log.Info("worker draining", "id", w.id, "active", w.active.Load())
			// Wait for in-flight runs to settle so they are not left to the
			// janitor on an orderly shutdown.
			for i := 0; i < w.cfg.Concurrency; i++ {
				w.slots <- struct{}{}
			}
			wg.Wait()
			w.log.Info("worker stopped", "id", w.id)
			return nil
		case <-ticker.C:
		case <-w.wake:
		}
	}
}

// dispatch claims as much work as there is free capacity for and starts it.
func (w *Worker) dispatch(ctx context.Context) {
	free := w.cfg.Concurrency - int(w.active.Load())
	if free <= 0 {
		return
	}
	runs, err := w.store.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: w.id, Queues: w.cfg.Queues, Max: free, LeaseFor: w.cfg.LeaseDuration,
	})
	if err != nil {
		w.log.Warn("lease failed", "err", err)
		return
	}
	for i := range runs {
		run := runs[i]
		w.slots <- struct{}{}
		w.active.Add(1)
		w.trackLease(run.ID, true)

		go func() {
			defer func() {
				w.trackLease(run.ID, false)
				w.active.Add(-1)
				<-w.slots
				// Finishing a run frees a slot; look for more immediately
				// rather than waiting for the next tick.
				w.nudge()
			}()
			w.log.Info("executing run",
				"run", run.ID, "flow", run.FlowName, "queue", run.WorkQueue,
				"priority", run.Priority, "attempt", run.RunCount)
			w.engine.Execute(context.WithoutCancel(ctx), &run)
		}()
	}
}

func (w *Worker) nudge() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *Worker) trackLease(runID string, add bool) {
	w.leaseMu.Lock()
	defer w.leaseMu.Unlock()
	if add {
		w.leases[runID] = struct{}{}
	} else {
		delete(w.leases, runID)
	}
}

// subscribe wires bus notifications: new work wakes the poll loop, control
// messages cancel a run this worker holds.
func (w *Worker) subscribe(ctx context.Context) {
	if w.bus == nil {
		return
	}
	err := w.bus.Subscribe(ctx, []string{bus.TopicWork, bus.TopicControl}, func(m bus.Message) {
		switch m.Topic {
		case bus.TopicWork:
			var p struct {
				Queue string `json:"queue"`
			}
			if json.Unmarshal(m.Payload, &p) == nil && w.watches(p.Queue) {
				w.nudge()
			}
		case bus.TopicControl:
			var c bus.ControlMessage
			if json.Unmarshal(m.Payload, &c) != nil || c.Action != "cancel" {
				return
			}
			if w.engine.Cancel(c.FlowRunID) {
				w.log.Info("cancelling run on request", "run", c.FlowRunID)
			}
		}
	})
	if err != nil {
		w.log.Warn("bus subscribe failed; falling back to polling", "err", err)
	}
}

// serveMetrics exposes this worker's Prometheus registry on its own listener, so
// flow- and task-execution timings (which happen in this process, not the
// server) are scrapeable. One goroutine, shut down with ctx.
func (w *Worker) serveMetrics(ctx context.Context) {
	reg := w.cfg.Metrics.Registry()
	if reg == nil || w.cfg.MetricsAddr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: w.cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sc, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sc)
	}()
	go func() {
		w.log.Info("worker metrics listening", "addr", w.cfg.MetricsAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			w.log.Warn("worker metrics listener stopped", "err", err)
		}
	}()
}

func (w *Worker) watches(queue string) bool {
	if queue == "" {
		return true
	}
	for _, q := range w.cfg.Queues {
		if q == queue {
			return true
		}
	}
	return false
}

// publishCatalogue registers this worker's flows so the UI can list them and
// deployments can reference them before any run exists.
func (w *Worker) publishCatalogue(ctx context.Context) {
	for _, f := range w.reg.List() {
		fl := &core.Flow{
			ID: uuid.NewString(), Name: f.Name, Version: f.Version,
			Description: f.Description, Tags: f.Tags, ParamsSchema: f.ParamsSchema,
		}
		if err := w.store.UpsertFlow(ctx, fl); err != nil {
			w.log.Warn("register flow failed", "flow", f.Name, "err", err)
		}
	}
	for _, q := range w.cfg.Queues {
		// Ensure, never upsert: a worker knows only the lane's name, so writing
		// a full queue definition here would wipe the operator's concurrency
		// limit, pause switch and autoscaling envelope on every restart.
		if err := w.store.EnsureWorkQueue(ctx, q); err != nil {
			w.log.Debug("ensure queue failed", "queue", q, "err", err)
		}
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	beat := func() {
		info := &core.WorkerInfo{
			ID: w.id, Name: w.cfg.Name, Queues: w.cfg.Queues,
			Concurrency: w.cfg.Concurrency, ActiveRuns: int(w.active.Load()),
			StartedAt: w.started,
		}
		if err := w.store.HeartbeatWorker(context.WithoutCancel(ctx), info); err != nil {
			w.log.Debug("heartbeat failed", "err", err)
		}
	}
	beat()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			beat()
		}
	}
}

// leaseRenewLoop keeps this worker's claims alive. A renewal that fails means
// the run was reclaimed elsewhere, so we cancel our copy rather than let two
// workers execute the same run.
func (w *Worker) leaseRenewLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.leaseMu.Lock()
			ids := make([]string, 0, len(w.leases))
			for id := range w.leases {
				ids = append(ids, id)
			}
			w.leaseMu.Unlock()

			for _, id := range ids {
				err := w.store.RenewLease(context.WithoutCancel(ctx), id, w.id, w.cfg.LeaseDuration)
				if err != nil {
					w.log.Warn("lost lease; abandoning run", "run", id, "err", err)
					w.engine.Cancel(id)
				}
			}
		}
	}
}
