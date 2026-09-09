// Package worker polls work queues, holds leases, and hands runs to the engine.
//
// A worker owns no state. Everything it knows is re-derivable from Postgres, so
// killing one mid-run loses nothing: the lease expires, the janitor marks the
// run crashed, and another worker resumes it from its last checkpoint.
//
// Where a run executes is a deployment choice, not a change to any of that: in
// a goroutine of this process by default, or in a child process per run when
// Config.ExecMode is ExecProcess. See exec.go.
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

	"github.com/DetroittTxP/primeflow/internal/bus"
	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/engine"
	"github.com/DetroittTxP/primeflow/internal/events"
	"github.com/DetroittTxP/primeflow/internal/metrics"
	"github.com/DetroittTxP/primeflow/internal/store"
	"github.com/DetroittTxP/primeflow/pkg/sdk"
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
	// CancelPollInterval is how often a running flow re-reads its own run to
	// notice a cancellation. Zero takes the engine's default. Negative turns it
	// off, which is what a worker reaching the orchestrator over its API wants:
	// the heartbeat already carries cancellations, and one read per running
	// flow every few seconds is the WAN cost the heartbeat exists to remove.
	CancelPollInterval time.Duration

	// ExecMode names where runs execute. It is a label for logs and the
	// console; what actually decides is NewLauncher.
	ExecMode string
	// NewLauncher, when set, is asked for the launcher this worker executes
	// through — a child process, a Kubernetes Job — once its id exists, since
	// the id is the lease a child runs under. Nil means inline.
	//
	// It is a constructor rather than a value because everything that can fail
	// about a launcher (finding this binary, reading a service-account token,
	// parsing a template) belongs to start-up, and this is called after it.
	NewLauncher func(workerID string) Launcher
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
	// exec is nil in inline mode, where the engine above executes every run in
	// this process. Otherwise it starts one child per run — a process, a pod —
	// and the engine is left holding only the lease bookkeeping.
	exec Launcher
	reg  *sdk.Registry
	log  *slog.Logger

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
		CancelPollInterval: cfg.CancelPollInterval,
	})
	w := &Worker{
		id: id, cfg: cfg, store: s, bus: b, engine: eng, reg: reg, log: log,
		slots:   make(chan struct{}, cfg.Concurrency),
		wake:    make(chan struct{}, 1),
		leases:  map[string]struct{}{},
		started: time.Now().UTC(),
	}
	if cfg.NewLauncher != nil {
		w.exec = cfg.NewLauncher(id)
	}
	return w
}

// ID returns the worker's identity, used in lease records.
func (w *Worker) ID() string { return w.id }

// Run blocks until ctx is cancelled, then drains in-flight runs.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker starting",
		"id", w.id, "name", w.cfg.Name, "queues", w.cfg.Queues,
		"concurrency", w.cfg.Concurrency, "exec", w.execMode(), "flows", w.reg.Names())

	w.publishCatalogue(ctx)
	w.subscribe(ctx)
	w.serveMetrics(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	// One periodic conversation, not three: liveness, lease renewal and
	// cancellation all ride the same call.
	go func() { defer wg.Done(); w.heartbeatLoop(ctx) }()

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
			w.execute(context.WithoutCancel(ctx), &run)
		}()
	}
}

// execute runs one leased run, in this process or in a child of it. Either way
// it blocks for the whole run, because the slot it occupies is the concurrency
// limit and the lease the heartbeat renews.
func (w *Worker) execute(ctx context.Context, run *core.FlowRun) {
	if w.exec == nil {
		w.engine.Execute(ctx, run)
		return
	}
	if err := w.exec.Launch(ctx, run); err != nil {
		w.log.Warn("run did not settle where it was sent", "run", run.ID, "err", err)
		w.reap(ctx, run, err)
	}
}

// reap deals with a child that died without settling its run — killed, OOMed,
// or unable to start at all. The run is still PENDING or RUNNING under a lease
// this worker holds and no longer intends to renew, so expiring the lease now
// hands it to the janitor's crash path a lease period earlier than waiting
// would. That path is the one an inline worker's death takes, retry budget
// ("one extra attempt beyond the configured retries") included.
func (w *Worker) reap(ctx context.Context, run *core.FlowRun, cause error) {
	cur, err := w.store.GetFlowRun(ctx, run.ID)
	if err != nil {
		w.log.Warn("could not re-read run after its process exited", "run", run.ID, "err", err)
		return
	}
	// Every settling transition clears the lease, so still holding it is what
	// says the child left the run behind.
	if cur.WorkerID == nil || *cur.WorkerID != w.id {
		return
	}
	if err := w.store.RenewLease(ctx, run.ID, w.id, 0); err != nil {
		w.log.Warn("could not expire the lease of an unsettled run", "run", run.ID, "err", err)
		return
	}
	w.log.Warn("run process left its run unsettled; lease expired for the janitor",
		"run", run.ID, "state", cur.State, "cause", cause)
}

// cancelRun stops a run this worker is executing, wherever it is executing.
func (w *Worker) cancelRun(runID string) bool {
	if w.exec != nil {
		return w.exec.Cancel(runID)
	}
	return w.engine.Cancel(runID)
}

func (w *Worker) execMode() string {
	if w.cfg.ExecMode != "" {
		return w.cfg.ExecMode
	}
	return ExecInline
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
			if w.cancelRun(c.FlowRunID) {
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

// heartbeatLoop is the whole of a worker's periodic conversation with the
// orchestrator: it says this worker is alive, renews every lease it holds, and
// learns which of those runs an operator has asked to stop — in one call.
//
// It used to be three: a heartbeat, a renewal per held run, and, inside the
// engine, a cancellation poll per running run. Beside a database that is
// cheap. Across a WAN it is a worker's request rate scaling with how much work
// it is doing, which is exactly backwards — a busy site should not be the one
// that floods the link.
//
// A lease that comes back lost was reclaimed elsewhere, so the local copy is
// cancelled rather than allowed to finish work another worker has taken over.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	beat := func() {
		ctx := context.WithoutCancel(ctx)
		info := &core.WorkerInfo{
			ID: w.id, Name: w.cfg.Name, Queues: w.cfg.Queues,
			Concurrency: w.cfg.Concurrency, ActiveRuns: int(w.active.Load()),
			StartedAt: w.started,
		}
		if err := w.store.HeartbeatWorker(ctx, info); err != nil {
			w.log.Debug("heartbeat failed", "err", err)
		}

		w.leaseMu.Lock()
		held := make([]string, 0, len(w.leases))
		for id := range w.leases {
			held = append(held, id)
		}
		w.leaseMu.Unlock()
		if len(held) == 0 {
			return
		}

		renewed, cancelling, err := w.store.RenewLeases(ctx, w.id, held, w.cfg.LeaseDuration)
		if err != nil {
			// Nothing is assumed lost on a transport failure: the lease still
			// has its full period to run, and the next beat re-asks.
			w.log.Warn("lease renewal failed; will retry", "held", len(held), "err", err)
			return
		}
		keep := make(map[string]bool, len(renewed))
		for _, id := range renewed {
			keep[id] = true
		}
		for _, id := range held {
			if !keep[id] {
				w.log.Warn("lost lease; abandoning run", "run", id)
				w.cancelRun(id)
			}
		}
		for _, id := range cancelling {
			if w.cancelRun(id) {
				w.log.Info("cancelling run on request", "run", id)
			}
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
