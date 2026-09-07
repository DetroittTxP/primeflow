// Package engine executes a leased flow run and translates the outcome into
// state transitions.
//
// The durability model is checkpoint-and-replay, not deterministic replay: a
// resumed run executes its function again from the top, and every task that
// already completed returns its stored result instead of running. That trades
// one authoring rule ("side effects go inside tasks") for the freedom to write
// flows as ordinary Go — goroutines, maps, time.Now and all.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/pkg/sdk"
)

// Config tunes execution behaviour.
type Config struct {
	// WorkerID identifies the executing process in state records.
	WorkerID string
	// CancelPollInterval is how often a running flow checks whether an
	// operator asked it to stop. The bus normally delivers that faster; this
	// is the backstop for when the bus is unavailable.
	CancelPollInterval time.Duration
	// SuspendThreshold is how long a durable wait must be before the run gives
	// up its worker slot.
	SuspendThreshold time.Duration
	// LogFlushInterval is how often buffered logs are written mid-run.
	LogFlushInterval time.Duration
}

func (c *Config) applyDefaults() {
	if c.CancelPollInterval <= 0 {
		c.CancelPollInterval = 5 * time.Second
	}
	if c.SuspendThreshold <= 0 {
		c.SuspendThreshold = 30 * time.Second
	}
	if c.LogFlushInterval <= 0 {
		c.LogFlushInterval = time.Second
	}
}

// Engine runs flows.
type Engine struct {
	store    store.Store
	registry *sdk.Registry
	events   *events.Emitter
	log      *slog.Logger
	cfg      Config

	mu     sync.Mutex
	active map[string]context.CancelFunc
}

// New builds an engine.
func New(s store.Store, reg *sdk.Registry, em *events.Emitter, log *slog.Logger, cfg Config) *Engine {
	cfg.applyDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Engine{
		store: s, registry: reg, events: em, log: log, cfg: cfg,
		active: map[string]context.CancelFunc{},
	}
}

// Cancel stops a run this engine is currently executing. It is a no-op if the
// run belongs to another worker.
func (e *Engine) Cancel(runID string) bool {
	e.mu.Lock()
	cancel, ok := e.active[runID]
	e.mu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

// ActiveCount reports how many runs this engine is executing.
func (e *Engine) ActiveCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.active)
}

// Execute runs one leased flow run to a terminal or suspended state.
//
// It always leaves the run in a settled state: nothing here returns without
// having written COMPLETED, FAILED, CANCELLED or SCHEDULED, because a run
// stuck in RUNNING with no worker is the one failure mode operators cannot
// diagnose from the UI.
func (e *Engine) Execute(parent context.Context, run *core.FlowRun) {
	flow, ok := e.registry.Get(run.FlowName)
	if !ok {
		// The worker leased something it cannot run. Put it back rather than
		// failing it — a differently-configured worker may own this flow.
		e.log.Warn("flow not registered on this worker; returning run to queue",
			"flow", run.FlowName, "run", run.ID)
		e.settle(parent, run, core.NewState(core.StateScheduled, "AwaitingWorker",
			fmt.Sprintf("flow %q is not registered on worker %s", run.FlowName, e.cfg.WorkerID)),
			store.StateOpts{ClearLease: true, ScheduleAt: ptr(time.Now().UTC().Add(30 * time.Second))})
		return
	}

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	e.mu.Lock()
	e.active[run.ID] = cancel
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.active, run.ID)
		e.mu.Unlock()
	}()

	bridge := &runtimeBridge{eng: e, runID: run.ID, flowName: run.FlowName}
	defer bridge.Flush(context.WithoutCancel(parent))

	// Move PENDING -> RUNNING. If this fails the run was cancelled or reclaimed
	// underneath us and we must not execute user code.
	started := time.Now().UTC()
	running, err := e.store.SetFlowRunState(ctx, run.ID,
		core.NewState(core.StateRunning, "Running", ""),
		store.StateOpts{WorkerID: &e.cfg.WorkerID, StartedAt: &started})
	if err != nil {
		e.log.Warn("could not start run", "run", run.ID, "err", err)
		return
	}
	e.events.FlowRunStateChanged(ctx, running)
	run = running

	// Background helpers: periodic log flush and cancellation polling.
	stop := make(chan struct{})
	var helpers sync.WaitGroup
	helpers.Add(2)
	go func() { defer helpers.Done(); e.flushLoop(ctx, bridge, stop) }()
	go func() { defer helpers.Done(); e.cancelWatch(ctx, run.ID, cancel, stop) }()
	defer func() { close(stop); helpers.Wait() }()

	// Apply the flow timeout, preferring the run's own value over the flow default.
	timeout := run.Timeout
	if timeout <= 0 {
		timeout = flow.Timeout
	}
	execCtx := ctx
	if timeout > 0 {
		var tcancel context.CancelFunc
		execCtx, tcancel = context.WithTimeout(ctx, timeout)
		defer tcancel()
	}

	sctx := sdk.NewContext(sdk.WithParams(execCtx, run.Parameters), bridge, sdk.RunInfo{
		RunID: run.ID, RunName: run.Name, FlowName: run.FlowName,
		WorkQueue: run.WorkQueue, Priority: run.Priority, Attempt: run.RunCount,
		Tags: run.Tags, ScheduledAt: run.ScheduledAt,
	})
	sctx.SetSuspendThreshold(e.cfg.SuspendThreshold)
	if run.DeploymentID != nil {
		info := sctx.Run()
		info.DeploymentID = *run.DeploymentID
		_ = info
	}

	result, runErr := e.invoke(sctx, flow)
	bridge.Flush(context.WithoutCancel(parent))

	e.finish(context.WithoutCancel(parent), run, flow, result, runErr)
}

// invoke calls the flow function, converting a panic into an error so a bug in
// one flow cannot take the worker down with it.
func (e *Engine) invoke(c *sdk.Context, flow *sdk.FlowDef) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = sdk.Permanent(fmt.Errorf("panic in flow %q: %v", flow.Name, r))
		}
	}()
	return flow.Fn(c)
}

// finish maps the outcome of user code onto a state transition.
func (e *Engine) finish(ctx context.Context, run *core.FlowRun, flow *sdk.FlowDef, result any, runErr error) {
	now := time.Now().UTC()

	// Suspension: the flow asked to continue later. Not a failure, and the
	// attempt does not count against the retry budget.
	if s, ok := sdk.IsSuspend(runErr); ok {
		e.settle(ctx, run,
			core.NewState(core.StateScheduled, "Suspended", s.Reason),
			store.StateOpts{ScheduleAt: &s.Until, ClearLease: true})
		return
	}

	// Cancellation, whether requested by an operator or observed as a
	// cancelled context.
	if cancelled, _ := e.cancelRequested(ctx, run.ID); cancelled ||
		errors.Is(runErr, sdk.ErrCancelled) {
		e.settle(ctx, run,
			core.NewState(core.StateCancelled, "Cancelled", "cancelled by operator"),
			store.StateOpts{EndedAt: &now, ClearLease: true, Force: true})
		return
	}

	if runErr == nil {
		var raw json.RawMessage
		if result != nil {
			if b, err := json.Marshal(result); err == nil {
				raw = b
			}
		}
		e.settle(ctx, run, core.NewState(core.StateCompleted, "Completed", ""),
			store.StateOpts{EndedAt: &now, Result: raw, ClearLease: true})
		return
	}

	// Failure. RunCount was incremented when the run was leased, so attempt N
	// means RunCount == N.
	budget := run.Retries
	if budget == 0 {
		budget = flow.Retries
	}
	msg := truncate(runErr.Error(), 4000)

	if !sdk.IsPermanent(runErr) && run.RunCount <= budget {
		delay := run.RetryDelay
		if delay <= 0 {
			delay = flow.RetryDelay
		}
		if delay <= 0 {
			delay = 30 * time.Second
		}
		at := now.Add(delay * time.Duration(run.RunCount))
		e.log.Info("run failed, rescheduling",
			"run", run.ID, "attempt", run.RunCount, "budget", budget, "retry_at", at)
		e.settle(ctx, run,
			core.NewState(core.StateScheduled, "AwaitingRetry", msg),
			store.StateOpts{ScheduleAt: &at, ClearLease: true})
		return
	}

	e.settle(ctx, run, core.NewState(core.StateFailed, "Failed", msg),
		store.StateOpts{EndedAt: &now, ClearLease: true})
}

// settle writes a terminal or requeueing transition and emits the event. It
// retries once with Force, because leaving a run un-settled is worse than
// bending the transition rules.
func (e *Engine) settle(ctx context.Context, run *core.FlowRun, st core.State, opts store.StateOpts) {
	updated, err := e.store.SetFlowRunState(ctx, run.ID, st, opts)
	if err != nil {
		opts.Force = true
		updated, err = e.store.SetFlowRunState(ctx, run.ID, st, opts)
	}
	if err != nil {
		e.log.Error("could not settle run", "run", run.ID, "state", st.Type, "err", err)
		return
	}
	e.events.FlowRunStateChanged(ctx, updated)
	if st.Type == core.StateScheduled {
		e.events.WorkAvailable(ctx, updated.WorkQueue)
	}
}

// flushLoop writes buffered logs while the flow runs, so a long run's logs are
// visible in the UI as they happen rather than only at the end.
func (e *Engine) flushLoop(ctx context.Context, b *runtimeBridge, stop <-chan struct{}) {
	t := time.NewTicker(e.cfg.LogFlushInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			b.Flush(context.WithoutCancel(ctx))
		}
	}
}

// cancelWatch polls for an operator cancellation and cancels the run's context.
func (e *Engine) cancelWatch(ctx context.Context, runID string, cancel context.CancelFunc, stop <-chan struct{}) {
	t := time.NewTicker(e.cfg.CancelPollInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			if yes, err := e.cancelRequested(ctx, runID); err == nil && yes {
				e.log.Info("cancellation observed", "run", runID)
				cancel()
				return
			}
		}
	}
}

func (e *Engine) cancelRequested(ctx context.Context, runID string) (bool, error) {
	r, err := e.store.GetFlowRun(ctx, runID)
	if err != nil {
		return false, err
	}
	return r.CancelRequest || r.State == core.StateCancelling || r.State == core.StateCancelled, nil
}

func isNotFound(err error) bool { return errors.Is(err, store.ErrNotFound) }

func ptr[T any](v T) *T { return &v }
