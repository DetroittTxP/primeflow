package worker

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/DetroittTxP/primeflow/internal/core"
)

// Execution modes. Inline is the default and what every worker did before any
// of this existed: each leased run is a goroutine of the worker process.
// Process gives each run a child process of the same binary, and Kubernetes a
// Job of its own, so a panic that escapes a flow, a goroutine that outlives it,
// or an OOM kill reaches one run instead of every run sharing the process.
//
// Only where a run executes changes. The worker still leases through the same
// statement, still heartbeats, still renews and still drains, so per-queue
// concurrency, cancellation and crash recovery behave identically in all three.
const (
	ExecInline     = "inline"
	ExecProcess    = "process"
	ExecKubernetes = "kubernetes"
)

// The environment an out-of-process child is started with. Both public entry
// points check for EnvRunID before serving: finding it, they execute that one
// run under the lease named in EnvLeaseWorkerID and exit, so a worker's main()
// is the same program whichever mode it runs in.
//
// EnvLeaseRenew asks the child to renew that lease itself. A process-mode child
// does not need to — its parent is alive for exactly as long as it is — but a
// pod outlives a rolled launcher, and a run whose lease lapsed under a live pod
// would be crashed and leased again while still executing.
const (
	EnvRunID         = "PRIMEFLOW_RUN_ID"
	EnvLeaseWorkerID = "PRIMEFLOW_LEASE_WORKER_ID"
	EnvExecMode      = "PRIMEFLOW_EXEC_MODE"
	EnvLeaseRenew    = "PRIMEFLOW_LEASE_RENEW"
)

// Launcher runs one leased run somewhere other than this goroutine. The worker
// holds the slot and the lease for as long as Launch blocks, and an error from
// it means "nobody settled this run" — never the flow's own failure, which the
// engine settles wherever it ran.
type Launcher interface {
	Launch(ctx context.Context, run *core.FlowRun) error
	// Cancel stops the run if this launcher is still running it, reporting
	// whether there was anything to stop.
	Cancel(runID string) bool
}

// killGrace is how long a cancelled child has to settle its run before it is
// killed outright. Comfortably above the engine's settle path and well below a
// default 60s lease, so a child that ignores the signal is gone before the run
// it holds could be reclaimed under it.
const killGrace = 20 * time.Second

// processLauncher starts one child process per leased run.
//
// The child does not lease, heartbeat, publish a catalogue or serve metrics.
// The parent holds the lease and goes on renewing it for as long as the child
// runs, which is what keeps dispatch, queue accounting, cancellation and crash
// recovery exactly where they already were: from the orchestrator's side a
// process-mode worker is indistinguishable from an inline one.
type processLauncher struct {
	path     string   // this binary
	args     []string // arguments it is re-invoked with (none, normally)
	env      []string // route to the orchestrator, for a worker configured in Go
	workerID string   // the lease the child executes under
	grace    time.Duration
	log      *slog.Logger

	mu    sync.Mutex
	procs map[string]*os.Process
}

// NewProcessLauncher builds the process-mode launcher. path is the binary to
// re-invoke — this one — and env is the route to the orchestrator for a worker
// that was configured in Go rather than through the environment.
func NewProcessLauncher(path string, args, env []string, workerID string, log *slog.Logger) Launcher {
	return &processLauncher{
		path: path, args: args, env: env, workerID: workerID,
		grace: killGrace, log: log, procs: map[string]*os.Process{},
	}
}

// Launch runs one child to completion. The context is deliberately unused: a
// draining worker waits for its runs rather than killing them, exactly as the
// inline path does.
func (p *processLauncher) Launch(_ context.Context, run *core.FlowRun) error {
	cmd := exec.Command(p.path, p.args...)
	// os/exec keeps the last of a repeated variable, so what is appended here
	// overrides what the worker inherited — including the exec mode, since a
	// child never launches children of its own.
	cmd.Env = append(append(os.Environ(), p.env...),
		EnvRunID+"="+run.ID,
		EnvLeaseWorkerID+"="+p.workerID,
		EnvExecMode+"="+ExecInline,
	)
	// The child's structured log goes where the worker's does, so one container
	// log stream still carries every run.
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	isolate(cmd)

	if err := cmd.Start(); err != nil {
		return err
	}
	p.track(run.ID, cmd.Process)
	defer p.untrack(run.ID)
	p.log.Info("run process started", "run", run.ID, "flow", run.FlowName, "pid", cmd.Process.Pid)
	return cmd.Wait()
}

// Cancel asks the child holding runID to stop. It is the process-mode
// equivalent of cancelling a run's context in the engine: the child settles the
// run itself — CANCELLED when an operator asked for it, a retry or a failure
// otherwise — and only a child that ignores the signal for grace is killed,
// which leaves the run to the janitor.
func (p *processLauncher) Cancel(runID string) bool {
	p.mu.Lock()
	proc := p.procs[runID]
	p.mu.Unlock()
	if proc == nil {
		return false
	}
	if err := terminate(proc); err != nil {
		p.log.Warn("could not signal run process", "run", runID, "pid", proc.Pid, "err", err)
		return false
	}
	go func() {
		time.Sleep(p.grace)
		// Kill after the process has been reaped is refused by os.Process, so
		// this cannot reach a recycled pid.
		if err := proc.Kill(); err == nil {
			p.log.Warn("run process ignored the stop request and was killed",
				"run", runID, "pid", proc.Pid, "grace", p.grace)
		}
	}()
	return true
}

func (p *processLauncher) track(runID string, proc *os.Process) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.procs[runID] = proc
}

func (p *processLauncher) untrack(runID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.procs, runID)
}
