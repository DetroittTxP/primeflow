package worker

import (
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/DetroittTxP/primeflow/internal/core"
)

// Execution modes. Inline is the default and what every worker did before this
// existed: each leased run is a goroutine of the worker process. Process gives
// each run a child process of the same binary, so a panic that escapes a flow,
// a goroutine that outlives it, or an OOM kill reaches one run instead of every
// run sharing the process.
const (
	ExecInline  = "inline"
	ExecProcess = "process"
)

// The environment a process-mode child is started with. Both public entry
// points check for EnvRunID before serving: finding it, they execute that one
// run under the parent's lease and exit, so a worker's main() is the same
// program whichever mode it runs in.
const (
	EnvRunID         = "PRIMEFLOW_RUN_ID"
	EnvLeaseWorkerID = "PRIMEFLOW_LEASE_WORKER_ID"
	EnvExecMode      = "PRIMEFLOW_EXEC_MODE"
)

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

func newProcessLauncher(path string, args, env []string, workerID string, log *slog.Logger) *processLauncher {
	return &processLauncher{
		path: path, args: args, env: env, workerID: workerID,
		grace: killGrace, log: log, procs: map[string]*os.Process{},
	}
}

// Launch runs one child to completion. It returns the child's exit error, which
// the caller treats as "this run was not settled" — never as the flow's own
// failure, because a flow that fails settles itself and exits 0.
func (p *processLauncher) Launch(run *core.FlowRun) error {
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
