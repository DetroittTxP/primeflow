package kube

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/DetroittTxP/primeflow/internal/core"
)

// RunReader is the one thing the launcher needs from the store: whether the run
// its Job was created for has started yet. A pod that cannot pull its image, or
// that no node will admit, leaves a Job that is neither running nor failed;
// the run's own state is what distinguishes "starting" from "never will".
type RunReader interface {
	GetFlowRun(ctx context.Context, id string) (*core.FlowRun, error)
}

// Launcher runs each leased run in its own Kubernetes Job.
//
// It is a worker.Launcher, which is the whole trick: the worker leases,
// heartbeats, renews and drains exactly as it always has, and only where the
// run executes changes. Dispatch, per-queue concurrency, cancellation and crash
// recovery are untouched, because the orchestrator is never told that the run
// is in a pod.
//
// The pod renews the lease as well, and that is not redundant: a launcher can
// be rolled or evicted while its Jobs keep running, and a run whose lease
// lapsed under a live pod would be crashed by the janitor and leased again —
// the same flow executing twice. The pod holding its own lease closes that.
type Launcher struct {
	client   *Client
	tmpl     Template
	runs     RunReader
	workerID string
	// childEnv names the run and the lease to the pod. The launcher is handed
	// it rather than knowing it, so this package stays clear of the worker's
	// child contract.
	childEnv func(runID string) map[string]string

	poll time.Duration
	// startDeadline is how long a Job has to get its pod as far as RUNNING
	// before the launcher gives up on it. Image pull failures, unschedulable
	// pods and admission rejections all land here: without it the run would sit
	// leased and renewed for as long as the cluster kept retrying.
	startDeadline time.Duration
	log           *slog.Logger

	mu   sync.Mutex
	jobs map[string]string // run id -> job name
}

// LauncherOptions are the knobs with sensible defaults.
type LauncherOptions struct {
	Poll          time.Duration
	StartDeadline time.Duration
}

// NewLauncher builds a launcher for one worker.
func NewLauncher(c *Client, t Template, runs RunReader, workerID string,
	childEnv func(runID string) map[string]string, log *slog.Logger, o LauncherOptions) *Launcher {
	if o.Poll <= 0 {
		o.Poll = 3 * time.Second
	}
	if o.StartDeadline <= 0 {
		o.StartDeadline = 5 * time.Minute
	}
	return &Launcher{
		client: c, tmpl: t, runs: runs, workerID: workerID, childEnv: childEnv,
		poll: o.Poll, startDeadline: o.StartDeadline, log: log,
		jobs: map[string]string{},
	}
}

// Launch creates the Job and blocks until it settles, so the worker slot and
// the lease behind it last exactly as long as the run does.
func (l *Launcher) Launch(ctx context.Context, run *core.FlowRun) error {
	ref := RunRef{ID: run.ID, FlowName: run.FlowName, WorkQueue: run.WorkQueue, Attempt: run.RunCount}
	name := JobName(ref)

	if _, err := l.client.CreateJob(ctx, l.tmpl.Job(name, ref, l.childEnv(run.ID))); err != nil {
		return fmt.Errorf("create job for run %s: %w", run.ID, err)
	}
	l.track(run.ID, name)
	defer l.untrack(run.ID)
	l.log.Info("run job created", "run", run.ID, "flow", run.FlowName,
		"job", name, "namespace", l.client.Namespace())

	return l.wait(ctx, run.ID, name)
}

// wait polls the Job to a conclusion. Polling rather than watching is
// deliberate: a watch is a long-lived connection per run to re-establish on
// every hiccup, and a launcher is already sized by how many runs it admits.
func (l *Launcher) wait(ctx context.Context, runID, name string) error {
	t := time.NewTicker(l.poll)
	defer t.Stop()

	started := false
	deadline := time.Now().Add(l.startDeadline)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}

		job, err := l.client.GetJob(ctx, name)
		switch {
		case NotFound(err):
			// Deleted by something other than a cancellation, or reaped before
			// we saw it finish. The run itself is the authority.
			if l.settled(ctx, runID) {
				return nil
			}
			return fmt.Errorf("job %s disappeared while its run was unfinished", name)
		case err != nil:
			// A transient API error must not fail a run that is executing
			// perfectly well; the next tick asks again.
			l.log.Warn("could not read run job", "run", runID, "job", name, "err", err)
			continue
		}

		if done, ok, reason := job.Finished(); done {
			if ok {
				return nil
			}
			return fmt.Errorf("job %s failed: %s", name, reason)
		}

		if started {
			continue
		}
		if l.running(ctx, runID) {
			started = true
			continue
		}
		if time.Now().After(deadline) {
			l.log.Warn("run job never started; deleting it",
				"run", runID, "job", name, "deadline", l.startDeadline)
			if err := l.client.DeleteJob(ctx, name); err != nil {
				l.log.Warn("could not delete a job that never started", "job", name, "err", err)
			}
			return fmt.Errorf("job %s did not start a run within %s", name, l.startDeadline)
		}
	}
}

// running reports whether the pod has taken the run past PENDING, which is the
// state leasing left it in. A finished run counts too: a fast flow can complete
// between two polls.
//
// The state is the whole test. StartedAt is not a second opinion — leasing
// stamps it (see the LeaseFlowRuns statement), so a run whose pod has never
// existed already has one.
func (l *Launcher) running(ctx context.Context, runID string) bool {
	run, err := l.runs.GetFlowRun(ctx, runID)
	if err != nil {
		l.log.Warn("could not read run while waiting for its pod", "run", runID, "err", err)
		return false
	}
	return run.State != core.StatePending
}

func (l *Launcher) settled(ctx context.Context, runID string) bool {
	run, err := l.runs.GetFlowRun(ctx, runID)
	if err != nil {
		return false
	}
	return run.State.IsFinished() || run.State == core.StateScheduled
}

// Cancel deletes the run's Job. The pod is signalled and gets its termination
// grace period, in which the flow's context is cancelled and the engine settles
// the run — the same sequence a process-mode child follows on SIGTERM.
func (l *Launcher) Cancel(runID string) bool {
	l.mu.Lock()
	name := l.jobs[runID]
	l.mu.Unlock()
	if name == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := l.client.DeleteJob(ctx, name); err != nil {
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Code != 404 {
			l.log.Warn("could not delete a run job", "run", runID, "job", name, "err", err)
			return false
		}
	}
	l.log.Info("run job deleted", "run", runID, "job", name)
	return true
}

func (l *Launcher) track(runID, name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.jobs[runID] = name
}

func (l *Launcher) untrack(runID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.jobs, runID)
}

// ManagedBy selects the Jobs PrimeFlow created.
const ManagedBy = "app.kubernetes.io/managed-by=primeflow"

// runIDLabel is how a Job says which run it exists for.
const runIDLabel = "primeflow.io/run-id"

// SweepFinished deletes Jobs whose run has finished, and is meant to be called
// once, before a launcher starts leasing.
//
// A launcher normally leaves no litter: a pod settles its run and exits, and
// ttlSecondsAfterFinished collects the record. The exception is a launcher that
// is replaced — rolled, evicted, OOMed — while a Job of its is still around. A
// Job whose pod is executing perfectly well is picked up by nobody afterwards,
// but it does not need to be: the pod renews the lease and settles the run on
// its own. What is left behind is the Job record, and, for a pod that never
// started at all, a Job that will retry a hopeless image pull indefinitely.
//
// Only Jobs for finished runs are deleted. A run that is still RUNNING may
// belong to a live launcher — or to a pod outliving a dead one — and deleting
// its Job would kill a flow mid-execution.
func SweepFinished(ctx context.Context, c *Client, runs RunReader, log *slog.Logger) int {
	list, err := c.ListJobs(ctx, ManagedBy)
	if err != nil {
		// Not fatal: a launcher that cannot tidy up can still work. The Role
		// may simply not grant "list", which older manifests did not.
		log.Warn("could not list run jobs to sweep", "err", err)
		return 0
	}
	swept := 0
	for _, item := range list.Items {
		runID := item.Metadata.Labels[runIDLabel]
		if runID == "" {
			continue
		}
		run, err := runs.GetFlowRun(ctx, runID)
		if err != nil || !run.State.IsFinished() {
			continue
		}
		if err := c.DeleteJob(ctx, item.Metadata.Name); err != nil {
			log.Warn("could not delete a finished run's job", "job", item.Metadata.Name, "err", err)
			continue
		}
		log.Info("swept the job of a finished run", "job", item.Metadata.Name,
			"run", runID, "state", run.State)
		swept++
	}
	return swept
}
