package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/primex/primeflow/internal/bus"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/engine"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/internal/store/postgres"
	"github.com/primex/primeflow/pkg/sdk"
)

type harness struct {
	store  *postgres.Store
	engine *engine.Engine
	reg    *sdk.Registry
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dsn := os.Getenv("PRIMEFLOW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set PRIMEFLOW_TEST_DATABASE_URL to run engine integration tests")
	}
	ctx := context.Background()
	st, err := postgres.Open(ctx, dsn, 10)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := st.DB().ExecContext(ctx, `
TRUNCATE pf_logs, pf_artifacts, pf_task_runs, pf_flow_runs, pf_events,
         pf_automations, pf_deployments, pf_workers, pf_leader, pf_flows RESTART IDENTITY CASCADE;
DELETE FROM pf_work_queues WHERE name <> 'default';
UPDATE pf_work_queues SET paused = false, concurrency_limit = NULL;`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := bus.NewInMemory()
	reg := sdk.NewRegistry()
	em := events.New(st, b, log)
	eng := engine.New(st, reg, em, log, engine.Config{
		WorkerID:           "test-worker",
		CancelPollInterval: 50 * time.Millisecond,
		SuspendThreshold:   time.Second,
		LogFlushInterval:   20 * time.Millisecond,
	})
	return &harness{store: st, engine: eng, reg: reg}
}

// lease claims the next run the way a worker would, so the engine sees the same
// PENDING state it would in production.
func (h *harness) lease(t *testing.T) *core.FlowRun {
	t.Helper()
	runs, err := h.store.LeaseFlowRuns(context.Background(), store.LeaseRequest{
		WorkerID: "test-worker", Queues: []string{"default"}, Max: 1, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 leased run, got %d", len(runs))
	}
	return &runs[0]
}

func (h *harness) create(t *testing.T, flow string, retries int) *core.FlowRun {
	t.Helper()
	r, err := h.store.CreateFlowRun(context.Background(), store.CreateRunInput{
		FlowName: flow, WorkQueue: "default", Priority: 50,
		ScheduledAt: time.Now().UTC().Add(-time.Second),
		Retries:     retries, RetryDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	return r
}

func (h *harness) reload(t *testing.T, id string) *core.FlowRun {
	t.Helper()
	r, err := h.store.GetFlowRun(context.Background(), id)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	return r
}

// A worker that reaches the orchestrator over its API has no database to append
// events to and no bus to publish them on, so it is built without an emitter and
// the server records transitions on its behalf. Running a flow to completion
// with a nil emitter is what proves nothing on the execution path depends on
// one, and that the narrow WorkerStore is genuinely all a worker needs.
func TestEngineRunsWithoutAnEmitter(t *testing.T) {
	h := newHarness(t)
	var ran atomic.Int32
	h.reg.Register("no-emitter", func(c *sdk.Context) (any, error) {
		return sdk.Task(c, "step", func(c *sdk.Context) (int, error) {
			ran.Add(1)
			c.Info("working without an emitter")
			_ = c.Markdown("note", "ran")
			return 7, nil
		})
	})

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	// The narrow interface is what a remote worker would be handed; taking it
	// here keeps the assertion honest.
	var narrow store.WorkerStore = h.store
	eng := engine.New(narrow, h.reg, nil, log, engine.Config{
		WorkerID:           "no-emitter-worker",
		CancelPollInterval: 50 * time.Millisecond,
		SuspendThreshold:   time.Second,
		LogFlushInterval:   20 * time.Millisecond,
	})

	run := h.create(t, "no-emitter", 0)
	leased, err := h.store.LeaseFlowRuns(context.Background(), store.LeaseRequest{
		WorkerID: "no-emitter-worker", Queues: []string{"default"}, Max: 1, LeaseFor: time.Minute,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: %v (%d runs)", err, len(leased))
	}
	eng.Execute(context.Background(), &leased[0])

	got := h.reload(t, run.ID)
	if got.State != core.StateCompleted {
		t.Fatalf("state = %s (%s), want COMPLETED", got.State, got.StateMessage)
	}
	if ran.Load() != 1 {
		t.Fatalf("task ran %d times, want 1", ran.Load())
	}

	// The checkpoint, the log line and the artifact are all still written: only
	// the event stream is absent.
	tasks, err := h.store.ListTaskRuns(context.Background(), run.ID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("checkpoint missing: %v (%d task runs)", err, len(tasks))
	}
	arts, err := h.store.ListArtifacts(context.Background(), run.ID)
	if err != nil || len(arts) != 1 {
		t.Fatalf("artifact missing: %v (%d)", err, len(arts))
	}
}

// TestDurableResumeSkipsCompletedTasks is the headline guarantee: after a
// failure, the retry must not repeat work that already succeeded.
func TestDurableResumeSkipsCompletedTasks(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	var expensiveCalls, cheapCalls atomic.Int64
	var failOnce atomic.Bool
	failOnce.Store(true)

	h.reg.Register("resumable", func(c *sdk.Context) (any, error) {
		vm, err := sdk.Task(c, "create-vm", func(*sdk.Context) (string, error) {
			expensiveCalls.Add(1)
			return "vm-123", nil
		})
		if err != nil {
			return nil, err
		}
		// This step fails on the first flow attempt and succeeds on the second.
		if err := sdk.Do(c, "register", func(*sdk.Context) error {
			cheapCalls.Add(1)
			if failOnce.Swap(false) {
				return errors.New("metering unavailable")
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return map[string]string{"vm": vm}, nil
	})

	run := h.create(t, "resumable", 2)

	// Attempt 1: fails, and the engine reschedules it.
	h.engine.Execute(ctx, h.lease(t))
	got := h.reload(t, run.ID)
	if got.State != core.StateScheduled || got.StateName != "AwaitingRetry" {
		t.Fatalf("after failure: state=%s name=%s msg=%s", got.State, got.StateName, got.StateMessage)
	}
	if expensiveCalls.Load() != 1 {
		t.Fatalf("expensive task ran %d times on attempt 1", expensiveCalls.Load())
	}

	// Attempt 2: resumes.
	time.Sleep(20 * time.Millisecond)
	h.engine.Execute(ctx, h.lease(t))
	got = h.reload(t, run.ID)
	if got.State != core.StateCompleted {
		t.Fatalf("attempt 2 should complete, got %s (%s)", got.State, got.StateMessage)
	}
	if expensiveCalls.Load() != 1 {
		t.Fatalf("expensive task re-executed on resume: %d calls", expensiveCalls.Load())
	}
	if cheapCalls.Load() != 2 {
		t.Fatalf("failed task should run again: %d calls", cheapCalls.Load())
	}

	// The checkpoint ledger reflects both steps.
	tasks, err := h.store.ListTaskRuns(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("expected 2 checkpoints, got %d", len(tasks))
	}
	for _, tr := range tasks {
		if tr.State != core.StateCompleted {
			t.Fatalf("checkpoint %s ended %s", tr.TaskKey, tr.State)
		}
	}
}

func TestFailureExhaustsRetryBudget(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	var calls atomic.Int64
	h.reg.Register("always-fails", func(c *sdk.Context) (any, error) {
		calls.Add(1)
		return nil, errors.New("nope")
	})

	run := h.create(t, "always-fails", 1) // 1 retry => 2 attempts

	h.engine.Execute(ctx, h.lease(t))
	if got := h.reload(t, run.ID); got.State != core.StateScheduled {
		t.Fatalf("attempt 1 should reschedule, got %s", got.State)
	}
	time.Sleep(20 * time.Millisecond)
	h.engine.Execute(ctx, h.lease(t))

	got := h.reload(t, run.ID)
	if got.State != core.StateFailed {
		t.Fatalf("attempt 2 should fail terminally, got %s", got.State)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", calls.Load())
	}
	if got.EndedAt == nil {
		t.Fatal("failed run should have an end time")
	}
}

func TestPermanentErrorSkipsRetryBudget(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.reg.Register("bad-request", func(c *sdk.Context) (any, error) {
		return nil, sdk.Permanent(errors.New("org_name is required"))
	})
	run := h.create(t, "bad-request", 5)

	h.engine.Execute(ctx, h.lease(t))
	got := h.reload(t, run.ID)
	if got.State != core.StateFailed {
		t.Fatalf("permanent error should fail immediately despite the budget, got %s", got.State)
	}
}

// TestDurableSleepReleasesTheWorker checks that a long wait comes back as a
// future SCHEDULED time rather than blocking a slot.
func TestDurableSleepReleasesTheWorker(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	var before, after atomic.Int64
	h.reg.Register("waiter", func(c *sdk.Context) (any, error) {
		if err := sdk.Do(c, "before", func(*sdk.Context) error {
			before.Add(1)
			return nil
		}); err != nil {
			return nil, err
		}
		if err := sdk.Sleep(c, "settle", time.Hour); err != nil {
			return nil, err
		}
		after.Add(1)
		return "done", nil
	})

	run := h.create(t, "waiter", 0)
	h.engine.Execute(ctx, h.lease(t))

	got := h.reload(t, run.ID)
	if got.State != core.StateScheduled || got.StateName != "Suspended" {
		t.Fatalf("expected a suspended run, got %s/%s", got.State, got.StateName)
	}
	if time.Until(got.ScheduledAt) < 55*time.Minute {
		t.Fatalf("wake time not honoured: %s", got.ScheduledAt)
	}
	if got.WorkerID != nil {
		t.Fatal("suspended run should not hold a worker")
	}
	if before.Load() != 1 || after.Load() != 0 {
		t.Fatalf("unexpected execution: before=%d after=%d", before.Load(), after.Load())
	}

	// Bring the wake time forward and resume: the pre-sleep step is replayed
	// from its checkpoint, not re-executed.
	if _, err := h.store.RescheduleRun(ctx, run.ID, time.Now().UTC().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	// The recorded wake time is in the past now only because we moved the run;
	// the checkpoint still holds the original instant, so shorten it directly.
	if _, err := h.store.DB().ExecContext(ctx,
		`UPDATE pf_task_runs SET result = to_jsonb($2::text) WHERE flow_run_id=$1 AND task_key='wait:settle'`,
		run.ID, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	h.engine.Execute(ctx, h.lease(t))

	got = h.reload(t, run.ID)
	if got.State != core.StateCompleted {
		t.Fatalf("resumed run should complete, got %s (%s)", got.State, got.StateMessage)
	}
	if before.Load() != 1 {
		t.Fatalf("pre-sleep step re-executed: %d calls", before.Load())
	}
	if after.Load() != 1 {
		t.Fatalf("post-sleep step did not run: %d calls", after.Load())
	}
}

func TestCancellationStopsARunningFlow(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	started := make(chan struct{})
	h.reg.Register("long", func(c *sdk.Context) (any, error) {
		close(started)
		return sdk.Task(c, "block", func(c *sdk.Context) (string, error) {
			select {
			case <-c.Done():
				return "", c.Err()
			case <-time.After(10 * time.Second):
				return "finished", nil
			}
		})
	})

	run := h.create(t, "long", 3)
	leased := h.lease(t)

	done := make(chan struct{})
	go func() { defer close(done); h.engine.Execute(ctx, leased) }()

	<-started
	if _, err := h.store.RequestCancel(ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("cancellation did not stop the run")
	}

	got := h.reload(t, run.ID)
	if got.State != core.StateCancelled {
		t.Fatalf("expected CANCELLED, got %s (%s)", got.State, got.StateMessage)
	}
}

func TestUnknownFlowGoesBackToTheQueue(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	run := h.create(t, "not-registered-here", 0)
	h.engine.Execute(ctx, h.lease(t))

	got := h.reload(t, run.ID)
	if got.State != core.StateScheduled || got.StateName != "AwaitingWorker" {
		t.Fatalf("an unknown flow must be requeued for another worker, got %s/%s",
			got.State, got.StateName)
	}
}

func TestLogsAndArtifactsArePersisted(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.reg.Register("chatty", func(c *sdk.Context) (any, error) {
		c.Info("starting", "org", "acme")
		c.Warn("slow response", "ms", 1200)
		if err := c.Markdown("summary", "### done"); err != nil {
			return nil, err
		}
		return nil, nil
	})

	run := h.create(t, "chatty", 0)
	h.engine.Execute(ctx, h.lease(t))

	if got := h.reload(t, run.ID); got.State != core.StateCompleted {
		t.Fatalf("run failed: %s %s", got.State, got.StateMessage)
	}
	logs, err := h.store.ListLogs(ctx, run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) < 2 {
		t.Fatalf("expected the flow's logs to be persisted, got %d", len(logs))
	}
	arts, err := h.store.ListArtifacts(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(arts) != 1 || arts[0].Key != "summary" {
		t.Fatalf("expected one artifact named summary, got %+v", arts)
	}
}

func TestStateChangesEmitEvents(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.reg.Register("quick", func(c *sdk.Context) (any, error) { return "ok", nil })
	run := h.create(t, "quick", 0)
	h.engine.Execute(ctx, h.lease(t))

	evs, err := h.store.ListEvents(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	var sawRunning, sawCompleted bool
	for _, e := range evs {
		if e.ResourceID != run.ID {
			continue
		}
		switch e.Name {
		case "flow-run.RUNNING":
			sawRunning = true
		case "flow-run.COMPLETED":
			sawCompleted = true
		}
	}
	if !sawRunning || !sawCompleted {
		t.Fatalf("missing state events (running=%v completed=%v)", sawRunning, sawCompleted)
	}
}

// TestWaitingDoesNotSpendTheRetryBudget is the end-to-end form of the contract
// engine.finish documents: "a suspension is not a failure, and the attempt does
// not count against the retry budget".
//
// The dispatcher increments run_count on every lease and engine.finish reads
// run_count as the attempt number, so without StateOpts.Resume a flow that waits
// arrives at its first real error with the budget already spent. A
// RunDeploymentAndWait parent re-suspends every 30 seconds, which made this the
// normal case for any flow with children rather than an edge case.
func TestWaitingDoesNotSpendTheRetryBudget(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const waits = 3
	var attempts atomic.Int64
	h.reg.Register("patient", func(c *sdk.Context) (any, error) {
		attempts.Add(1)
		for i := 0; i < waits; i++ {
			if err := sdk.Sleep(c, fmt.Sprintf("wait-%d", i), time.Hour); err != nil {
				return nil, err
			}
		}
		return nil, errors.New("boom")
	})

	run := h.create(t, "patient", 2) // two retries after the first attempt

	// Each pass suspends on the next sleep. The run keeps returning to the lane
	// and being re-leased, and must still read as attempt 1 throughout.
	for i := 0; i < waits; i++ {
		leased := h.lease(t)
		if leased.RunCount != 1 {
			t.Fatalf("wait %d: leased at attempt %d, want 1", i, leased.RunCount)
		}
		h.engine.Execute(ctx, leased)

		got := h.reload(t, run.ID)
		if got.State != core.StateScheduled || got.StateName != "Suspended" {
			t.Fatalf("wait %d: got %s/%s, want SCHEDULED/Suspended", i, got.State, got.StateName)
		}
		// Bring the wake time forward, both on the row and in the checkpoint the
		// SDK reads back, so the next pass falls through this sleep.
		if _, err := h.store.RescheduleRun(ctx, run.ID, time.Now().UTC().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.DB().ExecContext(ctx,
			`UPDATE pf_task_runs SET result = to_jsonb($2::text)
			  WHERE flow_run_id=$1 AND task_key=$3`,
			run.ID, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano),
			fmt.Sprintf("wait:wait-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Past every sleep now: the flow reaches its error. That is attempt 1 of 3.
	h.engine.Execute(ctx, h.lease(t))
	got := h.reload(t, run.ID)
	if got.State != core.StateScheduled || got.StateName != "AwaitingRetry" {
		t.Fatalf("first real failure after %d waits: got %s/%s, want SCHEDULED/AwaitingRetry — "+
			"the waits spent the retry budget", waits, got.State, got.StateName)
	}

	// Burn the remaining two attempts; only then is the run failed.
	for i := 2; i <= 3; i++ {
		if _, err := h.store.RescheduleRun(ctx, run.ID, time.Now().UTC().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
		leased := h.lease(t)
		if leased.RunCount != i {
			t.Fatalf("expected attempt %d, got %d", i, leased.RunCount)
		}
		h.engine.Execute(ctx, leased)
	}

	got = h.reload(t, run.ID)
	if got.State != core.StateFailed {
		t.Fatalf("run should be FAILED once the budget is exhausted, got %s", got.State)
	}
	if attempts.Load() != waits+3 {
		t.Errorf("flow function ran %d times, want %d (%d replays through the waits + 3 attempts)",
			attempts.Load(), waits+3, waits)
	}
}
