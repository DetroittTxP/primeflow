package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

func TestSubflowLineageAndParentResume(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	parent := mkRun(t, st, "default", 50, time.Now().Add(-time.Second))

	// Two children of the parent.
	pid := parent.ID
	c1, err := st.CreateFlowRun(ctx, store.CreateRunInput{
		FlowName: "child", WorkQueue: "default", ScheduledAt: time.Now(),
		ParentRunID: &pid, ParentTaskKey: "subflow:child-1",
	})
	if err != nil {
		t.Fatalf("child 1: %v", err)
	}
	c2, err := st.CreateFlowRun(ctx, store.CreateRunInput{
		FlowName: "child", WorkQueue: "default", ScheduledAt: time.Now(),
		ParentRunID: &pid, ParentTaskKey: "subflow:child-2",
	})
	if err != nil {
		t.Fatalf("child 2: %v", err)
	}

	kids, err := st.ListChildRuns(ctx, parent.ID)
	if err != nil || len(kids) != 2 {
		t.Fatalf("ListChildRuns: %d %v", len(kids), err)
	}
	if kids[0].ParentRunID == nil || *kids[0].ParentRunID != parent.ID {
		t.Fatalf("child parent link not persisted: %+v", kids[0])
	}

	if n, _ := st.CountUnfinishedChildren(ctx, parent.ID); n != 2 {
		t.Fatalf("want 2 unfinished children, got %d", n)
	}

	// Finish one child -> still one outstanding.
	term := time.Now().UTC()
	if _, err := st.SetFlowRunState(ctx, c1.ID,
		core.NewState(core.StateCompleted, "Completed", ""),
		store.StateOpts{EndedAt: &term, Force: true}); err != nil {
		t.Fatalf("complete c1: %v", err)
	}
	if n, _ := st.CountUnfinishedChildren(ctx, parent.ID); n != 1 {
		t.Fatalf("want 1 unfinished child, got %d", n)
	}

	// Finish the last -> zero, and the parent is now resumable.
	if _, err := st.SetFlowRunState(ctx, c2.ID,
		core.NewState(core.StateFailed, "Failed", "boom"),
		store.StateOpts{EndedAt: &term, Force: true}); err != nil {
		t.Fatalf("fail c2: %v", err)
	}
	if n, _ := st.CountUnfinishedChildren(ctx, parent.ID); n != 0 {
		t.Fatalf("want 0 unfinished children, got %d", n)
	}

	// The parent is still SCHEDULED (never leased in this test), so a resume
	// succeeds and only moves scheduled_at forward.
	woken, err := st.ResumeSuspendedRun(ctx, parent.ID, time.Now().UTC())
	if err != nil || woken.State != core.StateScheduled {
		t.Fatalf("ResumeSuspendedRun on a suspended parent: %v (state %s)", err, woken.State)
	}
	// Lease it -> RUNNING -> a resume must now be a no-op (ErrNotFound).
	leased, _ := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w", Queues: []string{"default"}, Max: 5, LeaseFor: time.Minute,
	})
	var running bool
	for _, r := range leased {
		if r.ID == parent.ID {
			running = true
		}
	}
	if running {
		if _, err := st.ResumeSuspendedRun(ctx, parent.ID, time.Now().UTC()); err != store.ErrNotFound {
			t.Fatalf("ResumeSuspendedRun must not touch a RUNNING parent, got %v", err)
		}
	}
	// A CRASHED child, by contrast, still counts as unfinished.
	if _, err := st.SetFlowRunState(ctx, c1.ID,
		core.NewState(core.StateScheduled, "Rerun", ""), store.StateOpts{Force: true}); err == nil {
		if _, err := st.SetFlowRunState(ctx, c1.ID,
			core.NewState(core.StateCrashed, "Crashed", ""), store.StateOpts{Force: true}); err == nil {
			if n, _ := st.CountUnfinishedChildren(ctx, parent.ID); n != 1 {
				t.Fatalf("a CRASHED child must count as unfinished, got %d", n)
			}
		}
	}
}

func TestAncestorDeploymentIDs(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	// Build deployments A and B, then a run chain runA -> runB -> runC.
	depA := &core.Deployment{ID: "dep-a", Name: "dep-a", FlowName: "f"}
	depB := &core.Deployment{ID: "dep-b", Name: "dep-b", FlowName: "f"}
	if err := st.UpsertDeployment(ctx, depA); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertDeployment(ctx, depB); err != nil {
		t.Fatal(err)
	}

	runA, _ := st.CreateFlowRun(ctx, store.CreateRunInput{FlowName: "f", DeploymentID: &depA.ID, WorkQueue: "default", ScheduledAt: time.Now()})
	aID := runA.ID
	runB, _ := st.CreateFlowRun(ctx, store.CreateRunInput{FlowName: "f", DeploymentID: &depB.ID, WorkQueue: "default", ScheduledAt: time.Now(), ParentRunID: &aID})
	bID := runB.ID
	runC, _ := st.CreateFlowRun(ctx, store.CreateRunInput{FlowName: "f", DeploymentID: &depA.ID, WorkQueue: "default", ScheduledAt: time.Now(), ParentRunID: &bID})

	ids, err := st.AncestorDeploymentIDs(ctx, runC.ID, 8)
	if err != nil {
		t.Fatalf("AncestorDeploymentIDs: %v", err)
	}
	// nearest first: runB's deployment (dep-b), then runA's (dep-a).
	if len(ids) != 2 || ids[0] != "dep-b" || ids[1] != "dep-a" {
		t.Fatalf("ancestor chain wrong: %v", ids)
	}
}

func TestLogRetentionDelete(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	run := mkRun(t, st, "default", 50, time.Now())

	// One old log line, one fresh.
	if _, err := st.DB().ExecContext(ctx,
		`INSERT INTO pf_logs (flow_run_id, level, message, ts) VALUES ($1,'INFO','old', now() - interval '10 days')`,
		run.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendLogs(ctx, []core.LogRecord{{FlowRunID: run.ID, Level: "INFO", Message: "new", Timestamp: time.Now().UTC()}}); err != nil {
		t.Fatal(err)
	}

	n, more, err := st.DeleteLogsOlderThan(ctx, time.Now().Add(-24*time.Hour), 1000)
	if err != nil || n != 1 || more {
		t.Fatalf("DeleteLogsOlderThan: n=%d more=%v err=%v", n, more, err)
	}
	logs, _ := st.ListLogs(ctx, run.ID, 0, 100)
	if len(logs) != 1 || logs[0].Message != "new" {
		t.Fatalf("retention deleted the wrong rows: %+v", logs)
	}
}

func TestWorkPoolFieldsRoundTrip(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	max := 12
	q := &core.WorkQueue{
		Name: "vcd", Description: "vCD provisioning",
		MinWorkers: 2, MaxWorkers: &max, TargetReadyPerWorker: 4,
		Owner: "platform-team", PoolType: "pull",
	}
	if err := st.UpsertWorkQueue(ctx, q); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, err := st.GetWorkQueue(ctx, "vcd")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.MinWorkers != 2 || got.MaxWorkers == nil || *got.MaxWorkers != 12 ||
		got.TargetReadyPerWorker != 4 || got.Owner != "platform-team" || got.PoolType != "pull" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	stats, _ := st.QueueStats(ctx)
	var found bool
	for _, s := range stats {
		if s.Name == "vcd" {
			found = true
			if s.MinWorkers != 2 || s.TargetReadyPerWorker != 4 {
				t.Fatalf("QueueStats missing pool fields: %+v", s)
			}
		}
	}
	if !found {
		t.Fatal("vcd not in QueueStats")
	}
}
