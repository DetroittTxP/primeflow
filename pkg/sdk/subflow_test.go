package sdk_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/primex/primeflow/pkg/sdk"
)

// replayCtx builds a fresh Context over the same runtime. It stands in for the
// parent being rescheduled after a suspension: task keys count from one again
// and the only memory of the earlier attempt is what was checkpointed.
func replayCtx(rt sdk.Runtime) *sdk.Context {
	return sdk.NewContext(context.Background(), rt, sdk.RunInfo{RunID: "parent-1"})
}

const auditKey = "subflow:site-audit-site-a-1"

func TestRunDeploymentAndWaitClosesLaneOnCompletion(t *testing.T) {
	rt := newFake()
	rt.runStates["child-1"] = sdk.RunState{Status: "RUNNING"}

	_, err := replayCtx(rt).RunDeploymentAndWait("site-audit-site-a", nil)
	if _, ok := sdk.IsSuspend(err); !ok {
		t.Fatalf("child still running: want a suspension, got %v", err)
	}
	if got := rt.cps[auditKey].Status; got != sdk.CheckpointRunning {
		t.Fatalf("lane status while waiting = %q, want RUNNING", got)
	}
	if rt.cps[auditKey].Started == nil {
		t.Fatal("the trigger left the lane without a start time")
	}

	rt.runStates["child-1"] = sdk.RunState{Status: "COMPLETED", Result: json.RawMessage(`{"ok":true}`)}
	got, err := replayCtx(rt).RunDeploymentAndWait("site-audit-site-a", nil)
	if err != nil {
		t.Fatalf("child completed: %v", err)
	}
	if got.RunID != "child-1" || string(got.Result) != `{"ok":true}` {
		t.Fatalf("child result = %+v", got)
	}

	cp := rt.cps[auditKey]
	if cp.Status != sdk.CheckpointCompleted {
		t.Fatalf("lane status after the child landed = %q, want COMPLETED", cp.Status)
	}
	// Result stays the child run id — the replay path reads it back to recover
	// childID, so overwriting it with the child's payload would re-trigger.
	var childID string
	if err := json.Unmarshal(cp.Result, &childID); err != nil || childID != "child-1" {
		t.Fatalf("settled checkpoint result = %s, want the child run id", cp.Result)
	}
	// Settling carries no Started of its own: the store keeps the one the
	// trigger stamped, and the engine times a task's span from the Started on
	// the terminal checkpoint, so a fresh one here would report ~0s for a wait
	// that took minutes.
	if cp.Started != nil {
		t.Fatalf("settling stamped a second start time: %v", cp.Started)
	}
	if cp.Ended == nil {
		t.Fatal("settled lane has no end time")
	}
	if len(rt.triggers) != 1 {
		t.Fatalf("triggered %d times across the replay, want exactly 1", len(rt.triggers))
	}
}

func TestRunDeploymentAndWaitClosesLaneOnFailure(t *testing.T) {
	rt := newFake()
	rt.runStates["child-1"] = sdk.RunState{Status: "FAILED", Message: "disk full"}

	_, err := replayCtx(rt).RunDeploymentAndWait("site-audit-site-a", nil)
	if err == nil || !sdk.IsPermanent(err) {
		t.Fatalf("child failed: want a permanent error, got %v", err)
	}
	cp := rt.cps[auditKey]
	if cp.Status != sdk.CheckpointFailed {
		t.Fatalf("lane status after the child failed = %q, want FAILED", cp.Status)
	}
	var childID string
	if err := json.Unmarshal(cp.Result, &childID); err != nil || childID != "child-1" {
		t.Fatalf("settled checkpoint result = %s, want the child run id", cp.Result)
	}
}

// A settled checkpoint records what the console should draw, not an answer the
// parent may reuse. Flipping the child back to RUNNING is not a transition a
// real run makes; it is a probe that fails if the settled status is ever taken
// as a reason to skip reading the child's state.
func TestRunDeploymentAndWaitRereadsChildAfterSettling(t *testing.T) {
	rt := newFake()
	rt.runStates["child-1"] = sdk.RunState{Status: "COMPLETED"}
	if _, err := replayCtx(rt).RunDeploymentAndWait("site-audit-site-a", nil); err != nil {
		t.Fatalf("first wait: %v", err)
	}

	rt.runStates["child-1"] = sdk.RunState{Status: "RUNNING"}
	_, err := replayCtx(rt).RunDeploymentAndWait("site-audit-site-a", nil)
	if _, ok := sdk.IsSuspend(err); !ok {
		t.Fatalf("settled checkpoint short-circuited the state re-check: %v", err)
	}
	if len(rt.triggers) != 1 {
		t.Fatalf("triggered %d times, want exactly 1", len(rt.triggers))
	}
}
