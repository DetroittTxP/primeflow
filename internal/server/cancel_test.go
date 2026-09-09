package server_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/store"
)

// Cancelling a run that already finished is a conflict, not a silent 200 that
// leaves cancel_requested set on a COMPLETED run.
func TestCancelFinishedRunIsRefused(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	run := a.mkRun("default")
	for _, st := range []core.StateType{core.StateRunning, core.StateCompleted} {
		if _, err := a.store.SetFlowRunState(ctx, run.ID,
			core.NewState(st, string(st), ""), store.StateOpts{}); err != nil {
			t.Fatalf("drive run to %s: %v", st, err)
		}
	}

	rec := a.call(http.MethodPost, "/api/v1/runs/"+run.ID+"/cancel", nil, testWorker)
	if rec.Code != http.StatusConflict {
		t.Fatalf("want 409, got %d %s", rec.Code, rec.Body.String())
	}

	var after core.FlowRun
	a.ok(http.MethodGet, "/api/v1/runs/"+run.ID, nil, &after)
	if after.State != core.StateCompleted || after.CancelRequest {
		t.Fatalf("refused cancel must leave the run untouched: %s cancel_requested=%v",
			after.State, after.CancelRequest)
	}
}
