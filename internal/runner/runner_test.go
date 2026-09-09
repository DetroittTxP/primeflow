package runner

import (
	"context"
	"strings"
	"testing"

	"github.com/DetroittTxP/primeflow/internal/worker"
)

// An unusable exec mode must stop the worker at start-up rather than at the
// first leased run, when it would already be holding one.
func TestServeWorkerRejectsAnUnknownExecMode(t *testing.T) {
	err := ServeWorker(context.Background(), Deps{}, Config{ExecMode: "docker"})
	if err == nil {
		t.Fatal("ServeWorker accepted an unknown exec mode")
	}
	for _, want := range []string{"docker", worker.ExecInline, worker.ExecProcess} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestLeasedNeedsBothVariables(t *testing.T) {
	for _, tc := range []struct {
		name            string
		runID, workerID string
		want            bool
	}{
		{"neither", "", "", false},
		{"run only", "run-1", "", false},
		{"worker only", "", "worker-1", false},
		{"both", "run-1", "worker-1", true},
	} {
		t.Setenv(worker.EnvRunID, tc.runID)
		t.Setenv(worker.EnvLeaseWorkerID, tc.workerID)
		if _, _, ok := Leased(); ok != tc.want {
			t.Errorf("%s: Leased() ok = %v, want %v", tc.name, ok, tc.want)
		}
	}
}
