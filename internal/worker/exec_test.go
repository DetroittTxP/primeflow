package worker

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DetroittTxP/primeflow/internal/core"
)

// TestExecChild is not a test of this package: it is the child process the
// launcher tests start, in the usual shape — this test binary re-executed with
// -test.run pointed back here. PF_EXEC_CHILD says what it should do.
func TestExecChild(t *testing.T) {
	switch os.Getenv("PF_EXEC_CHILD") {
	case "":
		t.Skip("not a launched child")
	case "ok":
		os.Exit(0)
	case "fail":
		os.Exit(3)
	case "assert-env":
		// The contract a real child relies on: which run, under whose lease,
		// never launching children of its own, and a route to the orchestrator
		// even when the parent was configured in Go.
		if os.Getenv(EnvRunID) != "run-1" ||
			os.Getenv(EnvLeaseWorkerID) != "worker-1" ||
			os.Getenv(EnvExecMode) != ExecInline ||
			os.Getenv("PRIMEFLOW_DATABASE_URL") != "postgres://child" {
			os.Exit(9)
		}
		os.Exit(0)
	case "sleep":
		time.Sleep(time.Minute)
		os.Exit(0) // reached only if the stop signal never arrives
	}
}

func testLauncher(t *testing.T, behaviour string, env ...string) *processLauncher {
	t.Helper()
	return newProcessLauncher(
		os.Args[0], []string{"-test.run=^TestExecChild$"},
		append([]string{"PF_EXEC_CHILD=" + behaviour}, env...),
		"worker-1", slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

var testRun = &core.FlowRun{ID: "run-1", FlowName: "child-flow"}

// A child that exits non-zero is a run nobody settled, which is the signal the
// worker turns into an expired lease and hands to the janitor.
func TestProcessLauncherReportsAnAbnormalExit(t *testing.T) {
	if err := testLauncher(t, "ok").Launch(testRun); err != nil {
		t.Errorf("Launch of a clean child = %v, want nil", err)
	}
	if err := testLauncher(t, "fail").Launch(testRun); err == nil {
		t.Error("Launch of a child that exited 3 returned nil")
	}
}

func TestProcessLauncherChildEnvironment(t *testing.T) {
	// PRIMEFLOW_EXEC_MODE is inherited as "process" and must not stay that way,
	// or the child would fork a grandchild per run.
	t.Setenv(EnvExecMode, ExecProcess)
	err := testLauncher(t, "assert-env", "PRIMEFLOW_DATABASE_URL=postgres://child").Launch(testRun)
	if err != nil {
		t.Errorf("child did not see the environment it was promised: %v", err)
	}
}

func TestProcessLauncherCancelStopsTheChild(t *testing.T) {
	l := testLauncher(t, "sleep")
	l.grace = time.Second

	done := make(chan error, 1)
	go func() { done <- l.Launch(testRun) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		l.mu.Lock()
		started := l.procs[testRun.ID] != nil
		l.mu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child never started")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !l.Cancel(testRun.ID) {
		t.Fatal("Cancel of a running child returned false")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Error("a cancelled child returned no error; the run would look settled")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled child did not exit")
	}

	if l.Cancel(testRun.ID) {
		t.Error("Cancel of a finished run returned true")
	}
	if l.Cancel("no-such-run") {
		t.Error("Cancel of an unknown run returned true")
	}
}

// The launcher must not leave a finished run behind in its table: the map is
// what Cancel looks in, and a stale entry there would signal the wrong process.
func TestProcessLauncherForgetsFinishedRuns(t *testing.T) {
	l := testLauncher(t, "ok")
	if err := l.Launch(testRun); err != nil {
		t.Fatalf("Launch: %v", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.procs) != 0 {
		t.Errorf("launcher still tracks %d process(es) after the run finished", len(l.procs))
	}
}

func TestExecModesAreTheDocumentedStrings(t *testing.T) {
	// They appear in worker.env files and in the README table, so they are API.
	for got, want := range map[string]string{ExecInline: "inline", ExecProcess: "process"} {
		if got != want {
			t.Errorf("exec mode constant = %q, want %q", got, want)
		}
	}
	if !strings.HasPrefix(EnvRunID, "PRIMEFLOW_") || !strings.HasPrefix(EnvLeaseWorkerID, "PRIMEFLOW_") {
		t.Error("child variables must stay in the PRIMEFLOW_ namespace")
	}
}
