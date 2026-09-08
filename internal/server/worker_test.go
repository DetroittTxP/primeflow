package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/primex/primeflow/internal/bus"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/server"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/internal/store/postgres"
)

const (
	testToken  = "worker-api-test-token"
	testWorker = "worker-under-test"
)

type api struct {
	t     *testing.T
	h     http.Handler
	store *postgres.Store
}

func newAPI(t *testing.T) *api {
	t.Helper()
	dsn := os.Getenv("PRIMEFLOW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set PRIMEFLOW_TEST_DATABASE_URL to run worker API integration tests")
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
	srv := server.New(st, b, events.New(st, b, log), log, server.Config{APIToken: testToken})
	return &api{t: t, h: srv.Handler(), store: st}
}

// call drives one request as a worker would: machine bearer token plus the
// worker identity header. Passing worker="" omits the header.
func (a *api) call(method, path string, body any, worker string) *httptest.ResponseRecorder {
	a.t.Helper()
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			a.t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testToken)
	if worker != "" {
		req.Header.Set("X-PrimeFlow-Worker-ID", worker)
	}
	rec := httptest.NewRecorder()
	a.h.ServeHTTP(rec, req)
	return rec
}

func (a *api) ok(method, path string, body any, out any) {
	a.t.Helper()
	rec := a.call(method, path, body, testWorker)
	if rec.Code >= 300 {
		a.t.Fatalf("%s %s: %d %s", method, path, rec.Code, rec.Body.String())
	}
	if out != nil {
		if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
			a.t.Fatalf("%s %s: decode response: %v (%s)", method, path, err, rec.Body.String())
		}
	}
}

func (a *api) mkRun(queue string) *core.FlowRun {
	a.t.Helper()
	r, err := a.store.CreateFlowRun(context.Background(), store.CreateRunInput{
		FlowName: "demo", WorkQueue: queue, Priority: 50,
		ScheduledAt: time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		a.t.Fatalf("create run: %v", err)
	}
	return r
}

// A run leased and driven to COMPLETED entirely over HTTP has to land the same
// rows a locally connected worker would, and the events the worker can no
// longer write itself have to be there too.
func TestWorkerAPIDrivesARunEndToEnd(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	run := a.mkRun("default")

	var leased []core.FlowRun
	a.ok(http.MethodPost, "/api/v1/worker/lease",
		map[string]any{"queues": []string{"default"}, "max": 4, "lease": "60s"}, &leased)
	if len(leased) != 1 || leased[0].ID != run.ID {
		t.Fatalf("lease returned %d runs, want the one we created", len(leased))
	}
	if leased[0].State != core.StatePending {
		t.Fatalf("leased run state = %s, want PENDING", leased[0].State)
	}
	// The lease is attributed to the header identity, never to anything in the
	// body — the body has no field for it.
	if leased[0].WorkerID == nil || *leased[0].WorkerID != testWorker {
		t.Fatalf("lease not attributed to the credential: %v", leased[0].WorkerID)
	}

	a.ok(http.MethodPost, "/api/v1/worker/runs/"+run.ID+"/renew", map[string]any{"lease": "60s"}, nil)

	var running core.FlowRun
	a.ok(http.MethodPost, "/api/v1/worker/runs/"+run.ID+"/state",
		map[string]any{"state": "RUNNING", "state_name": "Running"}, &running)
	if running.State != core.StateRunning {
		t.Fatalf("state = %s, want RUNNING", running.State)
	}

	// A checkpoint, a log line and an artifact — everything a flow writes.
	var tr core.TaskRun
	a.ok(http.MethodPut, "/api/v1/worker/runs/"+run.ID+"/tasks/step-1", core.TaskRun{
		TaskName: "step", State: core.StateCompleted, StateName: "Completed",
		Result: json.RawMessage(`42`),
	}, &tr)
	var back core.TaskRun
	a.ok(http.MethodGet, "/api/v1/worker/runs/"+run.ID+"/tasks/step-1", nil, &back)
	if string(back.Result) != "42" || back.TaskKey != "step-1" {
		t.Fatalf("checkpoint did not round-trip: %+v", back)
	}

	a.ok(http.MethodPost, "/api/v1/worker/runs/"+run.ID+"/logs", map[string]any{
		"records": []core.LogRecord{{Level: "INFO", Message: "over http", Timestamp: time.Now().UTC()}},
	}, nil)
	a.ok(http.MethodPost, "/api/v1/worker/runs/"+run.ID+"/artifacts", core.Artifact{
		Key: "summary", Kind: core.ArtifactMarkdown, Data: json.RawMessage(`"done"`),
	}, nil)

	ended := time.Now().UTC()
	var done core.FlowRun
	a.ok(http.MethodPost, "/api/v1/worker/runs/"+run.ID+"/state", map[string]any{
		"state": "COMPLETED", "state_name": "Completed",
		"ended_at": ended, "clear_lease": true, "result": json.RawMessage(`{"sum":42}`),
	}, &done)
	if done.State != core.StateCompleted {
		t.Fatalf("state = %s (%s), want COMPLETED", done.State, done.StateMessage)
	}

	// The rows a local worker would have written are all present.
	tasks, err := a.store.ListTaskRuns(ctx, run.ID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("checkpoints: %v (%d)", err, len(tasks))
	}
	logs, err := a.store.ListLogs(ctx, run.ID, 0, 10)
	if err != nil || len(logs) != 1 {
		t.Fatalf("logs: %v (%d)", err, len(logs))
	}
	arts, err := a.store.ListArtifacts(ctx, run.ID)
	if err != nil || len(arts) != 1 {
		t.Fatalf("artifacts: %v (%d)", err, len(arts))
	}

	// And the server emitted on the worker's behalf, attributing the caller —
	// which the run row itself can no longer say, having released its lease.
	evs, err := a.store.ListEvents(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	var sawCompleted bool
	for _, e := range evs {
		if e.Name != "flow-run.COMPLETED" {
			continue
		}
		var p events.FlowRunPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatalf("event payload: %v", err)
		}
		if p.RunID != run.ID {
			continue
		}
		sawCompleted = true
		if p.WorkerID != testWorker {
			t.Errorf("event not attributed: worker_id = %q, want %q", p.WorkerID, testWorker)
		}
	}
	if !sawCompleted {
		t.Fatal("no flow-run.COMPLETED event was recorded for the run")
	}
	if done.WorkerID != nil {
		t.Errorf("settled run should have released its lease, still holds %q", *done.WorkerID)
	}
}

// Force skips the transition table. It belongs to the janitor, which runs
// centrally, and a worker must not be able to ask for it.
func TestWorkerAPIRefusesForce(t *testing.T) {
	a := newAPI(t)
	run := a.mkRun("default")
	rec := a.call(http.MethodPost, "/api/v1/worker/runs/"+run.ID+"/state",
		map[string]any{"state": "COMPLETED", "force": true}, testWorker)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("force was not refused: %d %s", rec.Code, rec.Body.String())
	}
	got, err := a.store.GetFlowRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != core.StateScheduled {
		t.Fatalf("refused request still changed state to %s", got.State)
	}
}

// The transition table is the server's, not the worker's: a nonsense hop is
// rejected by the same rule that governs a locally connected worker.
func TestWorkerAPIEnforcesTheTransitionTable(t *testing.T) {
	a := newAPI(t)
	run := a.mkRun("default")
	// Lease it first, so what the server refuses is the hop itself and not the
	// ownership precondition.
	a.ok(http.MethodPost, "/api/v1/worker/lease",
		map[string]any{"queues": []string{"default"}, "max": 1}, nil)

	rec := a.call(http.MethodPost, "/api/v1/worker/runs/"+run.ID+"/state",
		map[string]any{"state": "COMPLETED"}, testWorker)
	if rec.Code != http.StatusConflict {
		t.Fatalf("PENDING -> COMPLETED should conflict, got %d %s", rec.Code, rec.Body.String())
	}

	rec = a.call(http.MethodPost, "/api/v1/worker/runs/"+run.ID+"/state",
		map[string]any{"state": "NONSENSE"}, testWorker)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown state should be rejected, got %d", rec.Code)
	}
}

// A worker whose lease was reclaimed must not be able to finish work another
// worker has taken over. SetFlowRunState only guarded the state it read, so
// without the ownership precondition any caller in the right state could win.
func TestWorkerAPIRefusesAStateWriteFromANonHolder(t *testing.T) {
	a := newAPI(t)
	run := a.mkRun("default")

	var leased []core.FlowRun
	a.ok(http.MethodPost, "/api/v1/worker/lease",
		map[string]any{"queues": []string{"default"}, "max": 1}, &leased)
	if len(leased) != 1 {
		t.Fatalf("expected to lease the run, got %d", len(leased))
	}

	// A different worker, correct state, valid transition — and still refused.
	rec := a.call(http.MethodPost, "/api/v1/worker/runs/"+run.ID+"/state",
		map[string]any{"state": "RUNNING"}, "some-other-worker")
	if rec.Code != http.StatusConflict {
		t.Fatalf("a non-holder drove the run to RUNNING: %d %s", rec.Code, rec.Body.String())
	}

	got, err := a.store.GetFlowRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != core.StatePending {
		t.Fatalf("state changed to %s despite the refusal", got.State)
	}

	// The holder still can.
	a.ok(http.MethodPost, "/api/v1/worker/runs/"+run.ID+"/state",
		map[string]any{"state": "RUNNING"}, nil)
}

// Identity comes from the credential. Without it the server has no one to
// attribute a lease to and refuses rather than guessing.
func TestWorkerAPIRequiresAnIdentity(t *testing.T) {
	a := newAPI(t)
	rec := a.call(http.MethodPost, "/api/v1/worker/lease",
		map[string]any{"queues": []string{"default"}, "max": 1}, "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("anonymous lease should be refused, got %d %s", rec.Code, rec.Body.String())
	}
}

// The worker's boot-time "make sure my lane exists" call must not reset what an
// operator configured — the same guarantee EnsureWorkQueue gives locally.
func TestWorkerAPIEnsureQueuePreservesConfiguration(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	limit := 3
	if err := a.store.UpsertWorkQueue(ctx, &core.WorkQueue{
		Name: "vcd", ConcurrencyLimit: &limit, Paused: true, MinWorkers: 2, Owner: "platform",
	}); err != nil {
		t.Fatal(err)
	}
	a.ok(http.MethodPost, "/api/v1/worker/queues/vcd", nil, nil)

	got, err := a.store.GetWorkQueue(ctx, "vcd")
	if err != nil {
		t.Fatal(err)
	}
	if got.ConcurrencyLimit == nil || *got.ConcurrencyLimit != limit || !got.Paused ||
		got.MinWorkers != 2 || got.Owner != "platform" {
		t.Fatalf("worker boot reset the lane's configuration: %+v", got)
	}
}

// A paused lane hands out nothing, and the cache lookup answers on server time
// — the request has no field with which to claim a different "now".
func TestWorkerAPIRespectsQueuePauseAndServerClock(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	if err := a.store.UpsertWorkQueue(ctx, &core.WorkQueue{Name: "held", Paused: true}); err != nil {
		t.Fatal(err)
	}
	a.mkRun("held")

	var leased []core.FlowRun
	a.ok(http.MethodPost, "/api/v1/worker/lease",
		map[string]any{"queues": []string{"held"}, "max": 4}, &leased)
	if len(leased) != 0 {
		t.Fatalf("paused lane dispatched %d runs", len(leased))
	}

	var miss struct {
		Hit bool `json:"hit"`
	}
	a.ok(http.MethodGet, "/api/v1/worker/cache/nothing-cached-here", nil, &miss)
	if miss.Hit {
		t.Fatal("cache reported a hit for a key that was never written")
	}
}
