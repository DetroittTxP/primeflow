package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DetroittTxP/primeflow/internal/apiauth"
	"github.com/DetroittTxP/primeflow/internal/bus"
	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/engine"
	"github.com/DetroittTxP/primeflow/internal/events"
	"github.com/DetroittTxP/primeflow/internal/server"
	"github.com/DetroittTxP/primeflow/internal/store"
	"github.com/DetroittTxP/primeflow/internal/store/postgres"
	"github.com/DetroittTxP/primeflow/internal/store/remote"
	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

const (
	testToken  = "worker-api-test-token"
	testWorker = "worker-under-test"
)

type api struct {
	t     *testing.T
	h     http.Handler
	store *postgres.Store
	bus   bus.Bus
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
         pf_automations, pf_deployments, pf_workers, pf_leader, pf_flows,
         pf_api_keys, pf_api_key_events RESTART IDENTITY CASCADE;
DELETE FROM pf_work_queues WHERE name <> 'default';
UPDATE pf_work_queues SET paused = false, concurrency_limit = NULL;`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := bus.NewInMemory()
	srv := server.New(st, b, events.New(st, b, log), log, server.Config{APIToken: testToken})
	return &api{t: t, h: srv.Handler(), store: st, bus: b}
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

// --- phase 2: pool-scoped worker credentials ---

// issueWorkerKey mints an api-worker key bound to the given pools and returns
// the secret, the way the console would hand it to a site operator.
func (a *api) issueWorkerKey(name string, pools []string) (secret, id string) {
	a.t.Helper()
	full, prefix, hash := apiauth.NewSecret()
	k := &core.APIKey{
		ID: "key-" + name, Name: name, Prefix: prefix, SecretHash: hash,
		Role: "api-worker", Active: true, Pools: pools,
	}
	if err := a.store.CreateAPIKey(context.Background(), k); err != nil {
		a.t.Fatalf("issue key: %v", err)
	}
	return full, k.ID
}

// callKey drives a request authenticated by an API key rather than the shared
// operator token.
func (a *api) callKey(method, path string, body any, secret, worker string) *httptest.ResponseRecorder {
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
	req.Header.Set("X-API-Key", secret)
	req.Header.Set("X-PrimeFlow-Worker-ID", worker)
	rec := httptest.NewRecorder()
	a.h.ServeHTTP(rec, req)
	return rec
}

// The phase-2 guarantee: a key bound to one site cannot lease from another's
// lane, and the refusal is visible to an operator in that key's audit trail.
func TestWorkerKeyIsBoundToItsPools(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	for _, q := range []string{"site-a", "site-b"} {
		if err := a.store.EnsureWorkQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	a.mkRun("site-a")
	runB := a.mkRun("site-b")
	secret, keyID := a.issueWorkerKey("site-a-worker", []string{"site-a"})

	// Its own lane works.
	rec := a.callKey(http.MethodPost, "/api/v1/worker/lease",
		map[string]any{"queues": []string{"site-a"}, "max": 4}, secret, "a-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("own lane refused: %d %s", rec.Code, rec.Body.String())
	}
	var mine []core.FlowRun
	if err := json.Unmarshal(rec.Body.Bytes(), &mine); err != nil {
		t.Fatal(err)
	}
	if len(mine) != 1 || mine[0].WorkQueue != "site-a" {
		t.Fatalf("expected one run from site-a, got %d", len(mine))
	}

	// Another site's lane hands back nothing, however it is asked for.
	rec = a.callKey(http.MethodPost, "/api/v1/worker/lease",
		map[string]any{"queues": []string{"site-b"}, "max": 4}, secret, "a-1")
	var other []core.FlowRun
	if err := json.Unmarshal(rec.Body.Bytes(), &other); err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatalf("key leased %d runs from a lane outside its pools", len(other))
	}
	// Including when it is smuggled in beside a permitted one.
	rec = a.callKey(http.MethodPost, "/api/v1/worker/lease",
		map[string]any{"queues": []string{"site-a", "site-b"}, "max": 4}, secret, "a-1")
	var mixed []core.FlowRun
	if err := json.Unmarshal(rec.Body.Bytes(), &mixed); err != nil {
		t.Fatal(err)
	}
	for _, r := range mixed {
		if r.WorkQueue != "site-a" {
			t.Fatalf("a mixed request leaked a run from %q", r.WorkQueue)
		}
	}

	// Knowing another site's run id is not enough either.
	rec = a.callKey(http.MethodPost, "/api/v1/worker/runs/"+runB.ID+"/state",
		map[string]any{"state": "RUNNING"}, secret, "a-1")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-site state write returned %d %s", rec.Code, rec.Body.String())
	}

	// And the operator can see it happen.
	evs, err := a.store.ListAPIKeyEvents(ctx, keyID, 50)
	if err != nil {
		t.Fatal(err)
	}
	var denied bool
	for _, e := range evs {
		if e.Action == "auth-denied" && bytes.Contains(e.Detail, []byte("pool-denied:site-b")) {
			denied = true
		}
	}
	if !denied {
		t.Fatalf("the refusal is not in the key's audit trail: %+v", evs)
	}
}

// A key whose role is not api-worker has none of the worker scopes, so it is
// refused before it can reach any handler.
func TestNonWorkerKeyCannotUseTheWorkerAPI(t *testing.T) {
	a := newAPI(t)
	full, prefix, hash := apiauth.NewSecret()
	if err := a.store.CreateAPIKey(context.Background(), &core.APIKey{
		ID: "key-readonly", Name: "reporting", Prefix: prefix, SecretHash: hash,
		Role: "api-readonly", Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	rec := a.callKey(http.MethodPost, "/api/v1/worker/lease",
		map[string]any{"queues": []string{"default"}, "max": 1}, full, "impostor")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("a read-only key leased work: %d %s", rec.Code, rec.Body.String())
	}
}

// A deactivated key stops working immediately — the revocation story that a
// shared database credential cannot offer.
func TestDeactivatedWorkerKeyIsRefused(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	secret, keyID := a.issueWorkerKey("retired", []string{"default"})

	rec := a.callKey(http.MethodPost, "/api/v1/worker/heartbeat",
		map[string]any{"name": "w", "queues": []string{"default"}}, secret, "w-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("active key refused: %d %s", rec.Code, rec.Body.String())
	}

	k, err := a.store.GetAPIKey(ctx, keyID)
	if err != nil {
		t.Fatal(err)
	}
	k.Active = false
	if err := a.store.UpdateAPIKey(ctx, k); err != nil {
		t.Fatal(err)
	}

	rec = a.callKey(http.MethodPost, "/api/v1/worker/heartbeat",
		map[string]any{"name": "w", "queues": []string{"default"}}, secret, "w-1")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("deactivated key still worked: %d %s", rec.Code, rec.Body.String())
	}
}

// --- phase 3: the engine running against the HTTP store ---

// The strongest thing the two halves can be asked to prove together: run the
// real engine, over the real HTTP client, against the real handlers, and check
// that a durable resume still skips work that already succeeded. If the client
// and the server disagreed about any wire shape — a duration, a checkpoint key,
// a state option — this is where it would show.
func TestEngineRunsThroughTheRemoteStore(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	if err := a.store.EnsureWorkQueue(ctx, "site"); err != nil {
		t.Fatal(err)
	}
	secret, _ := a.issueWorkerKey("engine-site", []string{"site"})

	srv := httptest.NewServer(a.h)
	t.Cleanup(srv.Close)

	rs, err := remote.New(remote.Config{
		BaseURL: srv.URL, Token: secret, WorkerID: "remote-engine",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	// The flow fails once after its first task succeeds, so the retry has to
	// replay that task from its checkpoint rather than run it again.
	var built, attempts atomic.Int32
	reg := sdk.NewRegistry()
	reg.Register("resume-me", func(c *sdk.Context) (any, error) {
		if _, err := sdk.Task(c, "expensive", func(c *sdk.Context) (string, error) {
			built.Add(1)
			c.Info("did the expensive thing")
			return "vm-1", nil
		}); err != nil {
			return nil, err
		}
		if attempts.Add(1) == 1 {
			return nil, errors.New("transient")
		}
		_ = c.Markdown("done", "second attempt succeeded")
		return map[string]any{"ok": true}, nil
	})

	eng := engine.New(rs, reg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
		engine.Config{
			WorkerID:           "remote-engine",
			CancelPollInterval: 50 * time.Millisecond,
			SuspendThreshold:   time.Second,
			LogFlushInterval:   20 * time.Millisecond,
		})

	run, err := a.store.CreateFlowRun(ctx, store.CreateRunInput{
		FlowName: "resume-me", WorkQueue: "site", Priority: 50,
		ScheduledAt: time.Now().UTC().Add(-time.Minute),
		Retries:     1, RetryDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Two attempts, both leased and executed entirely through HTTP.
	for i := 0; i < 2; i++ {
		var leased []core.FlowRun
		deadline := time.Now().Add(3 * time.Second)
		for {
			leased, err = rs.LeaseFlowRuns(ctx, store.LeaseRequest{
				Queues: []string{"site"}, Max: 1, LeaseFor: time.Minute,
			})
			if err != nil {
				t.Fatalf("attempt %d lease: %v", i+1, err)
			}
			if len(leased) == 1 || time.Now().After(deadline) {
				break
			}
			time.Sleep(20 * time.Millisecond) // waiting out the retry delay
		}
		if len(leased) != 1 {
			t.Fatalf("attempt %d: nothing leased", i+1)
		}
		eng.Execute(ctx, &leased[0])
	}

	got, err := a.store.GetFlowRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != core.StateCompleted {
		t.Fatalf("state = %s (%s), want COMPLETED", got.State, got.StateMessage)
	}
	if attempts.Load() != 2 {
		t.Fatalf("flow body ran %d times, want 2", attempts.Load())
	}
	// The guarantee the whole durability model rests on, now over HTTP.
	if built.Load() != 1 {
		t.Fatalf("the expensive task ran %d times; the checkpoint did not survive the retry", built.Load())
	}

	tasks, err := a.store.ListTaskRuns(ctx, run.ID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("checkpoints: %v (%d)", err, len(tasks))
	}
	arts, err := a.store.ListArtifacts(ctx, run.ID)
	if err != nil || len(arts) != 1 {
		t.Fatalf("artifacts: %v (%d)", err, len(arts))
	}
	logs, err := a.store.ListLogs(ctx, run.ID, 0, 10)
	if err != nil || len(logs) == 0 {
		t.Fatalf("logs: %v (%d)", err, len(logs))
	}
}

// A refusal is an answer. Retrying a 4xx would turn a clear rejection into a
// slow one, and — for a lease refused on pool grounds — would hammer the server
// on every poll.
func TestRemoteStoreDoesNotRetryARefusal(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeTestErr(w, http.StatusForbidden, "out of scope")
	}))
	defer srv.Close()

	rs, err := remote.New(remote.Config{
		BaseURL: srv.URL, Token: "pmx_x", WorkerID: "w",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.LeaseFlowRuns(context.Background(), store.LeaseRequest{
		Queues: []string{"nope"}, Max: 1, LeaseFor: time.Minute,
	}); err == nil {
		t.Fatal("expected the refusal to surface as an error")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("a 4xx was retried %d times; it should be asked once", n)
	}
}

// A 5xx is worth another try: the server may simply have been restarting.
func TestRemoteStoreRetriesAServerError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			writeTestErr(w, http.StatusServiceUnavailable, "restarting")
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	rs, err := remote.New(remote.Config{
		BaseURL: srv.URL, Token: "pmx_x", WorkerID: "w",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rs.LeaseFlowRuns(context.Background(), store.LeaseRequest{
		Queues: []string{"q"}, Max: 1, LeaseFor: time.Minute,
	}); err != nil {
		t.Fatalf("the retry did not recover: %v", err)
	}
	if n := calls.Load(); n != 3 {
		t.Fatalf("expected three attempts, got %d", n)
	}
}

func writeTestErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(`{"error":"` + msg + `"}`))
}

// --- phase 4: wake-ups over the same 443 ---

// The stream has to carry work notices for the lanes a key owns and stay silent
// about every other lane: telling a site that some run it cannot see exists is
// a leak, not a hint.
func TestWorkerStreamFiltersByPool(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	for _, q := range []string{"mine", "theirs"} {
		if err := a.store.EnsureWorkQueue(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	secret, _ := a.issueWorkerKey("streaming", []string{"mine"})

	srv := httptest.NewServer(a.h)
	defer srv.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/worker/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-API-Key", secret)
	req.Header.Set("X-PrimeFlow-Worker-ID", "s-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream refused: %d", resp.StatusCode)
	}

	lines := make(chan string, 32)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	waitFor(t, lines, ": connected")

	// Give the subscription a moment to land before publishing to it.
	time.Sleep(100 * time.Millisecond)
	_ = a.bus.Publish(ctx, bus.TopicWork, map[string]string{"queue": "theirs"})
	_ = a.bus.Publish(ctx, bus.TopicWork, map[string]string{"queue": "mine"})

	// The first data frame must be the permitted lane: the other one is not
	// delayed, it is never sent.
	data := waitForPrefix(t, lines, "data:")
	if !strings.Contains(data, `"mine"`) {
		t.Fatalf("stream leaked another site's lane: %s", data)
	}
	if strings.Contains(data, "theirs") {
		t.Fatalf("stream carried a lane outside the key's pools: %s", data)
	}
}

func waitFor(t *testing.T, lines <-chan string, want string) {
	t.Helper()
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatalf("stream ended before %q", want)
			}
			if strings.Contains(l, want) {
				return
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for %q", want)
		}
	}
}

func waitForPrefix(t *testing.T, lines <-chan string, prefix string) string {
	t.Helper()
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatal("stream ended before a data frame arrived")
			}
			if strings.HasPrefix(l, prefix) {
				return l
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for a %q line", prefix)
		}
	}
}

// Losing the stream must cost latency and nothing else. The client reconnects
// on its own, and while it is disconnected the worker's poll is what finds
// work — which is the contract the whole bus is held to.
func TestRemoteBusReconnectsAfterTheLinkDrops(t *testing.T) {
	var opens atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := opens.Add(1)
		f, ok := w.(http.Flusher)
		if !ok {
			t.Error("test server cannot stream")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: work\ndata: {\"queue\":\"q%d\"}\n\n", n)
		f.Flush()
		// Then the link drops mid-flight, exactly as a proxy timing out would.
	}))
	defer srv.Close()

	got := make(chan string, 8)
	b, err := remote.NewBus(remote.Config{
		BaseURL: srv.URL, Token: "pmx_x", WorkerID: "w",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := b.Subscribe(ctx, []string{bus.TopicWork, bus.TopicControl}, func(m bus.Message) {
		got <- string(m.Payload)
	}); err != nil {
		t.Fatal(err)
	}

	first := receive(t, got)
	if !strings.Contains(first, "q1") {
		t.Fatalf("first wake-up = %s", first)
	}
	// Same client, new connection, without anyone asking it to reconnect.
	second := receive(t, got)
	if !strings.Contains(second, "q2") {
		t.Fatalf("did not reconnect after the drop: %s", second)
	}
	if n := opens.Load(); n < 2 {
		t.Fatalf("expected the stream to be reopened, saw %d connections", n)
	}
}

// A stream the server refuses must not become a retry storm, and must not stop
// the worker: it degrades to polling and says so once.
func TestRemoteBusToleratesARefusedStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestErr(w, http.StatusForbidden, "no scope")
	}))
	defer srv.Close()

	b, err := remote.NewBus(remote.Config{
		BaseURL: srv.URL, Token: "pmx_x", WorkerID: "w",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	// Subscribe returns immediately whatever the server thinks of us.
	if err := b.Subscribe(ctx, []string{bus.TopicWork}, func(bus.Message) {
		t.Error("a refused stream delivered a message")
	}); err != nil {
		t.Fatalf("Subscribe should not fail on a refused stream: %v", err)
	}
	<-ctx.Done()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}

func receive(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("no wake-up arrived")
		return ""
	}
}

// --- phase 5: one call per interval, whatever the worker is holding ---

// The point of folding renewal and cancellation into the heartbeat is that a
// worker's request rate stops depending on how much work it is doing. Counting
// requests at two very different concurrencies is the only honest way to say
// that.
func TestHeartbeatCostIsIndependentOfHeldRuns(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	if err := a.store.EnsureWorkQueue(ctx, "busy"); err != nil {
		t.Fatal(err)
	}
	secret, _ := a.issueWorkerKey("busy-site", []string{"busy"})

	var requests atomic.Int32
	counted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		a.h.ServeHTTP(w, r)
	}))
	defer counted.Close()

	rs, err := remote.New(remote.Config{
		BaseURL: counted.URL, Token: secret, WorkerID: "busy-1",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	measure := func(held int) int32 {
		for i := 0; i < held; i++ {
			a.mkRun("busy")
		}
		leased, err := rs.LeaseFlowRuns(ctx, store.LeaseRequest{
			Queues: []string{"busy"}, Max: held, LeaseFor: time.Minute,
		})
		if err != nil || len(leased) != held {
			t.Fatalf("expected to hold %d runs, got %d (%v)", held, len(leased), err)
		}
		ids := make([]string, len(leased))
		for i := range leased {
			ids[i] = leased[i].ID
		}

		requests.Store(0)
		// One interval's worth of conversation.
		if err := rs.HeartbeatWorker(ctx, &core.WorkerInfo{
			ID: "busy-1", Name: "busy", Queues: []string{"busy"},
			Concurrency: held, ActiveRuns: held, StartedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
		renewed, _, err := rs.RenewLeases(ctx, "busy-1", ids, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(renewed) != held {
			t.Fatalf("renewed %d of %d leases", len(renewed), held)
		}
		return requests.Load()
	}

	atOne := measure(1)
	atEight := measure(8)
	if atOne != atEight {
		t.Fatalf("request cost scales with load: %d requests holding 1 run, %d holding 8", atOne, atEight)
	}
	// Two: the liveness beat and the batched renewal. Not 1+N.
	if atEight > 2 {
		t.Fatalf("expected at most two calls per interval, got %d", atEight)
	}
}

// Those two calls share one route, and only the first carries identity. The
// renewal must leave the registry alone: a worker holding runs is exactly the
// one an operator is looking at, and a row with no name, no lanes and no
// concurrency is not a worker.
func TestLeaseRenewalDoesNotBlankTheWorkerRegistry(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	if err := a.store.EnsureWorkQueue(ctx, "site-a"); err != nil {
		t.Fatal(err)
	}
	secret, _ := a.issueWorkerKey("site-a", []string{"site-a"})

	srv := httptest.NewServer(a.h)
	defer srv.Close()

	const wid = "site-a-vm1-fe4b37fe"
	rs, err := remote.New(remote.Config{
		BaseURL: srv.URL, Token: secret, WorkerID: wid,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := rs.HeartbeatWorker(ctx, &core.WorkerInfo{
		ID: wid, Name: "site-a-vm1", Queues: []string{"site-a"},
		Concurrency: 2, ActiveRuns: 1, StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	a.mkRun("site-a")
	leased, err := rs.LeaseFlowRuns(ctx, store.LeaseRequest{
		Queues: []string{"site-a"}, Max: 1, LeaseFor: time.Minute,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease a run: got %d (%v)", len(leased), err)
	}

	// The renewal half of the same interval: held runs and nothing else.
	renewed, _, err := rs.RenewLeases(ctx, wid, []string{leased[0].ID}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(renewed) != 1 {
		t.Fatalf("renewed %d of 1 leases", len(renewed))
	}

	ws, err := a.store.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got *core.WorkerInfo
	for i := range ws {
		if ws[i].ID == wid {
			got = &ws[i]
		}
	}
	if got == nil {
		t.Fatal("the worker vanished from the registry")
	}
	if got.Name != "site-a-vm1" {
		t.Errorf("name = %q, want \"site-a-vm1\"", got.Name)
	}
	if len(got.Queues) != 1 || got.Queues[0] != "site-a" {
		t.Errorf("queues = %v, want [site-a]", got.Queues)
	}
	if got.Concurrency != 2 {
		t.Errorf("concurrency = %d, want 2", got.Concurrency)
	}
}

// The same call has to tell a worker what it has lost and what it must stop,
// because those are the two things it cannot safely guess.
func TestHeartbeatReportsLostLeasesAndCancellations(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	if err := a.store.EnsureWorkQueue(ctx, "mixed"); err != nil {
		t.Fatal(err)
	}
	secret, _ := a.issueWorkerKey("mixed-site", []string{"mixed"})
	for i := 0; i < 2; i++ {
		a.mkRun("mixed")
	}
	srv := httptest.NewServer(a.h)
	defer srv.Close()
	rs, err := remote.New(remote.Config{
		BaseURL: srv.URL, Token: secret, WorkerID: "mixed-1",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	leased, err := rs.LeaseFlowRuns(ctx, store.LeaseRequest{
		Queues: []string{"mixed"}, Max: 2, LeaseFor: time.Minute,
	})
	if err != nil || len(leased) != 2 {
		t.Fatalf("lease: %v (%d)", err, len(leased))
	}
	keep, stop := leased[0].ID, leased[1].ID

	// One run is asked to stop; the other is reclaimed out from under us.
	if _, err := a.store.SetFlowRunState(ctx, stop,
		core.NewState(core.StateRunning, "Running", ""), store.StateOpts{}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.store.RequestCancel(ctx, stop); err != nil {
		t.Fatal(err)
	}
	stolen := "someone-else"
	if _, err := a.store.SetFlowRunState(ctx, keep,
		core.NewState(core.StateRunning, "Running", ""),
		store.StateOpts{WorkerID: &stolen}); err != nil {
		t.Fatal(err)
	}

	renewed, cancelling, err := rs.RenewLeases(ctx, "mixed-1", []string{keep, stop}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(renewed) != 1 || renewed[0] != stop {
		t.Fatalf("renewed = %v, want just the run we still hold (%s)", renewed, stop)
	}
	if len(cancelling) != 1 || cancelling[0] != stop {
		t.Fatalf("cancelling = %v, want %s", cancelling, stop)
	}
}

// --- phase 6: hardening ---

// cuttable proxies to the real handler until it is cut, after which every
// request fails as it would with the link to the site down.
type cuttable struct {
	target http.Handler
	down   atomic.Bool
}

func (c *cuttable) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if c.down.Load() {
		// Hijack and drop the connection: a severed link does not answer
		// politely, and a worker must handle that, not just a clean 503.
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		http.Error(w, "link down", http.StatusBadGateway)
		return
	}
	c.target.ServeHTTP(w, r)
}

// The guarantee everything else exists to protect, now across a severed WAN
// link: a site that loses its connection after doing the expensive work must
// not do that work again when the run is recovered.
func TestPartitionedSiteResumesFromItsCheckpoint(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	if err := a.store.EnsureWorkQueue(ctx, "cut"); err != nil {
		t.Fatal(err)
	}
	secret, _ := a.issueWorkerKey("cut-site", []string{"cut"})

	link := &cuttable{target: a.h}
	srv := httptest.NewServer(link)
	defer srv.Close()

	rs, err := remote.New(remote.Config{
		BaseURL: srv.URL, Token: secret, WorkerID: "cut-1",
		MaxAttempts: 1, // fail fast; the retry policy is covered elsewhere
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	var built, cutOnce atomic.Int32
	reg := sdk.NewRegistry()
	reg.Register("provision", func(c *sdk.Context) (any, error) {
		vm, err := sdk.Task(c, "create-vm", func(c *sdk.Context) (string, error) {
			built.Add(1) // the side effect that must never happen twice
			return "vm-1", nil
		})
		if err != nil {
			return nil, err
		}
		// The link dies once the expensive step is safely checkpointed —
		// the worst moment for it to happen, which is why it is the one to test.
		// Only on the first attempt: the recovery has to be allowed to finish.
		if cutOnce.CompareAndSwap(0, 1) {
			link.down.Store(true)
		}
		return map[string]any{"vm": vm}, nil
	})

	eng := engine.New(rs, reg, nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
		engine.Config{
			WorkerID: "cut-1", CancelPollInterval: -1,
			SuspendThreshold: time.Second, LogFlushInterval: 20 * time.Millisecond,
		})

	run, err := a.store.CreateFlowRun(ctx, store.CreateRunInput{
		FlowName: "provision", WorkQueue: "cut", Priority: 50,
		ScheduledAt: time.Now().UTC().Add(-time.Minute),
		Retries:     1, RetryDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	// A short lease so the janitor can reclaim it without the test waiting.
	leased, err := rs.LeaseFlowRuns(ctx, store.LeaseRequest{
		Queues: []string{"cut"}, Max: 1, LeaseFor: time.Second,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: %v (%d)", err, len(leased))
	}
	eng.Execute(ctx, &leased[0])

	// The completion never reached the server: the run is stranded RUNNING with
	// a lease nobody is renewing.
	stranded, err := a.store.GetFlowRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stranded.State == core.StateCompleted {
		t.Fatal("the link was supposed to be cut before the run could settle")
	}
	if built.Load() != 1 {
		t.Fatalf("the expensive step ran %d times on the first attempt", built.Load())
	}

	// The link comes back and the janitor does what it always does.
	link.down.Store(false)
	time.Sleep(1100 * time.Millisecond) // let the one-second lease lapse
	crashed, err := a.store.ReclaimExpiredLeases(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(crashed) != 1 || crashed[0].ID != run.ID {
		t.Fatalf("the janitor reclaimed %d runs, want the stranded one", len(crashed))
	}
	if crashed[0].State != core.StateCrashed {
		t.Fatalf("reclaimed run is %s, want CRASHED", crashed[0].State)
	}

	// CRASHED is not terminal: a crash is not the flow's fault.
	if _, err := a.store.SetFlowRunState(ctx, run.ID,
		core.NewState(core.StateScheduled, "AwaitingRetry", "worker lease expired"),
		store.StateOpts{ScheduleAt: ptrTime(time.Now().UTC().Add(-time.Second))}); err != nil {
		t.Fatal(err)
	}

	// A second worker picks it up and finishes it.
	again, err := rs.LeaseFlowRuns(ctx, store.LeaseRequest{
		Queues: []string{"cut"}, Max: 1, LeaseFor: time.Minute,
	})
	if err != nil || len(again) != 1 {
		t.Fatalf("re-lease after the crash: %v (%d)", err, len(again))
	}
	eng.Execute(ctx, &again[0])

	done, err := a.store.GetFlowRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.State != core.StateCompleted {
		t.Fatalf("state = %s (%s), want COMPLETED", done.State, done.StateMessage)
	}
	if built.Load() != 1 {
		t.Fatalf("the VM was built %d times; the checkpoint did not survive the partition", built.Load())
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

// A site key can be required to present a client certificate, and the worker
// API has to honour that as the External API does — otherwise mutual TLS would
// be a setting an operator believes in and nothing enforces.
func TestWorkerKeyCanRequireMutualTLS(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	secret, keyID := a.issueWorkerKey("mtls-site", []string{"default"})
	k, err := a.store.GetAPIKey(ctx, keyID)
	if err != nil {
		t.Fatal(err)
	}
	k.RequireMTLS = true
	if err := a.store.UpdateAPIKey(ctx, k); err != nil {
		t.Fatal(err)
	}

	rec := a.callKey(http.MethodPost, "/api/v1/worker/heartbeat",
		map[string]any{"name": "w", "queues": []string{"default"}}, secret, "w-1")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("a key requiring mutual TLS was accepted without one: %d %s", rec.Code, rec.Body.String())
	}
	evs, err := a.store.ListAPIKeyEvents(ctx, keyID, 20)
	if err != nil {
		t.Fatal(err)
	}
	var logged bool
	for _, e := range evs {
		if bytes.Contains(e.Detail, []byte("mtls-not-verified")) {
			logged = true
		}
	}
	if !logged {
		t.Fatal("the refusal is not in the key's audit trail")
	}
}

// Per-key rate limiting is what stops one misconfigured site from becoming
// everyone's problem.
func TestWorkerKeyRateLimitApplies(t *testing.T) {
	a := newAPI(t)
	ctx := context.Background()
	secret, keyID := a.issueWorkerKey("chatty-site", []string{"default"})
	k, err := a.store.GetAPIKey(ctx, keyID)
	if err != nil {
		t.Fatal(err)
	}
	limit := 3
	k.RateLimitPerMin = &limit
	if err := a.store.UpdateAPIKey(ctx, k); err != nil {
		t.Fatal(err)
	}

	var limited bool
	for i := 0; i < 10; i++ {
		rec := a.callKey(http.MethodPost, "/api/v1/worker/heartbeat",
			map[string]any{"name": "w", "queues": []string{"default"}}, secret, "w-1")
		if rec.Code == http.StatusTooManyRequests {
			if rec.Header().Get("Retry-After") == "" {
				t.Error("a throttled worker was not told when to come back")
			}
			limited = true
			break
		}
	}
	if !limited {
		t.Fatal("the key's rate limit was never applied")
	}
}
