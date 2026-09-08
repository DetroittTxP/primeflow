package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/internal/store/postgres"
)

// newStore connects to the test database, applies the schema and truncates the
// tables so each test starts from a known state.
func newStore(t *testing.T) *postgres.Store {
	t.Helper()
	dsn := os.Getenv("PRIMEFLOW_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set PRIMEFLOW_TEST_DATABASE_URL to run store integration tests")
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
         pf_worker_specs RESTART IDENTITY CASCADE;
DELETE FROM pf_work_queues WHERE name <> 'default';
UPDATE pf_work_queues SET paused = false, concurrency_limit = NULL, push_endpoint = '', push_secret = '';`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	// A prior test may have dropped the legacy pf_logs_p0 partition (which covers
	// the recent past). Guarantee a wide window so tests can write logs at
	// now±months; overlaps with surviving partitions are ignored.
	if _, err := st.DB().ExecContext(ctx, `
DO $$
DECLARE lo date;
BEGIN
  FOR i IN -3..3 LOOP
    lo := (date_trunc('month', now()) + make_interval(months => i))::date;
    BEGIN
      EXECUTE format('CREATE TABLE IF NOT EXISTS pf_logs_%s PARTITION OF pf_logs FOR VALUES FROM (%L) TO (%L)',
        to_char(lo,'YYYY_MM'), lo, (lo + interval '1 month')::date);
    EXCEPTION WHEN others THEN NULL; -- overlaps an existing partition
    END;
  END LOOP;
END $$;`); err != nil {
		t.Fatalf("ensure log partitions: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func mkRun(t *testing.T, st *postgres.Store, queue string, priority int, at time.Time) *core.FlowRun {
	t.Helper()
	r, err := st.CreateFlowRun(context.Background(), store.CreateRunInput{
		FlowName: "demo", WorkQueue: queue, Priority: priority, ScheduledAt: at,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	return r
}

// TestLeaseOrdersByPriorityThenAge is the guarantee the whole admin story rests
// on: high priority first, FIFO inside a band.
func TestLeaseOrdersByPriorityThenAge(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Hour)

	oldNormal := mkRun(t, st, "default", 50, base)
	newNormal := mkRun(t, st, "default", 50, base.Add(time.Minute))
	urgent := mkRun(t, st, "default", 100, base.Add(2*time.Minute))
	background := mkRun(t, st, "default", 10, base.Add(-time.Minute))

	got, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w1", Queues: []string{"default"}, Max: 10, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{urgent.ID, oldNormal.ID, newNormal.ID, background.ID}
	if len(got) != len(want) {
		t.Fatalf("leased %d runs, want %d", len(got), len(want))
	}
	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("position %d: got %s (priority %d), want %s",
				i, got[i].Name, got[i].Priority, id)
		}
	}
	if got[0].State != core.StatePending || got[0].RunCount != 1 {
		t.Fatalf("leased run should be PENDING with run_count 1, got %s / %d",
			got[0].State, got[0].RunCount)
	}
}

// TestMoveToFrontBeatsPriority covers the "run this one next" control.
func TestMoveToFrontBeatsPriority(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Minute)

	urgent := mkRun(t, st, "default", 100, now)
	lowly := mkRun(t, st, "default", 5, now)

	if _, err := st.MoveRunToFront(ctx, lowly.ID); err != nil {
		t.Fatal(err)
	}
	pending, err := st.PendingInQueue(ctx, "default", 10)
	if err != nil {
		t.Fatal(err)
	}
	if pending[0].ID != lowly.ID {
		t.Fatalf("pinned run should be first, got %s", pending[0].Name)
	}

	got, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w1", Queues: []string{"default"}, Max: 1, LeaseFor: time.Minute,
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("lease: %v (%d runs)", err, len(got))
	}
	if got[0].ID != lowly.ID {
		t.Fatalf("expected the pinned run to be dispatched first, got %s", got[0].ID)
	}
	_ = urgent

	// Unpinning restores priority order.
	if _, err := st.ClearRunPin(ctx, lowly.ID); err != nil {
		t.Fatal(err)
	}
}

func TestQueueConcurrencyLimit(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	limit := 2
	if err := st.UpsertWorkQueue(ctx, &core.WorkQueue{Name: "vcd", ConcurrencyLimit: &limit}); err != nil {
		t.Fatal(err)
	}
	past := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < 5; i++ {
		mkRun(t, st, "vcd", 50, past)
	}

	first, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w1", Queues: []string{"vcd"}, Max: 10, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("concurrency limit not enforced: leased %d, want 2", len(first))
	}

	// A second worker gets nothing while the limit is saturated.
	second, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w2", Queues: []string{"vcd"}, Max: 10, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("limit leaked: second worker leased %d", len(second))
	}

	// Finishing one frees exactly one slot.
	ended := time.Now().UTC()
	if _, err := st.SetFlowRunState(ctx, first[0].ID,
		core.NewState(core.StateRunning, "Running", ""), store.StateOpts{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetFlowRunState(ctx, first[0].ID,
		core.NewState(core.StateCompleted, "Completed", ""),
		store.StateOpts{EndedAt: &ended, ClearLease: true}); err != nil {
		t.Fatal(err)
	}
	third, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w2", Queues: []string{"vcd"}, Max: 10, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 1 {
		t.Fatalf("expected exactly one freed slot, leased %d", len(third))
	}
}

// A worker knows only the names of the lanes it polls, so the call it makes on
// start-up must never write a queue definition: an Upsert with a bare name
// would reset the operator's concurrency limit, pause switch and autoscaling
// envelope every time a worker restarted.
func TestEnsureWorkQueuePreservesConfiguration(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	limit, max := 3, 9
	want := &core.WorkQueue{
		Name: "configured-pool", Description: "owned by the math team",
		ConcurrencyLimit: &limit, Paused: true,
		MinWorkers: 2, MaxWorkers: &max, TargetReadyPerWorker: 7,
		Owner: "math-team", PoolType: "pull",
	}
	if err := st.UpsertWorkQueue(ctx, want); err != nil {
		t.Fatal(err)
	}

	// What a worker does when it boots watching this lane.
	if err := st.EnsureWorkQueue(ctx, "configured-pool"); err != nil {
		t.Fatal(err)
	}

	got, err := st.GetWorkQueue(ctx, "configured-pool")
	if err != nil {
		t.Fatal(err)
	}
	if got.ConcurrencyLimit == nil || *got.ConcurrencyLimit != limit {
		t.Errorf("concurrency limit lost: got %v, want %d", got.ConcurrencyLimit, limit)
	}
	if !got.Paused {
		t.Error("pause switch reset: a lane an operator stopped would restart itself")
	}
	if got.MinWorkers != 2 || got.MaxWorkers == nil || *got.MaxWorkers != max {
		t.Errorf("autoscaling envelope lost: min %d, max %v", got.MinWorkers, got.MaxWorkers)
	}
	if got.TargetReadyPerWorker != 7 {
		t.Errorf("target_ready_per_worker lost: got %d, want 7", got.TargetReadyPerWorker)
	}
	if got.Owner != "math-team" || got.Description != "owned by the math team" {
		t.Errorf("attribution lost: owner %q, description %q", got.Owner, got.Description)
	}

	// It still creates a lane that does not exist yet.
	if err := st.EnsureWorkQueue(ctx, "brand-new-pool"); err != nil {
		t.Fatal(err)
	}
	fresh, err := st.GetWorkQueue(ctx, "brand-new-pool")
	if err != nil {
		t.Fatalf("EnsureWorkQueue did not create the lane: %v", err)
	}
	if fresh.Paused || fresh.ConcurrencyLimit != nil {
		t.Errorf("unexpected defaults on a fresh lane: %+v", fresh)
	}
}

// The limit has to hold when several workers dispatch at the same instant, not
// only when they take turns. Before the dispatch lock each of them counted the
// same zero active runs and each admitted the full headroom, so a lane capped
// at two handed out two runs *per worker*.
//
// The goroutines are given a warm connection before the barrier: without that,
// TCP setup staggers them far enough apart that the race does not reproduce.
func TestQueueConcurrencyLimitHoldsUnderConcurrentDispatch(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	const workers, rounds = 8, 6
	limit := 2

	for round := 0; round < rounds; round++ {
		lane := fmt.Sprintf("capped-%d", round)
		if err := st.UpsertWorkQueue(ctx, &core.WorkQueue{Name: lane, ConcurrencyLimit: &limit}); err != nil {
			t.Fatal(err)
		}
		past := time.Now().UTC().Add(-time.Minute)
		for i := 0; i < workers*2; i++ {
			mkRun(t, st, lane, 50, past)
		}

		var (
			wg    sync.WaitGroup
			mu    sync.Mutex
			total int
		)
		ready := make(chan struct{}, workers)
		start := make(chan struct{})
		errs := make(chan error, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				// Warm this goroutine's path through the pool, then wait.
				if _, err := st.GetWorkQueue(ctx, lane); err != nil {
					errs <- err
					ready <- struct{}{}
					return
				}
				ready <- struct{}{}
				<-start
				runs, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
					WorkerID: fmt.Sprintf("w%d", n), Queues: []string{lane},
					Max: 10, LeaseFor: time.Minute,
				})
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				total += len(runs)
				mu.Unlock()
			}(i)
		}
		for i := 0; i < workers; i++ {
			<-ready
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		if total != limit {
			t.Fatalf("round %d: limit leaked under concurrent dispatch: %d runs leased across %d workers, want %d",
				round, total, workers, limit)
		}
	}
}

// The serialisation is only worth anything if it actually holds a dispatcher
// off while another one is mid-dispatch. Taking the lane's dispatch lock by
// hand and watching a lease block on it asserts the mechanism directly, without
// depending on a race reproducing.
func TestCappedLaneDispatchIsSerialised(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	limit := 2
	if err := st.UpsertWorkQueue(ctx, &core.WorkQueue{Name: "held", ConcurrencyLimit: &limit}); err != nil {
		t.Fatal(err)
	}
	past := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < 6; i++ {
		mkRun(t, st, "held", 50, past)
	}

	tx, err := st.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtext('pf_dispatch:' || $1)::bigint)`, "held"); err != nil {
		t.Fatal(err)
	}

	done := make(chan int, 1)
	go func() {
		runs, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
			WorkerID: "blocked", Queues: []string{"held"}, Max: 10, LeaseFor: time.Minute,
		})
		if err != nil {
			done <- -1
			return
		}
		done <- len(runs)
	}()

	select {
	case n := <-done:
		t.Fatalf("dispatch was not serialised: it leased %d runs while the lane's dispatch lock was held", n)
	case <-time.After(400 * time.Millisecond):
		// Correct: it is waiting on the lock.
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-done:
		if n != limit {
			t.Fatalf("after the lock was released the dispatcher leased %d, want %d", n, limit)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch never completed after the lock was released")
	}
}

// Serialising a capped lane must not reintroduce the deadlock the design has
// always been free of. Two capped lanes, two workers polling them in opposite
// order: holding both dispatch locks in one transaction would wedge here, and
// Postgres would report a deadlock rather than hang.
func TestDispatchAcrossLanesDoesNotDeadlock(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	limit := 2
	for _, n := range []string{"lane-a", "lane-b"} {
		if err := st.UpsertWorkQueue(ctx, &core.WorkQueue{Name: n, ConcurrencyLimit: &limit}); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < 10; i++ {
		mkRun(t, st, "lane-a", 50, past)
		mkRun(t, st, "lane-b", 50, past)
	}

	orders := [][]string{{"lane-a", "lane-b"}, {"lane-b", "lane-a"}}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for round := 0; round < 8; round++ {
		for i, queues := range orders {
			wg.Add(1)
			go func(id string, qs []string) {
				defer wg.Done()
				if _, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
					WorkerID: id, Queues: qs, Max: 4, LeaseFor: time.Minute,
				}); err != nil {
					errs <- err
				}
			}(fmt.Sprintf("w%d-%d", round, i), queues)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("dispatch deadlocked or errored: %v", err)
	}
}

func TestPausedQueueDispatchesNothing(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	if err := st.UpsertWorkQueue(ctx, &core.WorkQueue{Name: "paused-lane", Paused: true}); err != nil {
		t.Fatal(err)
	}
	mkRun(t, st, "paused-lane", 100, time.Now().UTC().Add(-time.Minute))

	got, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w1", Queues: []string{"paused-lane"}, Max: 5, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("paused queue dispatched %d runs", len(got))
	}

	if err := st.SetQueuePaused(ctx, "paused-lane", false); err != nil {
		t.Fatal(err)
	}
	got, err = st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w1", Queues: []string{"paused-lane"}, Max: 5, LeaseFor: time.Minute,
	})
	if err != nil || len(got) != 1 {
		t.Fatalf("resumed queue should dispatch: %d runs, err %v", len(got), err)
	}
}

func TestFutureRunsAreNotDispatched(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	mkRun(t, st, "default", 100, time.Now().UTC().Add(time.Hour))

	got, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w1", Queues: []string{"default"}, Max: 5, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("dispatched a run scheduled in the future")
	}
}

func TestNoDoubleLease(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	past := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < 20; i++ {
		mkRun(t, st, "default", 50, past)
	}

	seen := map[string]string{}
	type res struct {
		worker string
		runs   []core.FlowRun
	}
	ch := make(chan res, 4)
	for w := 0; w < 4; w++ {
		go func(w int) {
			id := fmt.Sprintf("w%d", w)
			runs, _ := st.LeaseFlowRuns(ctx, store.LeaseRequest{
				WorkerID: id, Queues: []string{"default"}, Max: 20, LeaseFor: time.Minute,
			})
			ch <- res{id, runs}
		}(w)
	}
	total := 0
	for i := 0; i < 4; i++ {
		r := <-ch
		for _, run := range r.runs {
			if prev, dup := seen[run.ID]; dup {
				t.Fatalf("run %s leased twice: %s and %s", run.ID, prev, r.worker)
			}
			seen[run.ID] = r.worker
			total++
		}
	}
	if total != 20 {
		t.Fatalf("expected all 20 runs leased exactly once, got %d", total)
	}
}

func TestLeaseExpiryCrashesRun(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	mkRun(t, st, "default", 50, time.Now().UTC().Add(-time.Minute))

	leased, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w1", Queues: []string{"default"}, Max: 1, LeaseFor: time.Millisecond,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	crashed, err := st.ReclaimExpiredLeases(ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(crashed) != 1 || crashed[0].State != core.StateCrashed {
		t.Fatalf("expected one CRASHED run, got %+v", crashed)
	}

	// A renewal from the old worker must now fail.
	if err := st.RenewLease(ctx, leased[0].ID, "w1", time.Minute); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected a conflict renewing a reclaimed lease, got %v", err)
	}
}

func TestStateTransitionRulesAreEnforced(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	r := mkRun(t, st, "default", 50, time.Now().UTC())

	// SCHEDULED -> COMPLETED is not a legal jump.
	_, err := st.SetFlowRunState(ctx, r.ID, core.NewState(core.StateCompleted, "Completed", ""), store.StateOpts{})
	var bad core.ErrInvalidTransition
	if !errors.As(err, &bad) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}

	// ...unless forced, which is reserved for the janitor.
	if _, err := st.SetFlowRunState(ctx, r.ID,
		core.NewState(core.StateCompleted, "Completed", ""), store.StateOpts{Force: true}); err != nil {
		t.Fatalf("forced transition failed: %v", err)
	}
}

func TestCheckpointsSurviveReschedule(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	r := mkRun(t, st, "default", 50, time.Now().UTC())

	tr := &core.TaskRun{
		ID: uuid.NewString(), FlowRunID: r.ID, TaskKey: "create-vm-1", TaskName: "create-vm",
		State: core.StateCompleted, StateName: "Completed", Result: json.RawMessage(`{"id":"vm-1"}`),
	}
	if err := st.UpsertTaskRun(ctx, tr); err != nil {
		t.Fatal(err)
	}

	if _, err := st.RescheduleRun(ctx, r.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetTaskRun(ctx, r.ID, "create-vm-1")
	if err != nil {
		t.Fatalf("checkpoint lost on reschedule: %v", err)
	}
	// jsonb re-serialises, so compare the decoded value rather than the bytes.
	var decoded struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(got.Result, &decoded); err != nil || decoded.ID != "vm-1" {
		t.Fatalf("checkpoint result changed: %s (%v)", got.Result, err)
	}
}

func TestCacheLookupRespectsExpiry(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	r := mkRun(t, st, "default", 50, time.Now().UTC())

	past := time.Now().UTC().Add(-time.Hour)
	key := "org:acme"
	if err := st.UpsertTaskRun(ctx, &core.TaskRun{
		ID: uuid.NewString(), FlowRunID: r.ID, TaskKey: "lookup-1", TaskName: "lookup",
		State: core.StateCompleted, Result: json.RawMessage(`"urn:acme"`),
		CacheKey: &key, CacheUntil: &past,
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.FindCachedResult(ctx, key, time.Now().UTC()); err != nil || ok {
		t.Fatalf("expired cache entry was returned (ok=%v, err=%v)", ok, err)
	}

	future := time.Now().UTC().Add(time.Hour)
	if err := st.UpsertTaskRun(ctx, &core.TaskRun{
		ID: uuid.NewString(), FlowRunID: r.ID, TaskKey: "lookup-1", TaskName: "lookup",
		State: core.StateCompleted, Result: json.RawMessage(`"urn:acme"`),
		CacheKey: &key, CacheUntil: &future,
	}); err != nil {
		t.Fatal(err)
	}
	raw, ok, err := st.FindCachedResult(ctx, key, time.Now().UTC())
	if err != nil || !ok || string(raw) != `"urn:acme"` {
		t.Fatalf("live cache entry missing: %s ok=%v err=%v", raw, ok, err)
	}
}

func TestIdempotentScheduleMaterialisation(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	in := store.CreateRunInput{
		FlowName: "demo", WorkQueue: "default", Priority: 50,
		ScheduledAt: time.Now().UTC(), IdempotencyKey: "dep-1@1757200000",
	}
	first, err := st.CreateFlowRun(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := st.CreateFlowRun(ctx, in)
	if !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expected ErrConflict on duplicate schedule instant, got %v", err)
	}
	if second == nil || second.ID != first.ID {
		t.Fatal("duplicate insert should return the existing run")
	}
}

func TestLeadershipIsExclusive(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	got, err := st.AcquireLeadership(ctx, "scheduler", "replica-a", 5*time.Second)
	if err != nil || !got {
		t.Fatalf("first replica should win: %v %v", got, err)
	}
	got, err = st.AcquireLeadership(ctx, "scheduler", "replica-b", 5*time.Second)
	if err != nil || got {
		t.Fatalf("second replica should lose while the lease is live: %v %v", got, err)
	}
	// The holder renews freely.
	got, err = st.AcquireLeadership(ctx, "scheduler", "replica-a", 5*time.Second)
	if err != nil || !got {
		t.Fatalf("holder should renew: %v %v", got, err)
	}

	// After expiry, another replica takes over.
	if _, err := st.AcquireLeadership(ctx, "scheduler", "replica-a", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	got, err = st.AcquireLeadership(ctx, "scheduler", "replica-b", 5*time.Second)
	if err != nil || !got {
		t.Fatalf("expired leadership should transfer: %v %v", got, err)
	}
}

func TestCancelSemantics(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	// A waiting run cancels outright.
	waiting := mkRun(t, st, "default", 50, time.Now().UTC())
	got, err := st.RequestCancel(ctx, waiting.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != core.StateCancelled {
		t.Fatalf("scheduled run should cancel immediately, got %s", got.State)
	}

	// A running run enters CANCELLING so the worker can unwind.
	running := mkRun(t, st, "default", 50, time.Now().UTC().Add(-time.Minute))
	leased, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w1", Queues: []string{"default"}, Max: 1, LeaseFor: time.Minute,
	})
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: %v", err)
	}
	if _, err := st.SetFlowRunState(ctx, running.ID,
		core.NewState(core.StateRunning, "Running", ""), store.StateOpts{}); err != nil {
		t.Fatal(err)
	}
	got, err = st.RequestCancel(ctx, running.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != core.StateCancelling || !got.CancelRequest {
		t.Fatalf("running run should move to CANCELLING, got %s", got.State)
	}

	// A finished run has nothing left to stop. Accepting the request would
	// leave cancel_requested set on it forever, so every later reader would be
	// told a COMPLETED run had been cancelled.
	done := mkRun(t, st, "default", 50, time.Now().UTC())
	for _, to := range []core.StateType{core.StateRunning, core.StateCompleted} {
		if _, err := st.SetFlowRunState(ctx, done.ID,
			core.NewState(to, string(to), ""), store.StateOpts{}); err != nil {
			t.Fatalf("drive run to %s: %v", to, err)
		}
	}
	_, err = st.RequestCancel(ctx, done.ID)
	var bad core.ErrInvalidTransition
	if !errors.As(err, &bad) {
		t.Fatalf("cancelling a COMPLETED run should be refused, got %v", err)
	}
	if bad.From != core.StateCompleted {
		t.Fatalf("transition error should name the state it refused: %+v", bad)
	}
	after, err := st.GetFlowRun(ctx, done.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != core.StateCompleted || after.CancelRequest {
		t.Fatalf("refused cancel must leave the run untouched, got %s cancel_requested=%v",
			after.State, after.CancelRequest)
	}

	// A missing run is still a 404, not a conflict.
	if _, err := st.RequestCancel(ctx, uuid.NewString()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancelling an unknown run should be ErrNotFound, got %v", err)
	}
}

func TestQueueStats(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	mkRun(t, st, "default", 50, time.Now().UTC().Add(-time.Minute)) // ready
	mkRun(t, st, "default", 50, time.Now().UTC().Add(time.Hour))    // scheduled, not ready

	stats, err := st.QueueStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var d *store.QueueStat
	for i := range stats {
		if stats[i].Name == "default" {
			d = &stats[i]
		}
	}
	if d == nil {
		t.Fatal("default queue missing from stats")
	}
	if d.Scheduled != 2 || d.Ready != 1 {
		t.Fatalf("stats wrong: scheduled=%d ready=%d", d.Scheduled, d.Ready)
	}
}

func TestEventCursor(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := st.AppendEvent(ctx, &core.Event{
			ID: uuid.NewString(), Name: "flow-run.FAILED",
			ResourceType: "flow-run", ResourceID: fmt.Sprint(i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := st.ListEventsAfter(ctx, 0, 10)
	if err != nil || len(batch) != 5 {
		t.Fatalf("expected 5 events, got %d (%v)", len(batch), err)
	}
	// The cursor advances monotonically and never re-delivers.
	next, err := st.ListEventsAfter(ctx, batch[len(batch)-1].Seq, 10)
	if err != nil || len(next) != 0 {
		t.Fatalf("cursor re-delivered events: %d", len(next))
	}
	n, err := st.CountEvents(ctx, "flow-run.*", "flow-run", time.Now().UTC().Add(-time.Minute))
	if err != nil || n != 5 {
		t.Fatalf("wildcard count wrong: %d (%v)", n, err)
	}
}

// leaseOne claims exactly one run from a lane and fails the test otherwise.
func leaseOne(t *testing.T, st *postgres.Store, worker, queue string) *core.FlowRun {
	t.Helper()
	got, err := st.LeaseFlowRuns(context.Background(), store.LeaseRequest{
		WorkerID: worker, Queues: []string{queue}, Max: 1, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatalf("lease: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("leased %d runs, want 1", len(got))
	}
	return &got[0]
}

// TestSuspendDoesNotConsumeAnAttempt pins the contract engine.finish documents:
// a durable wait is not a failed attempt.
//
// The dispatcher increments run_count on every lease, and engine.finish reads
// run_count as the attempt number when it decides whether a failure still has
// retry budget. Without StateOpts.Resume the two are the same event, so a
// RunDeploymentAndWait parent — which re-suspends every 30 seconds until its
// children land — spends its whole retry budget waiting and then fails on its
// first real error with no attempts left.
func TestSuspendDoesNotConsumeAnAttempt(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	if err := st.UpsertWorkQueue(ctx, &core.WorkQueue{Name: "default"}); err != nil {
		t.Fatal(err)
	}
	run := mkRun(t, st, "default", 50, time.Now().UTC().Add(-time.Minute))

	const waits = 5
	for i := 1; i <= waits; i++ {
		leased := leaseOne(t, st, "w1", "default")
		if leased.RunCount != 1 {
			t.Fatalf("wait %d: run_count = %d, want 1 — the run is still on its first attempt", i, leased.RunCount)
		}
		if _, err := st.SetFlowRunState(ctx, run.ID,
			core.NewState(core.StateRunning, "Running", ""), store.StateOpts{}); err != nil {
			t.Fatal(err)
		}
		// engine.finish's suspend branch.
		at := time.Now().UTC().Add(-time.Second)
		if _, err := st.SetFlowRunState(ctx, run.ID,
			core.NewState(core.StateScheduled, "Suspended", "waiting for sub-flow"),
			store.StateOpts{ScheduleAt: &at, ClearLease: true, Resume: true}); err != nil {
			t.Fatal(err)
		}
	}

	// A genuine retry still counts, so the budget check keeps working.
	leased := leaseOne(t, st, "w1", "default")
	if leased.RunCount != 1 {
		t.Fatalf("after %d waits run_count = %d, want 1", waits, leased.RunCount)
	}
	if _, err := st.SetFlowRunState(ctx, run.ID,
		core.NewState(core.StateRunning, "Running", ""), store.StateOpts{}); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Second)
	if _, err := st.SetFlowRunState(ctx, run.ID,
		core.NewState(core.StateScheduled, "AwaitingRetry", "boom"),
		store.StateOpts{ScheduleAt: &at, ClearLease: true}); err != nil {
		t.Fatal(err)
	}
	if leased = leaseOne(t, st, "w1", "default"); leased.RunCount != 2 {
		t.Errorf("after a real failure run_count = %d, want 2", leased.RunCount)
	}
}

// TestResumeFlagIsConsumedByOneLease checks the flag cannot outlive the lease it
// was written for: two suspensions must not buy three free leases.
func TestResumeFlagIsConsumedByOneLease(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	if err := st.UpsertWorkQueue(ctx, &core.WorkQueue{Name: "default"}); err != nil {
		t.Fatal(err)
	}
	run := mkRun(t, st, "default", 50, time.Now().UTC().Add(-time.Minute))

	leaseOne(t, st, "w1", "default") // attempt 1
	if _, err := st.SetFlowRunState(ctx, run.ID,
		core.NewState(core.StateRunning, "Running", ""), store.StateOpts{}); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Second)
	if _, err := st.SetFlowRunState(ctx, run.ID,
		core.NewState(core.StateScheduled, "Suspended", "sleeping"),
		store.StateOpts{ScheduleAt: &at, ClearLease: true, Resume: true}); err != nil {
		t.Fatal(err)
	}

	// The resume itself is free...
	if got := leaseOne(t, st, "w1", "default"); got.RunCount != 1 {
		t.Fatalf("resume lease: run_count = %d, want 1", got.RunCount)
	}
	// ...and the flag is now spent, so a crash-and-requeue costs an attempt.
	if _, err := st.SetFlowRunState(ctx, run.ID,
		core.NewState(core.StateRunning, "Running", ""), store.StateOpts{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SetFlowRunState(ctx, run.ID,
		core.NewState(core.StateScheduled, "AwaitingRetry", "crashed"),
		store.StateOpts{ScheduleAt: &at, ClearLease: true, Force: true}); err != nil {
		t.Fatal(err)
	}
	if got := leaseOne(t, st, "w1", "default"); got.RunCount != 2 {
		t.Errorf("run_count = %d after the resume was spent, want 2", got.RunCount)
	}
}

// TestConcurrencyLimitCountsCancellingRuns is the lane cap under cancellation.
//
// Cancelling a run asks it to wind down; it does not stop it. The run keeps its
// worker and keeps renewing its lease until its own code returns, so it still
// occupies one of the lane's slots. Counting only RUNNING and PENDING let a lane
// capped at two admit two more, putting four flows at once against the endpoint
// the cap exists to protect.
func TestConcurrencyLimitCountsCancellingRuns(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	limit := 2
	if err := st.UpsertWorkQueue(ctx, &core.WorkQueue{Name: "vcd", ConcurrencyLimit: &limit}); err != nil {
		t.Fatal(err)
	}
	past := time.Now().UTC().Add(-time.Minute)
	for i := 0; i < 6; i++ {
		mkRun(t, st, "vcd", 50, past)
	}

	held, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w1", Queues: []string{"vcd"}, Max: 10, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(held) != 2 {
		t.Fatalf("setup: leased %d, want 2", len(held))
	}

	ids := make([]string, 0, len(held))
	for _, r := range held {
		ids = append(ids, r.ID)
		if _, err := st.SetFlowRunState(ctx, r.ID,
			core.NewState(core.StateRunning, "Running", ""), store.StateOpts{}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.SetFlowRunState(ctx, r.ID,
			core.NewState(core.StateCancelling, "Cancelling", "operator asked to stop"),
			store.StateOpts{}); err != nil {
			t.Fatal(err)
		}
	}

	// Both are winding down but still leased: they have not released their slots.
	renewed, _, err := st.RenewLeases(ctx, "w1", ids, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(renewed) != 2 {
		t.Fatalf("cancelling runs stopped renewing (%d/2); this test no longer models a held slot", len(renewed))
	}

	extra, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w2", Queues: []string{"vcd"}, Max: 10, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(extra) != 0 {
		t.Errorf("lane capped at %d admitted %d more while %d cancelling runs held their slots: %d concurrent",
			limit, len(extra), len(renewed), len(renewed)+len(extra))
	}

	// Once they actually land, the slots come back.
	for _, id := range ids {
		ended := time.Now().UTC()
		if _, err := st.SetFlowRunState(ctx, id,
			core.NewState(core.StateCancelled, "Cancelled", ""),
			store.StateOpts{EndedAt: &ended, ClearLease: true}); err != nil {
			t.Fatal(err)
		}
	}
	freed, err := st.LeaseFlowRuns(ctx, store.LeaseRequest{
		WorkerID: "w2", Queues: []string{"vcd"}, Max: 10, LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(freed) != 2 {
		t.Errorf("after the cancelled runs settled the lane leased %d, want %d", len(freed), limit)
	}
}
