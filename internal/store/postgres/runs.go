package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

const flowRunCols = `id, name, flow_name, deployment_id, parameters, work_queue,
	state, state_name, state_message, priority, queue_position, scheduled_at, started_at,
	ended_at, run_count, retries, retry_delay_ms, timeout_ms, worker_id, lease_expires_at,
	result, tags, cancel_requested, created_at, updated_at,
	parent_run_id, parent_task_key, trace_context`

func scanFlowRun(sc interface{ Scan(...any) error }) (*core.FlowRun, error) {
	var r core.FlowRun
	var st string
	var retryMs, timeoutMs int64
	if err := sc.Scan(&r.ID, &r.Name, &r.FlowName, &r.DeploymentID, scanJSON(&r.Parameters),
		&r.WorkQueue, &st, &r.StateName, &r.StateMessage, &r.Priority, &r.QueuePosition,
		&r.ScheduledAt, &r.StartedAt, &r.EndedAt, &r.RunCount, &r.Retries, &retryMs, &timeoutMs,
		&r.WorkerID, &r.LeaseExpiresAt, scanJSON(&r.Result), pq.Array(&r.Tags),
		&r.CancelRequest, &r.CreatedAt, &r.UpdatedAt,
		&r.ParentRunID, &r.ParentTaskKey, &r.TraceContext); err != nil {
		return nil, err
	}
	r.State = core.StateType(st)
	r.RetryDelay = durOf(retryMs)
	r.Timeout = durOf(timeoutMs)
	return &r, nil
}

// CreateFlowRun inserts a run in the SCHEDULED state.
//
// When IdempotencyKey is set (the scheduler always sets it to
// "<deployment>:<instant>") a duplicate insert is swallowed and the existing
// run is returned instead, which is what makes schedule materialisation safe
// to run from several replicas.
func (s *Store) CreateFlowRun(ctx context.Context, in store.CreateRunInput) (*core.FlowRun, error) {
	if in.WorkQueue == "" {
		in.WorkQueue = "default"
	}
	if in.ScheduledAt.IsZero() {
		in.ScheduledAt = time.Now().UTC()
	}
	if in.Priority == 0 {
		in.Priority = core.PriorityNormal
	}
	if in.Name == "" {
		in.Name = generateRunName(in.FlowName)
	}
	id := uuid.NewString()

	var idem any
	if in.IdempotencyKey != "" {
		idem = in.IdempotencyKey
	}

	var parentTaskKey any
	if in.ParentTaskKey != "" {
		parentTaskKey = in.ParentTaskKey
	}

	const stmt = `
INSERT INTO pf_flow_runs
  (id, name, flow_name, deployment_id, parameters, work_queue, state, state_name,
   priority, scheduled_at, retries, retry_delay_ms, timeout_ms, tags, idempotency_key,
   parent_run_id, parent_task_key, trace_context)
VALUES ($1,$2,$3,$4,$5,$6,'SCHEDULED','Scheduled',$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING ` + flowRunCols

	row := s.db.QueryRowContext(ctx, stmt, id, in.Name, in.FlowName, in.DeploymentID,
		nullJSON(in.Parameters), in.WorkQueue, in.Priority, in.ScheduledAt.UTC(),
		in.Retries, msOf(in.RetryDelay), msOf(in.Timeout), textArray(in.Tags), idem,
		in.ParentRunID, parentTaskKey, in.TraceContext)

	r, err := scanFlowRun(row)
	if errors.Is(err, sql.ErrNoRows) && in.IdempotencyKey != "" {
		// The run already exists for this schedule instant; return it.
		row := s.db.QueryRowContext(ctx,
			`SELECT `+flowRunCols+` FROM pf_flow_runs WHERE idempotency_key=$1`, in.IdempotencyKey)
		r, err = scanFlowRun(row)
		if err != nil {
			return nil, mapErr(err)
		}
		return r, store.ErrConflict
	}
	if err != nil {
		return nil, mapErr(err)
	}
	return r, nil
}

// GetFlowRun loads one run.
func (s *Store) GetFlowRun(ctx context.Context, id string) (*core.FlowRun, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+flowRunCols+` FROM pf_flow_runs WHERE id=$1`, id)
	r, err := scanFlowRun(row)
	return r, mapErr(err)
}

// ListChildRuns returns every run this run started, oldest first.
func (s *Store) ListChildRuns(ctx context.Context, parentID string) ([]core.FlowRun, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+flowRunCols+` FROM pf_flow_runs WHERE parent_run_id=$1 ORDER BY created_at`, parentID)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.FlowRun{}
	for rows.Next() {
		r, err := scanFlowRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// CountUnfinishedChildren counts children that have not reached a terminal
// state. A CRASHED child counts as unfinished: the janitor will retry it and the
// parent should keep waiting.
func (s *Store) CountUnfinishedChildren(ctx context.Context, parentID string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
SELECT count(*) FROM pf_flow_runs
 WHERE parent_run_id = $1 AND state NOT IN ('COMPLETED','FAILED','CANCELLED')`, parentID).Scan(&n)
	return n, mapErr(err)
}

// AncestorDeploymentIDs walks parent_run_id upward from runID and returns the
// deployment id of each ancestor run, nearest first, stopping at maxDepth. It is
// the input to the sub-flow recursion / depth guard.
func (s *Store) AncestorDeploymentIDs(ctx context.Context, runID string, maxDepth int) ([]string, error) {
	if maxDepth <= 0 {
		maxDepth = 16
	}
	const q = `
WITH RECURSIVE chain AS (
    SELECT id, parent_run_id, deployment_id, 1 AS depth
      FROM pf_flow_runs WHERE id = $1
    UNION ALL
    SELECT p.id, p.parent_run_id, p.deployment_id, c.depth + 1
      FROM pf_flow_runs p JOIN chain c ON p.id = c.parent_run_id
     WHERE c.depth < $2
)
SELECT deployment_id FROM chain WHERE id <> $1 ORDER BY depth`
	rows, err := s.db.QueryContext(ctx, q, runID, maxDepth)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var dep sql.NullString
		if err := rows.Scan(&dep); err != nil {
			return nil, err
		}
		if dep.Valid {
			out = append(out, dep.String)
		} else {
			out = append(out, "")
		}
	}
	return out, rows.Err()
}

// buildFilter renders a FlowRunFilter into a WHERE clause plus arguments.
func buildFilter(f store.FlowRunFilter) (string, []any) {
	var where []string
	var args []any
	add := func(clause string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(clause, len(args)))
	}
	if len(f.States) > 0 {
		ss := make([]string, len(f.States))
		for i, st := range f.States {
			ss[i] = string(st)
		}
		add("state = ANY($%d)", pq.Array(ss))
	}
	if len(f.WorkQueues) > 0 {
		add("work_queue = ANY($%d)", pq.Array(f.WorkQueues))
	}
	if len(f.FlowNames) > 0 {
		add("flow_name = ANY($%d)", pq.Array(f.FlowNames))
	}
	if f.DeploymentID != "" {
		add("deployment_id = $%d", f.DeploymentID)
	}
	if f.Tag != "" {
		add("$%d = ANY(tags)", f.Tag)
	}
	if f.Since != nil {
		add("created_at >= $%d", *f.Since)
	}
	if f.Search != "" {
		add("(name ILIKE '%%' || $%d || '%%' OR id = $%d)", f.Search)
		// The clause above consumes one placeholder twice; rewrite it.
		where[len(where)-1] = fmt.Sprintf("(name ILIKE '%%' || $%d || '%%' OR id = $%d)", len(args), len(args))
	}
	if len(where) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(where, " AND "), args
}

// ListFlowRuns returns runs matching the filter, newest first.
func (s *Store) ListFlowRuns(ctx context.Context, f store.FlowRunFilter) ([]core.FlowRun, error) {
	clause, args := buildFilter(f)
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `SELECT ` + flowRunCols + ` FROM pf_flow_runs` + clause +
		fmt.Sprintf(` ORDER BY created_at DESC LIMIT %d OFFSET %d`, limit, f.Offset)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.FlowRun{}
	for rows.Next() {
		r, err := scanFlowRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// PendingInQueue lists the runs waiting in a queue in dispatch order — the
// same ORDER BY the leasing statement uses, so what an operator sees is what
// will actually happen next.
func (s *Store) PendingInQueue(ctx context.Context, queue string, limit int) ([]core.FlowRun, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT `+flowRunCols+`
  FROM pf_flow_runs
 WHERE work_queue = $1 AND state = 'SCHEDULED' AND NOT cancel_requested
 ORDER BY queue_position ASC NULLS LAST, priority DESC, scheduled_at ASC, created_at ASC
 LIMIT $2`, queue, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.FlowRun{}
	for rows.Next() {
		r, err := scanFlowRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// PushReadyRuns returns runs in a push pool that are ready to dispatch: SCHEDULED,
// their time reached, not cancelled, and not currently held by a lease (a
// previous dispatch whose receiver has not yet claimed it). Same dispatch order
// as PendingInQueue.
func (s *Store) PushReadyRuns(ctx context.Context, pool string, limit int) ([]core.FlowRun, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT `+flowRunCols+`
  FROM pf_flow_runs
 WHERE work_queue = $1 AND state = 'SCHEDULED' AND NOT cancel_requested
   AND scheduled_at <= now()
   AND (worker_id IS NULL OR lease_expires_at IS NULL OR lease_expires_at < now())
 ORDER BY queue_position ASC NULLS LAST, priority DESC, scheduled_at ASC, created_at ASC
 LIMIT $2`, pool, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.FlowRun{}
	for rows.Next() {
		r, err := scanFlowRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// MarkPushDispatched holds a run for leaseFor while its push endpoint is being
// notified, without changing its state. If the receiver never claims it, the
// lease lapses and the run is re-dispatched (and eventually reclaimed as CRASHED
// by the janitor, like any abandoned run).
func (s *Store) MarkPushDispatched(ctx context.Context, runID string, leaseFor time.Duration) error {
	res, err := s.db.ExecContext(ctx, `
UPDATE pf_flow_runs
   SET worker_id = 'push-dispatch', lease_expires_at = now() + $2::interval, updated_at = now()
 WHERE id = $1 AND state = 'SCHEDULED'
   AND (worker_id IS NULL OR lease_expires_at IS NULL OR lease_expires_at < now())`,
		runID, fmt.Sprintf("%d milliseconds", leaseFor.Milliseconds()))
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrConflict // already claimed or no longer scheduled
	}
	return nil
}

// ClearPushDispatch releases a dispatch hold so the run is retried next cycle
// (used when the push POST fails).
func (s *Store) ClearPushDispatch(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE pf_flow_runs SET worker_id = NULL, lease_expires_at = NULL, updated_at = now()
 WHERE id = $1 AND state = 'SCHEDULED' AND worker_id = 'push-dispatch'`, runID)
	return mapErr(err)
}

// ClaimPushRun transitions a specific run SCHEDULED -> PENDING under workerID's
// lease. It mirrors a pull worker's claim: the engine then drives PENDING ->
// RUNNING and owns the flow-run.RUNNING event, so a push run produces exactly
// the same event stream as a leased one. The push receiver calls this before
// handing the run to the engine. Returns ErrConflict if the run is not
// claimable.
func (s *Store) ClaimPushRun(ctx context.Context, runID, workerID string, leaseFor time.Duration) (*core.FlowRun, error) {
	updated, err := s.SetFlowRunState(ctx, runID,
		core.NewState(core.StatePending, "Pending", ""),
		store.StateOpts{WorkerID: &workerID, BumpRun: true})
	if err != nil {
		return nil, err
	}
	if err := s.RenewLease(ctx, runID, workerID, leaseFor); err != nil {
		return updated, err
	}
	return updated, nil
}

// CountFlowRuns counts runs matching the filter.
func (s *Store) CountFlowRuns(ctx context.Context, f store.FlowRunFilter) (int, error) {
	clause, args := buildFilter(f)
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM pf_flow_runs`+clause, args...).Scan(&n)
	return n, mapErr(err)
}

// SetFlowRunState applies the orchestration rule table and writes the new state.
//
// The update is conditional on the state we read, so two workers racing to
// finish the same run cannot both win; the loser gets ErrConflict.
func (s *Store) SetFlowRunState(ctx context.Context, id string, st core.State, opts store.StateOpts) (*core.FlowRun, error) {
	cur, err := s.GetFlowRun(ctx, id)
	if err != nil {
		return nil, err
	}
	// Ownership is checked before anything else: a caller that no longer holds
	// the run has nothing to say about it, not even an idempotent repeat.
	if opts.RequireWorkerID != nil {
		if cur.WorkerID == nil || *cur.WorkerID != *opts.RequireWorkerID {
			return nil, fmt.Errorf("%w: run %s is not held by %s", store.ErrConflict, id, *opts.RequireWorkerID)
		}
	}
	if cur.State == st.Type && st.Type != core.StateScheduled {
		return cur, nil // idempotent no-op
	}
	if !opts.Force && !core.CanTransition(cur.State, st.Type) {
		return nil, core.ErrInvalidTransition{From: cur.State, To: st.Type}
	}

	set := []string{
		"state = $3", "state_name = $4", "state_message = $5", "updated_at = now()",
	}
	args := []any{id, string(cur.State), string(st.Type), st.Name, st.Message}
	add := func(frag string, v any) {
		args = append(args, v)
		set = append(set, fmt.Sprintf(frag, len(args)))
	}

	if opts.WorkerID != nil {
		add("worker_id = $%d", *opts.WorkerID)
	}
	if len(opts.Result) > 0 {
		add("result = $%d", []byte(opts.Result))
	}
	if opts.StartedAt != nil {
		add("started_at = COALESCE(started_at, $%d)", opts.StartedAt.UTC())
	}
	if opts.EndedAt != nil {
		add("ended_at = $%d", opts.EndedAt.UTC())
	}
	if opts.ScheduleAt != nil {
		add("scheduled_at = $%d", opts.ScheduleAt.UTC())
	}
	if opts.BumpRun {
		set = append(set, "run_count = run_count + 1")
	}
	if opts.ClearLease {
		set = append(set, "worker_id = NULL", "lease_expires_at = NULL")
	}
	if st.Type == core.StateScheduled {
		// Re-entering the queue clears any terminal timestamps so the run
		// reads correctly in the UI.
		set = append(set, "ended_at = NULL")
	}

	where := "id = $1 AND state = $2"
	if opts.RequireWorkerID != nil {
		// Re-checked in the write itself, so the lease cannot be reclaimed in
		// the window between the read above and this update.
		args = append(args, *opts.RequireWorkerID)
		where += fmt.Sprintf(" AND worker_id = $%d", len(args))
	}
	q := `UPDATE pf_flow_runs SET ` + strings.Join(set, ", ") +
		` WHERE ` + where + ` RETURNING ` + flowRunCols
	row := s.db.QueryRowContext(ctx, q, args...)
	r, err := scanFlowRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: run %s changed state concurrently", store.ErrConflict, id)
	}
	return r, mapErr(err)
}

// LeaseFlowRuns is the dispatcher.
//
// For each queue the worker polls, it claims up to the remaining capacity.
// Ordering is:
//
//  1. queue_position ascending, NULLs last  — operator pins jump the line
//  2. priority descending                   — urgent work before background work
//  3. scheduled_at ascending                — FIFO within a priority band
//
// SKIP LOCKED means concurrent workers never block each other and never claim
// the same run.
func (s *Store) LeaseFlowRuns(ctx context.Context, req store.LeaseRequest) ([]core.FlowRun, error) {
	if req.Max <= 0 || len(req.Queues) == 0 {
		return nil, nil
	}
	if req.LeaseFor <= 0 {
		req.LeaseFor = 60 * time.Second
	}
	lease := fmt.Sprintf("%d milliseconds", req.LeaseFor.Milliseconds())

	var out []core.FlowRun
	remaining := req.Max
	for _, qn := range req.Queues {
		if remaining <= 0 {
			break
		}
		// One transaction per lane, committed before the next one opens. A
		// worker watching several lanes must never hold two dispatch locks at
		// once: two workers whose PRIMEFLOW_QUEUES lists are in different orders
		// would then wait on each other.
		got, err := s.leaseFromQueue(ctx, req.WorkerID, qn, remaining, lease)
		if err != nil {
			return out, err
		}
		out = append(out, got...)
		remaining -= len(got)
	}
	return out, nil
}

// leaseFromQueue claims from one lane, inside one transaction.
//
// A lane that carries a concurrency limit is serialised with a
// transaction-scoped advisory lock, and that lock is what makes the limit
// exact. Under READ COMMITTED every statement takes its own snapshot, so two
// dispatchers running at the same instant both count the same "active" total,
// both see the full headroom, and both admit up to it — a lane capped at two
// admits two per dispatcher, not two in total. Taking the lock in an earlier
// statement of the same transaction forces the second dispatcher to count in a
// statement that begins after the first one committed, so its snapshot already
// contains those PENDING rows.
//
// An uncapped lane takes no lock at all. There is nothing to serialise, and it
// keeps the fully parallel behaviour SKIP LOCKED gives it today.
//
// Lock ordering is total and cannot cycle: the advisory lock is always taken
// first, the row locks that follow use SKIP LOCKED and never wait, and no
// transaction ever holds more than one advisory lock.
func (s *Store) leaseFromQueue(ctx context.Context, workerID, queue string, max int, lease string) ([]core.FlowRun, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, mapErr(err)
	}
	defer func() { _ = tx.Rollback() }() // no-op once Commit has run

	var paused bool
	var limit sql.NullInt64
	err = tx.QueryRowContext(ctx,
		`SELECT paused, concurrency_limit FROM pf_work_queues WHERE name = $1`, queue).
		Scan(&paused, &limit)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil // a lane nobody has declared holds no work
	case err != nil:
		return nil, mapErr(err)
	case paused:
		return nil, nil // stop the lane, keep the work
	}

	if limit.Valid {
		if _, err := tx.ExecContext(ctx,
			`SELECT pg_advisory_xact_lock(hashtext('pf_dispatch:' || $1)::bigint)`, queue); err != nil {
			return nil, mapErr(err)
		}
	}

	rows, err := tx.QueryContext(ctx, dispatchStmt, workerID, queue, max, lease)
	if err != nil {
		return nil, mapErr(err)
	}
	var out []core.FlowRun
	for rows.Next() {
		r, err := scanFlowRun(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, *r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	if err := tx.Commit(); err != nil {
		return nil, mapErr(err)
	}
	return out, nil
}

// dispatchStmt claims ready runs from one lane. The concurrency headroom is
// still computed here; leaseFromQueue is what guarantees only one dispatcher
// per capped lane evaluates it at a time.
var dispatchStmt = `
WITH cap AS (
    SELECT q.paused,
           q.concurrency_limit,
           (SELECT count(*) FROM pf_flow_runs a
             WHERE a.work_queue = q.name AND a.state IN ('RUNNING','PENDING')) AS active
      FROM pf_work_queues q
     WHERE q.name = $2
),
cand AS (
    SELECT r.id
      FROM pf_flow_runs r
     WHERE r.work_queue = $2
       AND r.state = 'SCHEDULED'
       AND r.scheduled_at <= now()
       AND NOT r.cancel_requested
       AND NOT (SELECT paused FROM cap)
     ORDER BY r.queue_position ASC NULLS LAST, r.priority DESC, r.scheduled_at ASC, r.created_at ASC
     LIMIT (SELECT GREATEST(LEAST($3::int,
                CASE WHEN concurrency_limit IS NULL THEN $3::int
                     ELSE concurrency_limit - active END), 0) FROM cap)
       FOR UPDATE OF r SKIP LOCKED
)
UPDATE pf_flow_runs r
   SET state            = 'PENDING',
       state_name       = 'Pending',
       state_message    = '',
       worker_id        = $1,
       lease_expires_at = now() + $4::interval,
       run_count        = r.run_count + 1,
       started_at       = COALESCE(r.started_at, now()),
       updated_at       = now()
  FROM cand
 WHERE r.id = cand.id
RETURNING ` + prefixCols("r.", flowRunCols)

// prefixCols qualifies a comma-separated column list with a table alias.
func prefixCols(prefix, cols string) string {
	parts := strings.Split(cols, ",")
	for i := range parts {
		parts[i] = prefix + strings.TrimSpace(parts[i])
	}
	return strings.Join(parts, ", ")
}

// RenewLease extends a worker's claim. It fails if the worker no longer owns
// the run, which is how a worker learns it was reclaimed after a network split.
// RenewLeases is the batched form: one statement renews everything this worker
// still holds and, from the same rows, says which of them an operator has asked
// to stop. Cancellation rides back on the heartbeat because it is the same
// question — a worker that can still renew a lease is a worker that can still
// be told to stop.
func (s *Store) RenewLeases(ctx context.Context, workerID string, runIDs []string, d time.Duration) ([]string, []string, error) {
	if len(runIDs) == 0 {
		return nil, nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
UPDATE pf_flow_runs
   SET lease_expires_at = now() + $3::interval, updated_at = now()
 WHERE id = ANY($1) AND worker_id = $2 AND state IN ('RUNNING','PENDING','CANCELLING')
RETURNING id, (cancel_requested OR state IN ('CANCELLING','CANCELLED'))`,
		pq.Array(runIDs), workerID, fmt.Sprintf("%d milliseconds", d.Milliseconds()))
	if err != nil {
		return nil, nil, mapErr(err)
	}
	defer rows.Close()
	var renewed, cancelling []string
	for rows.Next() {
		var id string
		var stop bool
		if err := rows.Scan(&id, &stop); err != nil {
			return nil, nil, err
		}
		renewed = append(renewed, id)
		if stop {
			cancelling = append(cancelling, id)
		}
	}
	return renewed, cancelling, rows.Err()
}

func (s *Store) RenewLease(ctx context.Context, runID, workerID string, d time.Duration) error {
	res, err := s.db.ExecContext(ctx, `
UPDATE pf_flow_runs
   SET lease_expires_at = now() + $3::interval, updated_at = now()
 WHERE id = $1 AND worker_id = $2 AND state IN ('RUNNING','PENDING','CANCELLING')`,
		runID, workerID, fmt.Sprintf("%d milliseconds", d.Milliseconds()))
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("%w: lease for run %s is no longer held by %s", store.ErrConflict, runID, workerID)
	}
	return nil
}

// ReclaimExpiredLeases marks runs whose worker went silent as CRASHED and
// returns them so the janitor can decide whether to retry.
func (s *Store) ReclaimExpiredLeases(ctx context.Context, now time.Time) ([]core.FlowRun, error) {
	stmt := `
UPDATE pf_flow_runs r
   SET state = 'CRASHED',
       state_name = 'Crashed',
       state_message = 'worker lease expired',
       worker_id = NULL,
       lease_expires_at = NULL,
       ended_at = now(),
       updated_at = now()
 WHERE r.state IN ('RUNNING','PENDING','CANCELLING')
   AND r.lease_expires_at IS NOT NULL
   AND r.lease_expires_at < $1
RETURNING ` + prefixCols("r.", flowRunCols)
	rows, err := s.db.QueryContext(ctx, stmt, now.UTC())
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []core.FlowRun
	for rows.Next() {
		r, err := scanFlowRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// ---------------------------------------------- operator queue controls ---

func (s *Store) updateRun(ctx context.Context, id, frag string, args ...any) (*core.FlowRun, error) {
	all := append([]any{id}, args...)
	q := `UPDATE pf_flow_runs SET ` + frag + `, updated_at = now() WHERE id = $1 RETURNING ` + flowRunCols
	row := s.db.QueryRowContext(ctx, q, all...)
	r, err := scanFlowRun(row)
	return r, mapErr(err)
}

// SetRunPriority changes dispatch weight. Takes effect on the next lease, so it
// reorders waiting work without disturbing anything already running.
func (s *Store) SetRunPriority(ctx context.Context, runID string, priority int) (*core.FlowRun, error) {
	if priority < core.PriorityMin {
		priority = core.PriorityMin
	}
	if priority > core.PriorityMax {
		priority = core.PriorityMax
	}
	return s.updateRun(ctx, runID, "priority = $2", priority)
}

// MoveRunToFront pins a run ahead of every other waiting run in its queue,
// regardless of priority. This is the "run this one next" button.
func (s *Store) MoveRunToFront(ctx context.Context, runID string) (*core.FlowRun, error) {
	stmt := `
UPDATE pf_flow_runs r
   SET queue_position = COALESCE(
         (SELECT MIN(x.queue_position) - 1 FROM pf_flow_runs x
           WHERE x.work_queue = r.work_queue AND x.queue_position IS NOT NULL), 0),
       updated_at = now()
 WHERE r.id = $1
RETURNING ` + prefixCols("r.", flowRunCols)
	row := s.db.QueryRowContext(ctx, stmt, runID)
	r, err := scanFlowRun(row)
	return r, mapErr(err)
}

// MoveRunToBack pins a run behind every other pinned run and, because pinned
// runs sort before unpinned ones, is combined with a priority drop to make the
// intent unambiguous.
func (s *Store) MoveRunToBack(ctx context.Context, runID string) (*core.FlowRun, error) {
	return s.updateRun(ctx, runID,
		"queue_position = NULL, priority = GREATEST(priority - 25, 0), scheduled_at = now()")
}

// ClearRunPin removes a manual pin, returning the run to priority ordering.
func (s *Store) ClearRunPin(ctx context.Context, runID string) (*core.FlowRun, error) {
	return s.updateRun(ctx, runID, "queue_position = NULL")
}

// RequestCancel flags a run for cancellation. A SCHEDULED run is cancelled
// immediately; a RUNNING one moves to CANCELLING and the worker's next
// checkpoint observes the flag and unwinds.
func (s *Store) RequestCancel(ctx context.Context, runID string) (*core.FlowRun, error) {
	const stmt = `
UPDATE pf_flow_runs
   SET cancel_requested = true,
       state = CASE WHEN state = 'SCHEDULED' THEN 'CANCELLED'
                    WHEN state IN ('RUNNING','PENDING') THEN 'CANCELLING'
                    ELSE state END,
       state_name = CASE WHEN state = 'SCHEDULED' THEN 'Cancelled'
                         WHEN state IN ('RUNNING','PENDING') THEN 'Cancelling'
                         ELSE state_name END,
       state_message = CASE WHEN state IN ('SCHEDULED','RUNNING','PENDING')
                            THEN 'cancelled by operator' ELSE state_message END,
       ended_at = CASE WHEN state = 'SCHEDULED' THEN now() ELSE ended_at END,
       updated_at = now()
 WHERE id = $1
RETURNING ` + flowRunCols
	row := s.db.QueryRowContext(ctx, stmt, runID)
	r, err := scanFlowRun(row)
	return r, mapErr(err)
}

// RescheduleRun puts a finished or waiting run back on the queue at a chosen
// time. Completed task checkpoints are preserved, so a retried run resumes
// rather than repeating expensive work.
func (s *Store) RescheduleRun(ctx context.Context, runID string, at time.Time) (*core.FlowRun, error) {
	const stmt = `
UPDATE pf_flow_runs
   SET state = 'SCHEDULED', state_name = 'Scheduled', state_message = '',
       scheduled_at = $2, ended_at = NULL, worker_id = NULL, lease_expires_at = NULL,
       cancel_requested = false, updated_at = now()
 WHERE id = $1
RETURNING ` + flowRunCols
	row := s.db.QueryRowContext(ctx, stmt, runID, at.UTC())
	r, err := scanFlowRun(row)
	return r, mapErr(err)
}

// ResumeSuspendedRun brings a run out of a durable wait, but ONLY if it is still
// SCHEDULED (i.e. actually suspended). A parent that is momentarily RUNNING — a
// child finished before the parent reached its wait — is left alone, so its
// lease is never cleared out from under an executing worker. Returns
// store.ErrNotFound when the run was not in a resumable state.
func (s *Store) ResumeSuspendedRun(ctx context.Context, runID string, at time.Time) (*core.FlowRun, error) {
	const stmt = `
UPDATE pf_flow_runs
   SET scheduled_at = $2, updated_at = now()
 WHERE id = $1 AND state = 'SCHEDULED'
RETURNING ` + flowRunCols
	row := s.db.QueryRowContext(ctx, stmt, runID, at.UTC())
	r, err := scanFlowRun(row)
	return r, mapErr(err)
}

// MoveRunToQueue reassigns a waiting run to a different work queue, e.g. to
// drain an overloaded lane onto spare capacity.
func (s *Store) MoveRunToQueue(ctx context.Context, runID, queue string) (*core.FlowRun, error) {
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO pf_work_queues (name) VALUES ($1) ON CONFLICT DO NOTHING`, queue); err != nil {
		return nil, mapErr(err)
	}
	return s.updateRun(ctx, runID, "work_queue = $2, queue_position = NULL", queue)
}

// generateRunName produces a short readable name like "provision-vm-3f2a".
func generateRunName(flowName string) string {
	if flowName == "" {
		flowName = "run"
	}
	return fmt.Sprintf("%s-%s", flowName, uuid.NewString()[:4])
}

var _ = json.Marshal
