package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

// --------------------------------------------------------- task runs ------

const taskRunCols = `id, flow_run_id, task_key, task_name, state, state_name, state_message,
	result, run_count, retries, cache_key, cache_expires_at, started_at, ended_at,
	created_at, updated_at`

func scanTaskRun(sc interface{ Scan(...any) error }) (*core.TaskRun, error) {
	var t core.TaskRun
	var st string
	if err := sc.Scan(&t.ID, &t.FlowRunID, &t.TaskKey, &t.TaskName, &st, &t.StateName,
		&t.StateMessage, scanJSON(&t.Result), &t.RunCount, &t.Retries, &t.CacheKey,
		&t.CacheUntil, &t.StartedAt, &t.EndedAt, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return nil, err
	}
	t.State = core.StateType(st)
	return &t, nil
}

// GetTaskRun looks up a checkpoint by its deterministic key. This is the read
// that makes durable resume work: on a re-run the engine asks for each task key
// before executing anything.
func (s *Store) GetTaskRun(ctx context.Context, flowRunID, taskKey string) (*core.TaskRun, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+taskRunCols+` FROM pf_task_runs WHERE flow_run_id=$1 AND task_key=$2`,
		flowRunID, taskKey)
	t, err := scanTaskRun(row)
	return t, mapErr(err)
}

// ListTaskRuns returns every checkpoint of a flow run in creation order, which
// is what the run timeline renders.
func (s *Store) ListTaskRuns(ctx context.Context, flowRunID string) ([]core.TaskRun, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+taskRunCols+` FROM pf_task_runs WHERE flow_run_id=$1 ORDER BY created_at, id`, flowRunID)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.TaskRun{}
	for rows.Next() {
		t, err := scanTaskRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *t)
	}
	return out, rows.Err()
}

// UpsertTaskRun writes a checkpoint, keyed on (flow_run_id, task_key).
func (s *Store) UpsertTaskRun(ctx context.Context, t *core.TaskRun) error {
	const stmt = `
INSERT INTO pf_task_runs
  (id, flow_run_id, task_key, task_name, state, state_name, state_message, result,
   run_count, retries, cache_key, cache_expires_at, started_at, ended_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (flow_run_id, task_key) DO UPDATE SET
   state            = EXCLUDED.state,
   state_name       = EXCLUDED.state_name,
   state_message    = EXCLUDED.state_message,
   result           = EXCLUDED.result,
   run_count        = EXCLUDED.run_count,
   retries          = EXCLUDED.retries,
   cache_key        = EXCLUDED.cache_key,
   cache_expires_at = EXCLUDED.cache_expires_at,
   started_at       = COALESCE(pf_task_runs.started_at, EXCLUDED.started_at),
   ended_at         = EXCLUDED.ended_at,
   updated_at       = now()
RETURNING id, created_at, updated_at`
	return mapErr(s.db.QueryRowContext(ctx, stmt, t.ID, t.FlowRunID, t.TaskKey, t.TaskName,
		string(t.State), t.StateName, t.StateMessage, nullJSON(t.Result), t.RunCount, t.Retries,
		t.CacheKey, t.CacheUntil, t.StartedAt, t.EndedAt).
		Scan(&t.ID, &t.CreatedAt, &t.UpdatedAt))
}

// FindCachedResult looks for a completed task result under a cross-run cache
// key that has not expired. This implements Prefect-style result caching: two
// different flow runs computing the same thing share one execution.
func (s *Store) FindCachedResult(ctx context.Context, cacheKey string, now time.Time) (json.RawMessage, bool, error) {
	const stmt = `
SELECT result FROM pf_task_runs
 WHERE cache_key = $1
   AND state = 'COMPLETED'
   AND (cache_expires_at IS NULL OR cache_expires_at > $2)
 ORDER BY updated_at DESC LIMIT 1`
	var raw json.RawMessage
	err := s.db.QueryRowContext(ctx, stmt, cacheKey, now.UTC()).Scan(scanJSON(&raw))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, mapErr(err)
	}
	return raw, true, nil
}

// -------------------------------------------------------------- logs ------

// AppendLogs writes a batch of log lines. Workers buffer and flush, so this is
// the hot write path; it uses a single multi-row INSERT.
func (s *Store) AppendLogs(ctx context.Context, recs []core.LogRecord) error {
	if len(recs) == 0 {
		return nil
	}
	var sb strings.Builder
	sb.WriteString(`INSERT INTO pf_logs (flow_run_id, task_run_id, level, message, fields, ts) VALUES `)
	args := make([]any, 0, len(recs)*6)
	for i, r := range recs {
		if i > 0 {
			sb.WriteByte(',')
		}
		b := len(args)
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d)", b+1, b+2, b+3, b+4, b+5, b+6)
		var fields any
		if len(r.Fields) > 0 {
			fields = r.Fields
		}
		ts := r.Timestamp
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		args = append(args, r.FlowRunID, r.TaskRunID, r.Level, r.Message, fields, ts.UTC())
	}
	_, err := s.db.ExecContext(ctx, sb.String(), args...)
	return mapErr(err)
}

// ListLogs streams a run's logs, paging by monotonically increasing id so the
// UI can tail without duplicates.
func (s *Store) ListLogs(ctx context.Context, flowRunID string, afterID int64, limit int) ([]core.LogRecord, error) {
	if limit <= 0 || limit > 2000 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, flow_run_id, task_run_id, level, message, fields, ts
  FROM pf_logs WHERE flow_run_id=$1 AND id > $2 ORDER BY id LIMIT $3`,
		flowRunID, afterID, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.LogRecord{}
	for rows.Next() {
		var r core.LogRecord
		var fields []byte
		if err := rows.Scan(&r.ID, &r.FlowRunID, &r.TaskRunID, &r.Level, &r.Message, &fields, &r.Timestamp); err != nil {
			return nil, err
		}
		r.Fields = fields
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------- artifacts -----

// CreateArtifact stores a human-facing output of a run.
func (s *Store) CreateArtifact(ctx context.Context, a *core.Artifact) error {
	const stmt = `
INSERT INTO pf_artifacts (id, flow_run_id, task_run_id, key, kind, description, data)
VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING created_at`
	return mapErr(s.db.QueryRowContext(ctx, stmt, a.ID, a.FlowRunID, a.TaskRunID, a.Key,
		string(a.Kind), a.Description, []byte(a.Data)).Scan(&a.CreatedAt))
}

// ListArtifacts returns a run's artifacts oldest first.
func (s *Store) ListArtifacts(ctx context.Context, flowRunID string) ([]core.Artifact, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, flow_run_id, task_run_id, key, kind, description, data, created_at
  FROM pf_artifacts WHERE flow_run_id=$1 ORDER BY created_at`, flowRunID)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.Artifact{}
	for rows.Next() {
		var a core.Artifact
		var kind string
		if err := rows.Scan(&a.ID, &a.FlowRunID, &a.TaskRunID, &a.Key, &kind,
			&a.Description, scanJSON(&a.Data), &a.CreatedAt); err != nil {
			return nil, err
		}
		a.Kind = core.ArtifactKind(kind)
		out = append(out, a)
	}
	return out, rows.Err()
}

// ------------------------------------------------------------- events -----

// AppendEvent records an immutable event.
func (s *Store) AppendEvent(ctx context.Context, e *core.Event) error {
	const stmt = `
INSERT INTO pf_events (id, event, resource_type, resource_id, payload, occurred)
VALUES ($1,$2,$3,$4,$5,COALESCE($6, now())) RETURNING occurred, seq`
	var occ any
	if !e.Occurred.IsZero() {
		occ = e.Occurred.UTC()
	}
	return mapErr(s.db.QueryRowContext(ctx, stmt, e.ID, e.Name, e.ResourceType,
		e.ResourceID, nullJSON(e.Payload), occ).Scan(&e.Occurred, &e.Seq))
}

// ListEvents returns the newest events for the activity feed.
func (s *Store) ListEvents(ctx context.Context, limit int) ([]core.Event, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, seq, event, resource_type, resource_id, payload, occurred
  FROM pf_events ORDER BY seq DESC LIMIT $1`, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.Event{}
	for rows.Next() {
		var e core.Event
		if err := rows.Scan(&e.ID, &e.Seq, &e.Name, &e.ResourceType, &e.ResourceID,
			scanJSON(&e.Payload), &e.Occurred); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ListEventsAfter reads events with seq greater than afterSeq, oldest first.
func (s *Store) ListEventsAfter(ctx context.Context, afterSeq int64, limit int) ([]core.Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, seq, event, resource_type, resource_id, payload, occurred
  FROM pf_events WHERE seq > $1 ORDER BY seq ASC LIMIT $2`, afterSeq, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.Event{}
	for rows.Next() {
		var e core.Event
		if err := rows.Scan(&e.ID, &e.Seq, &e.Name, &e.ResourceType, &e.ResourceID,
			scanJSON(&e.Payload), &e.Occurred); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MaxEventSeq returns the newest event cursor, so a starting evaluator can skip
// history instead of replaying every past event as if it just happened.
func (s *Store) MaxEventSeq(ctx context.Context) (int64, error) {
	var n sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT max(seq) FROM pf_events`).Scan(&n); err != nil {
		return 0, mapErr(err)
	}
	return n.Int64, nil
}

// CountEvents supports threshold triggers ("3 failures in 10 minutes").
func (s *Store) CountEvents(ctx context.Context, name, resourceType string, since time.Time) (int, error) {
	var (
		n    int
		err  error
		args = []any{since.UTC()}
		w    = []string{"occurred >= $1"}
	)
	if name != "" {
		args = append(args, name)
		if strings.HasSuffix(name, "*") {
			args[len(args)-1] = strings.TrimSuffix(name, "*") + "%"
			w = append(w, fmt.Sprintf("event LIKE $%d", len(args)))
		} else {
			w = append(w, fmt.Sprintf("event = $%d", len(args)))
		}
	}
	if resourceType != "" {
		args = append(args, resourceType)
		w = append(w, fmt.Sprintf("resource_type = $%d", len(args)))
	}
	err = s.db.QueryRowContext(ctx,
		`SELECT count(*) FROM pf_events WHERE `+strings.Join(w, " AND "), args...).Scan(&n)
	return n, mapErr(err)
}

// --------------------------------------------------------- automations ----

const automationCols = `id, name, description, enabled, match_event, match_deployment,
	match_flow, match_work_queue, match_tag, threshold, window_ms, action, action_config,
	last_fired_at, created_at, updated_at`

// UpsertAutomation creates or updates a rule by name.
func (s *Store) UpsertAutomation(ctx context.Context, a *core.Automation) error {
	if a.Threshold <= 0 {
		a.Threshold = 1
	}
	const stmt = `
INSERT INTO pf_automations
  (id, name, description, enabled, match_event, match_deployment, match_flow,
   match_work_queue, match_tag, threshold, window_ms, action, action_config)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT (name) DO UPDATE SET
   description      = EXCLUDED.description,
   enabled          = EXCLUDED.enabled,
   match_event      = EXCLUDED.match_event,
   match_deployment = EXCLUDED.match_deployment,
   match_flow       = EXCLUDED.match_flow,
   match_work_queue = EXCLUDED.match_work_queue,
   match_tag        = EXCLUDED.match_tag,
   threshold        = EXCLUDED.threshold,
   window_ms        = EXCLUDED.window_ms,
   action           = EXCLUDED.action,
   action_config    = EXCLUDED.action_config,
   updated_at       = now()
RETURNING id, created_at, updated_at`
	return mapErr(s.db.QueryRowContext(ctx, stmt, a.ID, a.Name, a.Description, a.Enabled,
		a.MatchEvent, a.MatchDeployment, a.MatchFlow, a.MatchWorkQueue, a.MatchTag,
		a.Threshold, msOf(a.Window), string(a.Action), nullJSON(a.ActionConfig)).
		Scan(&a.ID, &a.CreatedAt, &a.UpdatedAt))
}

// ListAutomations returns every rule.
func (s *Store) ListAutomations(ctx context.Context) ([]core.Automation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+automationCols+` FROM pf_automations ORDER BY name`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.Automation{}
	for rows.Next() {
		var a core.Automation
		var action string
		var windowMs int64
		if err := rows.Scan(&a.ID, &a.Name, &a.Description, &a.Enabled, &a.MatchEvent,
			&a.MatchDeployment, &a.MatchFlow, &a.MatchWorkQueue, &a.MatchTag, &a.Threshold,
			&windowMs, &action, scanJSON(&a.ActionConfig), &a.LastFiredAt,
			&a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		a.Action = core.ActionKind(action)
		a.Window = durOf(windowMs)
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeleteAutomation removes a rule.
func (s *Store) DeleteAutomation(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM pf_automations WHERE id=$1`, id)
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// TouchAutomation records that a rule fired, which also debounces it: the
// evaluator ignores events older than last_fired_at.
func (s *Store) TouchAutomation(ctx context.Context, id string, at time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE pf_automations SET last_fired_at=$2, updated_at=now() WHERE id=$1`, id, at.UTC())
	return mapErr(err)
}

// ------------------------------------------------------------ workers -----

// HeartbeatWorker records worker liveness for the UI and for capacity display.
func (s *Store) HeartbeatWorker(ctx context.Context, w *core.WorkerInfo) error {
	const stmt = `
INSERT INTO pf_workers (id, name, queues, concurrency, active_runs, last_heartbeat, started_at)
VALUES ($1,$2,$3,$4,$5, now(), COALESCE($6, now()))
ON CONFLICT (id) DO UPDATE SET
   name = EXCLUDED.name, queues = EXCLUDED.queues, concurrency = EXCLUDED.concurrency,
   active_runs = EXCLUDED.active_runs, last_heartbeat = now()`
	var started any
	if !w.StartedAt.IsZero() {
		started = w.StartedAt.UTC()
	}
	_, err := s.db.ExecContext(ctx, stmt, w.ID, w.Name, textArray(w.Queues),
		w.Concurrency, w.ActiveRuns, started)
	return mapErr(err)
}

// ListWorkers returns workers seen in the last hour.
func (s *Store) ListWorkers(ctx context.Context) ([]core.WorkerInfo, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, queues, concurrency, active_runs, last_heartbeat, started_at
  FROM pf_workers WHERE last_heartbeat > now() - interval '1 hour'
 ORDER BY name`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.WorkerInfo{}
	for rows.Next() {
		var w core.WorkerInfo
		if err := rows.Scan(&w.ID, &w.Name, pq.Array(&w.Queues), &w.Concurrency,
			&w.ActiveRuns, &w.LastHeartbeat, &w.StartedAt); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// Compile-time assertion that the Postgres store satisfies the full interface.
var _ store.Store = (*Store)(nil)
