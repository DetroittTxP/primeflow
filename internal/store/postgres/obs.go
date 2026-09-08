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

// GetLogRetention reads the pf_settings row keyed "log_retention".
func (s *Store) GetLogRetention(ctx context.Context) (core.LogRetention, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM pf_settings WHERE key='log_retention'`).Scan(&raw)
	if err != nil {
		if mapErr(err) == store.ErrNotFound {
			return core.LogRetention{Enabled: true, MaxAgeHours: 720}, nil
		}
		return core.LogRetention{}, mapErr(err)
	}
	var out core.LogRetention
	_ = json.Unmarshal(raw, &out)
	return out, nil
}

// PutLogRetention overwrites the log-retention setting.
func (s *Store) PutLogRetention(ctx context.Context, in core.LogRetention) error {
	raw, _ := json.Marshal(in)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO pf_settings (key, value, updated_at) VALUES ('log_retention', $1, now())
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, raw)
	return mapErr(err)
}

// DeleteLogsOlderThan removes pf_logs rows written before cutoff. With native
// monthly partitioning most old logs are removed by DropLogPartitionsOlderThan;
// this cleans the sub-partition tail (rows in a still-live partition that are
// already past retention). It deletes in bounded batches keyed on the (id, ts)
// primary key so it works on both the partitioned parent and a plain table, and
// stops after maxRows so one janitor pass cannot monopolise a connection.
func (s *Store) DeleteLogsOlderThan(ctx context.Context, cutoff time.Time, maxRows int) (deleted int, more bool, err error) {
	const batch = 5000
	if maxRows <= 0 {
		maxRows = 100000
	}
	for deleted < maxRows {
		res, e := s.db.ExecContext(ctx, `
WITH doomed AS (
    SELECT id, ts FROM pf_logs WHERE ts < $1 ORDER BY ts LIMIT $2
)
DELETE FROM pf_logs t USING doomed d WHERE t.id = d.id AND t.ts = d.ts`,
			cutoff.UTC(), batch)
		if e != nil {
			return deleted, false, mapErr(e)
		}
		n, _ := res.RowsAffected()
		deleted += int(n)
		if n < batch {
			return deleted, false, nil
		}
		select {
		case <-ctx.Done():
			return deleted, true, ctx.Err()
		default:
		}
	}
	return deleted, true, nil
}

// EnsureLogPartitions guarantees a monthly pf_logs partition exists for every
// month from the current one through monthsAhead months out, so a write never
// hits a gap (e.g. after DropLogPartitionsOlderThan removes the legacy p0). A
// month already covered by another partition is skipped. No-op and cheap when
// pf_logs is not partitioned, so it is safe to call unconditionally.
func (s *Store) EnsureLogPartitions(ctx context.Context, monthsAhead int) error {
	if !s.logsPartitioned(ctx) {
		return nil
	}
	if monthsAhead < 0 {
		monthsAhead = 0
	}
	now := time.Now().UTC()
	lo := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i <= monthsAhead; i++ {
		hi := lo.AddDate(0, 1, 0)
		stmt := fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF pf_logs FOR VALUES FROM ('%s') TO ('%s')`,
			quoteIdent("pf_logs_"+lo.Format("2006_01")),
			lo.Format("2006-01-02"), hi.Format("2006-01-02"))
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			// The month is already covered by another partition (e.g. the legacy
			// p0). That is fine — anything else is a real error.
			if strings.Contains(err.Error(), "would overlap") {
				lo = hi
				continue
			}
			return mapErr(err)
		}
		lo = hi
	}
	return nil
}

// DropLogPartitionsOlderThan drops every pf_logs partition whose entire range is
// before cutoff — an O(1) alternative to deleting rows. It returns the dropped
// partition names. Partitions that only partly precede cutoff are left for
// DeleteLogsOlderThan.
func (s *Store) DropLogPartitionsOlderThan(ctx context.Context, cutoff time.Time) ([]string, error) {
	if !s.logsPartitioned(ctx) {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT c.relname, pg_get_expr(c.relpartbound, c.oid)
  FROM pg_inherits i
  JOIN pg_class c   ON c.oid = i.inhrelid
  JOIN pg_class p   ON p.oid = i.inhparent
 WHERE p.relname = 'pf_logs'`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()

	var doomed []string
	for rows.Next() {
		var name, bound string
		if err := rows.Scan(&name, &bound); err != nil {
			return nil, err
		}
		// bound looks like: FOR VALUES FROM ('MINVALUE') TO ('2026-09-01 00:00:00+00')
		hi := partitionUpperBound(bound)
		if hi.IsZero() || !hi.Before(cutoff.UTC()) {
			continue
		}
		doomed = append(doomed, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, name := range doomed {
		if _, err := s.db.ExecContext(ctx, `DROP TABLE IF EXISTS `+quoteIdent(name)); err != nil {
			return doomed, mapErr(err)
		}
	}
	return doomed, nil
}

func (s *Store) logsPartitioned(ctx context.Context) bool {
	var ok bool
	_ = s.db.QueryRowContext(ctx, `
SELECT EXISTS (
  SELECT 1 FROM pg_partitioned_table pt
  JOIN pg_class c ON c.oid = pt.partrelid WHERE c.relname = 'pf_logs')`).Scan(&ok)
	return ok
}

// partitionUpperBound pulls the TO (...) timestamp out of a relpartbound
// expression. Returns the zero time when the upper bound is MAXVALUE or
// unparseable.
func partitionUpperBound(expr string) time.Time {
	i := strings.LastIndex(expr, "TO (")
	if i < 0 {
		return time.Time{}
	}
	rest := expr[i+4:]
	j := strings.Index(rest, ")")
	if j < 0 {
		return time.Time{}
	}
	v := strings.TrimSpace(rest[:j])
	v = strings.Trim(v, "'")
	if strings.EqualFold(v, "MAXVALUE") {
		return time.Time{}
	}
	for _, layout := range []string{"2006-01-02 15:04:05-07", "2006-01-02 15:04:05Z07:00", "2006-01-02"} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// Stats returns time-bucketed activity for the console Dashboard. It runs three
// date_bin GROUP BY queries (Postgres 14+); on an older server the queries error
// and the caller gets a zero-value Stats, which the dashboard renders as bare.
func (s *Store) Stats(ctx context.Context, window time.Duration, buckets int) (core.Stats, error) {
	if buckets <= 0 {
		buckets = 48
	}
	step := roundStep(window / time.Duration(buckets))
	out := core.Stats{
		WindowSeconds: int(window.Seconds()),
		BucketSeconds: int(step.Seconds()),
	}
	out.FlowRuns.ByState = map[string]int{}

	// The date_bin grid origin, matched on the Go side so bucket keys line up.
	const origin = "TIMESTAMPTZ '2000-01-01 00:00:00+00'"
	binExpr := func(col string) string {
		return "date_bin('" + itoa(int(step.Seconds())) + " seconds', " + col + ", " + origin + ")"
	}
	since := time.Now().Add(-window).UTC()

	// --- flow runs: activity by outcome class ---
	frRows, err := s.db.QueryContext(ctx, `
SELECT `+binExpr(`COALESCE(started_at, scheduled_at, created_at)`)+` AS b,
       count(*) FILTER (WHERE state = 'COMPLETED')            AS completed,
       count(*) FILTER (WHERE state IN ('FAILED','CRASHED'))  AS failed,
       count(*) FILTER (WHERE state NOT IN ('COMPLETED','FAILED','CRASHED')) AS other
  FROM pf_flow_runs
 WHERE COALESCE(started_at, scheduled_at, created_at) >= $1
 GROUP BY b`, since)
	if err != nil {
		return out, mapErr(err)
	}
	frByBin := map[int64][3]int{}
	for frRows.Next() {
		var b time.Time
		var c, f, o int
		if err := frRows.Scan(&b, &c, &f, &o); err != nil {
			frRows.Close()
			return out, err
		}
		frByBin[b.Unix()] = [3]int{c, f, o}
	}
	frRows.Close()
	if err := frRows.Err(); err != nil {
		return out, err
	}

	// by_state + total over the window (by creation time).
	stRows, err := s.db.QueryContext(ctx,
		`SELECT state, count(*) FROM pf_flow_runs WHERE created_at >= $1 GROUP BY state`, since)
	if err != nil {
		return out, mapErr(err)
	}
	for stRows.Next() {
		var st string
		var n int
		if err := stRows.Scan(&st, &n); err != nil {
			stRows.Close()
			return out, err
		}
		out.FlowRuns.ByState[st] = n
		out.FlowRuns.Total += n
	}
	stRows.Close()

	// --- task runs ---
	trRows, err := s.db.QueryContext(ctx, `
SELECT `+binExpr(`COALESCE(ended_at, created_at)`)+` AS b,
       count(*) FILTER (WHERE state = 'COMPLETED')           AS completed,
       count(*) FILTER (WHERE state IN ('FAILED','CRASHED')) AS failed
  FROM pf_task_runs
 WHERE COALESCE(ended_at, created_at) >= $1
 GROUP BY b`, since)
	if err != nil {
		return out, mapErr(err)
	}
	trByBin := map[int64][2]int{}
	for trRows.Next() {
		var b time.Time
		var c, f int
		if err := trRows.Scan(&b, &c, &f); err != nil {
			trRows.Close()
			return out, err
		}
		trByBin[b.Unix()] = [2]int{c, f}
		out.TaskRuns.Completed += c
		out.TaskRuns.Failed += f
		out.TaskRuns.Total += c + f
	}
	trRows.Close()

	// --- events ---
	evRows, err := s.db.QueryContext(ctx, `
SELECT `+binExpr(`occurred`)+` AS b, count(*)
  FROM pf_events WHERE occurred >= $1 GROUP BY b`, since)
	if err != nil {
		return out, mapErr(err)
	}
	evByBin := map[int64]int{}
	for evRows.Next() {
		var b time.Time
		var n int
		if err := evRows.Scan(&b, &n); err != nil {
			evRows.Close()
			return out, err
		}
		evByBin[b.Unix()] = n
		out.Events.Total += n
	}
	evRows.Close()

	// Materialise a contiguous, aligned bucket list.
	first := binStart(since, step)
	for t := first; !t.After(time.Now()); t = t.Add(step) {
		k := t.Unix()
		fr := frByBin[k]
		out.FlowRuns.Buckets = append(out.FlowRuns.Buckets,
			core.FlowRunBucket{T: t, Completed: fr[0], Failed: fr[1], Other: fr[2]})
		tr := trByBin[k]
		out.TaskRuns.Buckets = append(out.TaskRuns.Buckets,
			core.TaskBucket{T: t, Completed: tr[0], Failed: tr[1]})
		out.Events.Buckets = append(out.Events.Buckets, core.CountBucket{T: t, N: evByBin[k]})
	}
	return out, nil
}

// roundStep snaps a raw bucket width to a friendly value so axis labels are sane.
func roundStep(d time.Duration) time.Duration {
	steps := []time.Duration{
		time.Minute, 2 * time.Minute, 5 * time.Minute, 10 * time.Minute, 15 * time.Minute,
		30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour, 6 * time.Hour, 12 * time.Hour,
	}
	for _, s := range steps {
		if d <= s {
			return s
		}
	}
	return 24 * time.Hour
}

// binStart aligns t down to the date_bin grid (origin 2000-01-01 UTC).
func binStart(t time.Time, step time.Duration) time.Time {
	origin := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	n := t.Sub(origin) / step
	return origin.Add(n * step)
}

// itoa avoids importing strconv into this file for one call site.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
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
