// Package postgres implements the PrimeFlow store on PostgreSQL.
//
// Postgres is the source of truth for every piece of state, including queue
// order. Dispatch uses SELECT ... FOR UPDATE SKIP LOCKED so that any number of
// server or worker processes can pull from the same queue without a broker and
// without double-execution.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Store is the PostgreSQL-backed implementation of store.Store.
type Store struct {
	db *sql.DB
}

// Open connects to Postgres and verifies the connection.
func Open(ctx context.Context, dsn string, maxConns int) (*Store, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if maxConns <= 0 {
		maxConns = 20
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{db: db}, nil
}

// DB exposes the underlying handle for advanced use (LISTEN/NOTIFY, tests).
func (s *Store) DB() *sql.DB { return s.db }

// Close releases the connection pool.
func (s *Store) Close() error { return s.db.Close() }

// Migrate applies the embedded schema. Every statement is idempotent, so this
// is safe to run on every process start.
func (s *Store) Migrate(ctx context.Context) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, n := range names {
		b, err := migrationFS.ReadFile("migrations/" + n)
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, string(b)); err != nil {
			return fmt.Errorf("migration %s: %w", n, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------- helpers ---

func msOf(d time.Duration) int64 { return d.Milliseconds() }

func durOf(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

// textArray always writes a non-NULL array: the columns are NOT NULL with a
// '{}' default, and a nil Go slice would otherwise send NULL.
func textArray(v []string) any {
	if v == nil {
		v = []string{}
	}
	return pq.Array(v)
}

func nullJSON(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return []byte(b)
}

func scanJSON(dst *json.RawMessage) any { return (*rawJSON)(dst) }

type rawJSON json.RawMessage

func (r *rawJSON) Scan(v any) error {
	switch t := v.(type) {
	case nil:
		*r = nil
	case []byte:
		*r = append((*r)[:0], t...)
	case string:
		*r = rawJSON(t)
	default:
		return fmt.Errorf("cannot scan %T into json", v)
	}
	return nil
}

func mapErr(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return store.ErrNotFound
	}
	var pqe *pq.Error
	if errors.As(err, &pqe) && pqe.Code.Class() == "23" { // integrity constraint violation
		return fmt.Errorf("%w: %s", store.ErrConflict, pqe.Message)
	}
	return err
}

// ----------------------------------------------------------------- flows ---

// UpsertFlow records or refreshes a flow in the catalogue.
func (s *Store) UpsertFlow(ctx context.Context, f *core.Flow) error {
	labels, _ := json.Marshal(f.Labels)
	if f.Labels == nil {
		labels = []byte("{}")
	}
	const q = `
INSERT INTO pf_flows (id, name, version, description, tags, labels, params_schema)
VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (name, version) DO UPDATE
   SET description   = EXCLUDED.description,
       tags          = EXCLUDED.tags,
       labels        = EXCLUDED.labels,
       params_schema = COALESCE(EXCLUDED.params_schema, pf_flows.params_schema),
       updated_at    = now()
RETURNING id, created_at, updated_at`
	err := s.db.QueryRowContext(ctx, q, f.ID, f.Name, f.Version, f.Description,
		textArray(f.Tags), labels, nullJSON(f.ParamsSchema)).Scan(&f.ID, &f.CreatedAt, &f.UpdatedAt)
	return mapErr(err)
}

// ListFlows returns the whole catalogue, newest first.
func (s *Store) ListFlows(ctx context.Context) ([]core.Flow, error) {
	const q = `SELECT id, name, version, description, tags, labels, params_schema, created_at, updated_at
	           FROM pf_flows ORDER BY name, version`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []core.Flow
	for rows.Next() {
		var f core.Flow
		var labels []byte
		if err := rows.Scan(&f.ID, &f.Name, &f.Version, &f.Description,
			pq.Array(&f.Tags), &labels, scanJSON(&f.ParamsSchema), &f.CreatedAt, &f.UpdatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(labels, &f.Labels)
		out = append(out, f)
	}
	return out, rows.Err()
}

// ----------------------------------------------------------- work queues ---

const workQueueCols = `name, description, concurrency_limit, paused,
	min_workers, max_workers, target_ready_per_worker, owner, pool_type,
	push_endpoint, push_secret,
	created_at, updated_at`

func scanWorkQueue(sc interface{ Scan(...any) error }) (*core.WorkQueue, error) {
	var q core.WorkQueue
	if err := sc.Scan(&q.Name, &q.Description, &q.ConcurrencyLimit, &q.Paused,
		&q.MinWorkers, &q.MaxWorkers, &q.TargetReadyPerWorker, &q.Owner, &q.PoolType,
		&q.PushEndpoint, &q.PushSecret,
		&q.CreatedAt, &q.UpdatedAt); err != nil {
		return nil, err
	}
	q.HasPushSecret = q.PushSecret != ""
	return &q, nil
}

// ensureWorkQueueStmt creates a queue row if it is missing and leaves an
// existing one untouched. Anything that merely needs the lane to exist -- a
// deployment referencing it, a worker declaring what it polls -- must use this
// rather than UpsertWorkQueue, whose ON CONFLICT overwrites every column and
// would otherwise reset an operator's concurrency limit, pause switch and
// autoscaling envelope on every worker restart.
const ensureWorkQueueStmt = `INSERT INTO pf_work_queues (name) VALUES ($1) ON CONFLICT DO NOTHING`

// EnsureWorkQueue creates the queue if it does not exist, preserving the
// configuration of one that does.
func (s *Store) EnsureWorkQueue(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, ensureWorkQueueStmt, name)
	return mapErr(err)
}

// UpsertWorkQueue replaces a queue definition in full: every column is written
// from q, so callers must send the complete desired state. To create a lane
// without disturbing an existing one, use EnsureWorkQueue.
func (s *Store) UpsertWorkQueue(ctx context.Context, q *core.WorkQueue) error {
	if q.TargetReadyPerWorker <= 0 {
		q.TargetReadyPerWorker = 5
	}
	if q.PoolType == "" {
		q.PoolType = "pull"
	}
	const stmt = `
INSERT INTO pf_work_queues
  (name, description, concurrency_limit, paused, min_workers, max_workers,
   target_ready_per_worker, owner, pool_type, push_endpoint, push_secret)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (name) DO UPDATE
   SET description             = EXCLUDED.description,
       concurrency_limit       = EXCLUDED.concurrency_limit,
       paused                  = EXCLUDED.paused,
       min_workers             = EXCLUDED.min_workers,
       max_workers             = EXCLUDED.max_workers,
       target_ready_per_worker = EXCLUDED.target_ready_per_worker,
       owner                   = EXCLUDED.owner,
       pool_type               = EXCLUDED.pool_type,
       push_endpoint           = EXCLUDED.push_endpoint,
       -- keep the stored secret when the caller sends an empty one
       push_secret             = COALESCE(NULLIF(EXCLUDED.push_secret, ''), pf_work_queues.push_secret),
       updated_at              = now()
RETURNING created_at, updated_at, push_secret`
	err := s.db.QueryRowContext(ctx, stmt, q.Name, q.Description, q.ConcurrencyLimit,
		q.Paused, q.MinWorkers, q.MaxWorkers, q.TargetReadyPerWorker, q.Owner, q.PoolType,
		q.PushEndpoint, q.PushSecret).
		Scan(&q.CreatedAt, &q.UpdatedAt, &q.PushSecret)
	q.HasPushSecret = q.PushSecret != ""
	return mapErr(err)
}

// GetWorkQueue loads one queue by name.
func (s *Store) GetWorkQueue(ctx context.Context, name string) (*core.WorkQueue, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+workQueueCols+` FROM pf_work_queues WHERE name=$1`, name)
	q, err := scanWorkQueue(row)
	return q, mapErr(err)
}

// ListWorkQueues returns every queue, alphabetically.
func (s *Store) ListWorkQueues(ctx context.Context) ([]core.WorkQueue, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+workQueueCols+` FROM pf_work_queues ORDER BY name`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []core.WorkQueue
	for rows.Next() {
		q, err := scanWorkQueue(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *q)
	}
	return out, rows.Err()
}

// SetQueuePaused stops or resumes dispatch for a queue. Running work is not
// interrupted; only new leases are withheld.
func (s *Store) SetQueuePaused(ctx context.Context, name string, paused bool) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE pf_work_queues SET paused=$2, updated_at=now() WHERE name=$1`, name, paused)
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// QueueStats returns the per-queue counters the admin screen shows.
func (s *Store) QueueStats(ctx context.Context) ([]store.QueueStat, error) {
	const stmt = `
SELECT q.name, q.description, q.concurrency_limit, q.paused,
       q.min_workers, q.max_workers, q.target_ready_per_worker, q.owner, q.pool_type,
       q.push_endpoint, (q.push_secret <> '') AS has_push_secret,
       q.created_at, q.updated_at,
       COALESCE(c.scheduled,0), COALESCE(c.ready,0), COALESCE(c.running,0), COALESCE(c.failed,0)
FROM pf_work_queues q
LEFT JOIN (
    SELECT work_queue,
           count(*) FILTER (WHERE state='SCHEDULED')                          AS scheduled,
           count(*) FILTER (WHERE state='SCHEDULED' AND scheduled_at<=now())  AS ready,
           count(*) FILTER (WHERE state IN ('RUNNING','PENDING'))             AS running,
           count(*) FILTER (WHERE state='FAILED' AND updated_at>now()-interval '24 hours') AS failed
    FROM pf_flow_runs GROUP BY work_queue
) c ON c.work_queue = q.name
ORDER BY q.name`
	rows, err := s.db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []store.QueueStat
	for rows.Next() {
		var st store.QueueStat
		if err := rows.Scan(&st.Name, &st.Description, &st.ConcurrencyLimit, &st.Paused,
			&st.MinWorkers, &st.MaxWorkers, &st.TargetReadyPerWorker, &st.Owner, &st.PoolType,
			&st.PushEndpoint, &st.HasPushSecret,
			&st.CreatedAt, &st.UpdatedAt,
			&st.Scheduled, &st.Ready, &st.Running, &st.Failed24h); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// ----------------------------------------------------------- deployments ---

// UpsertDeployment creates or updates a deployment by name.
func (s *Store) UpsertDeployment(ctx context.Context, d *core.Deployment) error {
	if d.WorkQueue == "" {
		d.WorkQueue = "default"
	}
	if d.Timezone == "" {
		d.Timezone = "UTC"
	}
	// A deployment may only reference an existing queue; create it lazily so
	// declaring infrastructure in code does not need a second API call.
	if _, err := s.db.ExecContext(ctx, ensureWorkQueueStmt, d.WorkQueue); err != nil {
		return mapErr(err)
	}
	const stmt = `
INSERT INTO pf_deployments
  (id, name, flow_name, description, parameters, work_queue, priority, schedule_kind,
   schedule, timezone, paused, tags, retries, retry_delay_ms, timeout_ms, catchup)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
ON CONFLICT (name) DO UPDATE SET
   flow_name      = EXCLUDED.flow_name,
   description    = EXCLUDED.description,
   parameters     = EXCLUDED.parameters,
   work_queue     = EXCLUDED.work_queue,
   priority       = EXCLUDED.priority,
   schedule_kind  = EXCLUDED.schedule_kind,
   schedule       = EXCLUDED.schedule,
   timezone       = EXCLUDED.timezone,
   tags           = EXCLUDED.tags,
   retries        = EXCLUDED.retries,
   retry_delay_ms = EXCLUDED.retry_delay_ms,
   timeout_ms     = EXCLUDED.timeout_ms,
   catchup        = EXCLUDED.catchup,
   updated_at     = now()
RETURNING id, paused, created_at, updated_at`
	err := s.db.QueryRowContext(ctx, stmt,
		d.ID, d.Name, d.FlowName, d.Description, nullJSON(d.Parameters), d.WorkQueue, d.Priority,
		string(d.ScheduleKind), d.Schedule, d.Timezone, d.Paused, textArray(d.Tags),
		d.Retries, msOf(d.RetryDelay), msOf(d.Timeout), d.CatchUp).
		Scan(&d.ID, &d.Paused, &d.CreatedAt, &d.UpdatedAt)
	return mapErr(err)
}

const deploymentCols = `id, name, flow_name, description, parameters, work_queue, priority,
	schedule_kind, schedule, timezone, paused, tags, retries, retry_delay_ms, timeout_ms,
	catchup, created_at, updated_at`

func scanDeployment(sc interface{ Scan(...any) error }) (*core.Deployment, error) {
	var d core.Deployment
	var kind string
	var retryMs, timeoutMs int64
	if err := sc.Scan(&d.ID, &d.Name, &d.FlowName, &d.Description, scanJSON(&d.Parameters),
		&d.WorkQueue, &d.Priority, &kind, &d.Schedule, &d.Timezone, &d.Paused, pq.Array(&d.Tags),
		&d.Retries, &retryMs, &timeoutMs, &d.CatchUp, &d.CreatedAt, &d.UpdatedAt); err != nil {
		return nil, err
	}
	d.ScheduleKind = core.ScheduleKind(kind)
	d.RetryDelay = durOf(retryMs)
	d.Timeout = durOf(timeoutMs)
	return &d, nil
}

// GetDeployment loads a deployment by id.
func (s *Store) GetDeployment(ctx context.Context, id string) (*core.Deployment, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+deploymentCols+` FROM pf_deployments WHERE id=$1`, id)
	d, err := scanDeployment(row)
	return d, mapErr(err)
}

// GetDeploymentByName loads a deployment by its unique name.
func (s *Store) GetDeploymentByName(ctx context.Context, name string) (*core.Deployment, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+deploymentCols+` FROM pf_deployments WHERE name=$1`, name)
	d, err := scanDeployment(row)
	return d, mapErr(err)
}

// ListDeployments returns all deployments alphabetically.
func (s *Store) ListDeployments(ctx context.Context) ([]core.Deployment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+deploymentCols+` FROM pf_deployments ORDER BY name`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []core.Deployment
	for rows.Next() {
		d, err := scanDeployment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *d)
	}
	return out, rows.Err()
}

// DeleteDeployment removes a deployment; its historical runs are kept.
func (s *Store) DeleteDeployment(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM pf_deployments WHERE id=$1`, id)
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// SetDeploymentPaused stops or resumes schedule materialisation.
func (s *Store) SetDeploymentPaused(ctx context.Context, id string, paused bool) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE pf_deployments SET paused=$2, updated_at=now() WHERE id=$1`, id, paused)
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// ------------------------------------------------------------ leadership ---

// AcquireLeadership implements a lease-based singleton election so that only
// one server replica runs the scheduler and janitor loops at a time.
func (s *Store) AcquireLeadership(ctx context.Context, role, holder string, ttl time.Duration) (bool, error) {
	const stmt = `
INSERT INTO pf_leader (role, holder, expires_at)
VALUES ($1, $2, now() + $3::interval)
ON CONFLICT (role) DO UPDATE
   SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
 WHERE pf_leader.expires_at < now() OR pf_leader.holder = EXCLUDED.holder
RETURNING holder`
	var got string
	err := s.db.QueryRowContext(ctx, stmt, role, holder, fmt.Sprintf("%d milliseconds", ttl.Milliseconds())).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil // someone else holds a live lease
	}
	if err != nil {
		return false, mapErr(err)
	}
	return got == holder, nil
}
