package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

const workerSpecCols = `id, name, image, queues, concurrency, replicas, delivery,
	namespace, repo_path, auto_sync, env, argocd,
	last_render_hash, last_synced_hash, last_synced_sha, last_synced_at,
	sync_state, last_error, created_by, created_at, updated_at`

func scanWorkerSpec(sc interface{ Scan(...any) error }) (*core.WorkerSpec, error) {
	var (
		ws       core.WorkerSpec
		envRaw   []byte
		argoRaw  []byte
		syncedAt sql.NullTime
		state    string
	)
	if err := sc.Scan(&ws.ID, &ws.Name, &ws.Image, pq.Array(&ws.Queues), &ws.Concurrency,
		&ws.Replicas, &ws.Delivery, &ws.Namespace, &ws.RepoPath, &ws.AutoSync,
		&envRaw, &argoRaw, &ws.LastRenderHash, &ws.LastSyncedHash, &ws.LastSyncedSHA,
		&syncedAt, &state, &ws.LastError, &ws.CreatedBy, &ws.CreatedAt, &ws.UpdatedAt); err != nil {
		return nil, err
	}
	ws.SyncState = core.SyncState(state)
	if len(envRaw) > 0 {
		_ = json.Unmarshal(envRaw, &ws.Env)
	}
	if len(argoRaw) > 0 {
		var a core.WorkerSpecArgoCD
		if json.Unmarshal(argoRaw, &a) == nil && (a.Project != "" || a.DestServer != "" || a.Revision != "") {
			ws.ArgoCD = &a
		}
	}
	if syncedAt.Valid {
		t := syncedAt.Time.UTC()
		ws.LastSyncedAt = &t
	}
	return &ws, nil
}

// ListWorkerSpecs returns every worker spec, newest first.
func (s *Store) ListWorkerSpecs(ctx context.Context) ([]core.WorkerSpec, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+workerSpecCols+` FROM pf_worker_specs ORDER BY created_at DESC, name`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []core.WorkerSpec
	for rows.Next() {
		ws, err := scanWorkerSpec(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ws)
	}
	return out, rows.Err()
}

// GetWorkerSpec loads one spec by id.
func (s *Store) GetWorkerSpec(ctx context.Context, id string) (*core.WorkerSpec, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+workerSpecCols+` FROM pf_worker_specs WHERE id=$1`, id)
	ws, err := scanWorkerSpec(row)
	return ws, mapErr(err)
}

// GetWorkerSpecByName loads one spec by its (unique) name.
func (s *Store) GetWorkerSpecByName(ctx context.Context, name string) (*core.WorkerSpec, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+workerSpecCols+` FROM pf_worker_specs WHERE name=$1`, name)
	ws, err := scanWorkerSpec(row)
	return ws, mapErr(err)
}

// UpsertWorkerSpec inserts (when ID is empty) or updates a spec. The sync
// bookkeeping columns are never touched here — SetWorkerSpecSyncResult owns
// those — so saving an edit correctly leaves sync_state where it was until the
// next render decides the row has drifted.
func (s *Store) UpsertWorkerSpec(ctx context.Context, ws *core.WorkerSpec) error {
	if ws.Image == "" {
		ws.Image = "primex/primeflow:latest"
	}
	if ws.Concurrency <= 0 {
		ws.Concurrency = 4
	}
	if ws.Replicas <= 0 {
		ws.Replicas = 1
	}
	if ws.Delivery == "" {
		ws.Delivery = "git"
	}
	if ws.Namespace == "" {
		ws.Namespace = "primeflow"
	}
	envRaw, _ := json.Marshal(ws.Env)
	if len(envRaw) == 0 || string(envRaw) == "null" {
		envRaw = []byte("{}")
	}
	var argoRaw any
	if ws.ArgoCD != nil {
		argoRaw, _ = json.Marshal(ws.ArgoCD)
	}

	if ws.ID == "" {
		ws.ID = uuid.NewString()
		const stmt = `
INSERT INTO pf_worker_specs
  (id, name, image, queues, concurrency, replicas, delivery, namespace,
   repo_path, auto_sync, env, argocd, created_by)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
RETURNING created_at, updated_at`
		err := s.db.QueryRowContext(ctx, stmt, ws.ID, ws.Name, ws.Image,
			pq.Array(ws.Queues), ws.Concurrency, ws.Replicas, ws.Delivery, ws.Namespace,
			ws.RepoPath, ws.AutoSync, envRaw, argoRaw, ws.CreatedBy).
			Scan(&ws.CreatedAt, &ws.UpdatedAt)
		if ws.SyncState == "" {
			ws.SyncState = core.SyncPending
		}
		return mapErr(err)
	}

	const stmt = `
UPDATE pf_worker_specs SET
  name=$2, image=$3, queues=$4, concurrency=$5, replicas=$6, delivery=$7,
  namespace=$8, repo_path=$9, auto_sync=$10, env=$11, argocd=$12, updated_at=now()
WHERE id=$1
RETURNING created_at, updated_at, sync_state, last_error`
	var state string
	err := s.db.QueryRowContext(ctx, stmt, ws.ID, ws.Name, ws.Image,
		pq.Array(ws.Queues), ws.Concurrency, ws.Replicas, ws.Delivery, ws.Namespace,
		ws.RepoPath, ws.AutoSync, envRaw, argoRaw).
		Scan(&ws.CreatedAt, &ws.UpdatedAt, &state, &ws.LastError)
	ws.SyncState = core.SyncState(state)
	return mapErr(err)
}

// DeleteWorkerSpec removes a spec. It does not touch the GitOps repo — a
// previously pushed manifest stays until an operator prunes it.
func (s *Store) DeleteWorkerSpec(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM pf_worker_specs WHERE id=$1`, id)
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// WorkerSpecsForAutoSync returns the specs the reconciler is responsible for.
func (s *Store) WorkerSpecsForAutoSync(ctx context.Context) ([]core.WorkerSpec, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+workerSpecCols+` FROM pf_worker_specs WHERE auto_sync ORDER BY name`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []core.WorkerSpec
	for rows.Next() {
		ws, err := scanWorkerSpec(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *ws)
	}
	return out, rows.Err()
}

// SetWorkerSpecSyncResult records the outcome of one git-engine Sync.
func (s *Store) SetWorkerSpecSyncResult(ctx context.Context, id string, r core.WorkerSpecSyncResult) error {
	at := r.At
	if at.IsZero() {
		at = time.Now().UTC()
	}
	var syncedAt any
	syncedSHA := ""
	if r.State == core.SyncSynced {
		syncedAt = at
	}
	if r.CommitSHA != "" {
		syncedSHA = r.CommitSHA
	}
	const stmt = `
UPDATE pf_worker_specs SET
  last_render_hash = $2,
  last_synced_hash = CASE WHEN $3 = 'synced' THEN $4 ELSE last_synced_hash END,
  last_synced_sha  = CASE WHEN $3 = 'synced' AND $5 <> '' THEN $5 ELSE last_synced_sha END,
  last_synced_at   = COALESCE($6, last_synced_at),
  sync_state       = $3,
  last_error       = $7,
  updated_at       = now()
WHERE id=$1`
	res, err := s.db.ExecContext(ctx, stmt, id, r.Hash, string(r.State), r.Hash,
		syncedSHA, syncedAt, r.Err)
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// GitConnectionWithToken returns the git connection including the stored token.
// It exists for the git engine and reconciler in internal/gitsync; ordinary
// reads must use GetGitConnection, which strips the secret.
func (s *Store) GitConnectionWithToken(ctx context.Context) (core.GitConnection, string, error) {
	gc, err := s.GetGitConnection(ctx)
	if err != nil {
		return core.GitConnection{}, "", err
	}
	tok, err := s.gitToken(ctx)
	if err != nil && mapErr(err) != store.ErrNotFound {
		return gc, "", err
	}
	return gc, tok, nil
}
