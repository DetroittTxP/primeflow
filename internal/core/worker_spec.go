package core

import "time"

// SyncState is where a WorkerSpec stands relative to the GitOps repo.
type SyncState string

const (
	SyncPending SyncState = "pending" // never pushed, or spec changed since
	SyncSynced  SyncState = "synced"  // repo matches the last render
	SyncDrift   SyncState = "drift"   // render differs from last push (reconciler will act)
	SyncError   SyncState = "error"   // last sync attempt failed; see LastError
)

// WorkerSpecArgoCD holds the Argo CD Application fields, used only when
// Delivery == "argocd".
type WorkerSpecArgoCD struct {
	Project    string `json:"project,omitempty"`
	DestServer string `json:"dest_server,omitempty"`
	Revision   string `json:"revision,omitempty"`
}

// WorkerSpec is a persisted, server-rendered worker deployment: the inputs the
// Add-worker wizard collects, plus the sync bookkeeping the git engine and the
// reconciler maintain. It is the source of truth behind GitOps worker delivery.
type WorkerSpec struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Image       string            `json:"image"`
	Queues      []string          `json:"queues"`
	Concurrency int               `json:"concurrency"`
	Replicas    int               `json:"replicas"`
	Delivery    string            `json:"delivery"` // git | argocd | flux
	Namespace   string            `json:"namespace"`
	RepoPath    string            `json:"repo_path"` // blank => <git base_path>/<name>
	AutoSync    bool              `json:"auto_sync"`
	Env         map[string]string `json:"env,omitempty"`
	ArgoCD      *WorkerSpecArgoCD `json:"argocd,omitempty"`

	LastRenderHash string     `json:"last_render_hash,omitempty"`
	LastSyncedHash string     `json:"last_synced_hash,omitempty"`
	LastSyncedSHA  string     `json:"last_synced_sha,omitempty"`
	LastSyncedAt   *time.Time `json:"last_synced_at,omitempty"`
	SyncState      SyncState  `json:"sync_state"`
	LastError      string     `json:"last_error,omitempty"`

	CreatedBy string    `json:"created_by,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// WorkerSpecSyncResult is what one git-engine Sync produced. The store writes it
// back onto the row.
type WorkerSpecSyncResult struct {
	State     SyncState `json:"sync_state"`
	Hash      string    `json:"hash"`       // tree hash that was pushed
	CommitSHA string    `json:"commit_sha"` // "" when the push was a no-op
	Err       string    `json:"error,omitempty"`
	At        time.Time `json:"at"`
}
