package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/primex/primeflow/internal/core"
)

// ErrNotFound is returned by every lookup that finds nothing.
var ErrNotFound = errors.New("not found")

// ErrConflict is returned when a uniqueness constraint or an optimistic
// concurrency check rejects a write.
var ErrConflict = errors.New("conflict")

// FlowRunFilter narrows list queries. Zero values mean "no constraint".
type FlowRunFilter struct {
	States       []core.StateType
	WorkQueues   []string
	FlowNames    []string
	DeploymentID string
	Tag          string
	Since        *time.Time
	Search       string
	Limit        int
	Offset       int
}

// CreateRunInput is the payload for creating a flow run, whether from the API,
// the scheduler, or an automation.
type CreateRunInput struct {
	Name           string
	FlowName       string
	DeploymentID   *string
	Parameters     json.RawMessage
	WorkQueue      string
	Priority       int
	ScheduledAt    time.Time
	Retries        int
	RetryDelay     time.Duration
	Timeout        time.Duration
	Tags           []string
	IdempotencyKey string

	// Sub-flow lineage, set by runtimeBridge.TriggerDeployment.
	ParentRunID   *string
	ParentTaskKey string
	TraceContext  string
}

// LeaseRequest is a worker asking for work.
type LeaseRequest struct {
	WorkerID string
	Queues   []string
	// Max is how many runs the worker still has capacity for.
	Max int
	// LeaseFor is how long the claim is valid before the janitor may reclaim it.
	LeaseFor time.Duration
}

// QueueStat summarises one work queue for the admin UI.
type QueueStat struct {
	core.WorkQueue
	Scheduled int `json:"scheduled"`
	Running   int `json:"running"`
	Failed24h int `json:"failed_24h"`
	// Ready counts scheduled runs whose scheduled_at has already passed.
	Ready int `json:"ready"`
}

// Store is the full persistence surface. The Postgres implementation is the
// only one intended for production; the interface exists so the engine can be
// unit-tested and so a different backend stays possible.
type Store interface {
	// --- catalogue ---
	UpsertFlow(ctx context.Context, f *core.Flow) error
	ListFlows(ctx context.Context) ([]core.Flow, error)

	UpsertWorkQueue(ctx context.Context, q *core.WorkQueue) error
	GetWorkQueue(ctx context.Context, name string) (*core.WorkQueue, error)
	ListWorkQueues(ctx context.Context) ([]core.WorkQueue, error)
	SetQueuePaused(ctx context.Context, name string, paused bool) error
	QueueStats(ctx context.Context) ([]QueueStat, error)

	UpsertDeployment(ctx context.Context, d *core.Deployment) error
	GetDeployment(ctx context.Context, id string) (*core.Deployment, error)
	GetDeploymentByName(ctx context.Context, name string) (*core.Deployment, error)
	ListDeployments(ctx context.Context) ([]core.Deployment, error)
	DeleteDeployment(ctx context.Context, id string) error
	SetDeploymentPaused(ctx context.Context, id string, paused bool) error

	// --- flow runs ---
	CreateFlowRun(ctx context.Context, in CreateRunInput) (*core.FlowRun, error)
	GetFlowRun(ctx context.Context, id string) (*core.FlowRun, error)
	ListFlowRuns(ctx context.Context, f FlowRunFilter) ([]core.FlowRun, error)
	// PendingInQueue previews a queue in the exact order dispatch will take
	// it, which is what makes the admin reordering controls trustworthy.
	PendingInQueue(ctx context.Context, queue string, limit int) ([]core.FlowRun, error)
	CountFlowRuns(ctx context.Context, f FlowRunFilter) (int, error)

	// SetFlowRunState applies the orchestration rules and records the change.
	// It returns the updated run, or ErrInvalidTransition-wrapping error.
	SetFlowRunState(ctx context.Context, id string, st core.State, opts StateOpts) (*core.FlowRun, error)

	// LeaseFlowRuns atomically claims up to req.Max ready runs, honouring
	// queue pause flags, per-queue concurrency limits and the priority
	// ordering. This is the heart of dispatch.
	LeaseFlowRuns(ctx context.Context, req LeaseRequest) ([]core.FlowRun, error)
	RenewLease(ctx context.Context, runID, workerID string, d time.Duration) error
	ReclaimExpiredLeases(ctx context.Context, now time.Time) ([]core.FlowRun, error)

	// --- operator queue controls ---
	SetRunPriority(ctx context.Context, runID string, priority int) (*core.FlowRun, error)
	MoveRunToFront(ctx context.Context, runID string) (*core.FlowRun, error)
	MoveRunToBack(ctx context.Context, runID string) (*core.FlowRun, error)
	ClearRunPin(ctx context.Context, runID string) (*core.FlowRun, error)
	RequestCancel(ctx context.Context, runID string) (*core.FlowRun, error)
	RescheduleRun(ctx context.Context, runID string, at time.Time) (*core.FlowRun, error)
	MoveRunToQueue(ctx context.Context, runID, queue string) (*core.FlowRun, error)

	// --- task runs (durable checkpoints) ---
	GetTaskRun(ctx context.Context, flowRunID, taskKey string) (*core.TaskRun, error)
	ListTaskRuns(ctx context.Context, flowRunID string) ([]core.TaskRun, error)
	UpsertTaskRun(ctx context.Context, tr *core.TaskRun) error
	FindCachedResult(ctx context.Context, cacheKey string, now time.Time) (json.RawMessage, bool, error)

	// --- observability ---
	AppendLogs(ctx context.Context, recs []core.LogRecord) error
	ListLogs(ctx context.Context, flowRunID string, afterID int64, limit int) ([]core.LogRecord, error)
	CreateArtifact(ctx context.Context, a *core.Artifact) error
	ListArtifacts(ctx context.Context, flowRunID string) ([]core.Artifact, error)
	// GetLogRetention / PutLogRetention manage the pf_logs cleanup policy;
	// DeleteLogsOlderThan is the batched delete the janitor runs.
	GetLogRetention(ctx context.Context) (core.LogRetention, error)
	PutLogRetention(ctx context.Context, in core.LogRetention) error
	DeleteLogsOlderThan(ctx context.Context, cutoff time.Time, maxRows int) (deleted int, more bool, err error)

	// --- sub-flows ---
	// ListChildRuns returns every run whose parent_run_id is parentID.
	ListChildRuns(ctx context.Context, parentID string) ([]core.FlowRun, error)
	// CountUnfinishedChildren counts children not yet in a terminal state.
	CountUnfinishedChildren(ctx context.Context, parentID string) (int, error)
	// AncestorDeploymentIDs walks parent_run_id upward from runID (bounded by
	// maxDepth) and returns the deployment id of each ancestor, nearest first.
	// It is how the sub-flow guard detects recursion and enforces a depth cap.
	AncestorDeploymentIDs(ctx context.Context, runID string, maxDepth int) ([]string, error)
	// ResumeSuspendedRun reschedules a run only if it is still SCHEDULED, so a
	// parent that is transiently RUNNING is never disturbed. Returns
	// ErrNotFound when nothing was resumed.
	ResumeSuspendedRun(ctx context.Context, runID string, at time.Time) (*core.FlowRun, error)

	// --- events & automations ---
	AppendEvent(ctx context.Context, e *core.Event) error
	ListEvents(ctx context.Context, limit int) ([]core.Event, error)
	// Stats returns time-bucketed flow-run / task-run / event activity over the
	// window, for the console Dashboard. buckets is the desired slice count.
	Stats(ctx context.Context, window time.Duration, buckets int) (core.Stats, error)
	// ListEventsAfter reads forward from a sequence cursor; the automation
	// evaluator uses it so no event is evaluated twice or skipped.
	ListEventsAfter(ctx context.Context, afterSeq int64, limit int) ([]core.Event, error)
	MaxEventSeq(ctx context.Context) (int64, error)
	CountEvents(ctx context.Context, name, resourceType string, since time.Time) (int, error)
	UpsertAutomation(ctx context.Context, a *core.Automation) error
	ListAutomations(ctx context.Context) ([]core.Automation, error)
	DeleteAutomation(ctx context.Context, id string) error
	TouchAutomation(ctx context.Context, id string, at time.Time) error

	// --- workers ---
	HeartbeatWorker(ctx context.Context, w *core.WorkerInfo) error
	ListWorkers(ctx context.Context) ([]core.WorkerInfo, error)

	// --- leadership for singleton loops ---
	AcquireLeadership(ctx context.Context, role, holder string, ttl time.Duration) (bool, error)

	// --- operator login & external API (see AuthStore) ---
	AuthStore

	Close() error
}

// --- operator accounts, sessions ---

// UserInput creates or updates an operator account. PasswordHash is written only
// when non-empty, so a role/active change need not resupply it.
type UserInput struct {
	ID           string
	Email        string
	PasswordHash string
	Role         string
	Active       bool
}

// APIKeyFilter narrows an API-key listing. Zero values mean "no constraint".
type APIKeyFilter struct {
	Role   string
	Active *bool
	Search string
}

// AuthStore is the persistence surface for operator login and the External API
// subsystem. It is part of Store; it is called out separately only for reading.
type AuthStore interface {
	CreateUser(ctx context.Context, in UserInput) (*core.User, error)
	GetUser(ctx context.Context, id string) (*core.User, error)
	GetUserByEmail(ctx context.Context, email string) (*core.User, error)
	// VerifyLogin returns the user and its stored password hash for a login
	// attempt; the hash never leaves the store package otherwise.
	GetUserAuth(ctx context.Context, email string) (user *core.User, passwordHash string, err error)
	ListUsers(ctx context.Context) ([]core.User, error)
	CountUsers(ctx context.Context) (int, error)
	UpdateUser(ctx context.Context, id string, role *string, active *bool, passwordHash string) (*core.User, error)
	TouchUserLogin(ctx context.Context, id string, at time.Time) error

	CreateSession(ctx context.Context, s *core.Session) error
	// GetSession returns the session and its user, or ErrNotFound when the
	// session is missing, expired, or the user is inactive.
	GetSession(ctx context.Context, id string, now time.Time) (*core.Session, *core.User, error)
	TouchSession(ctx context.Context, id string, lastSeen, expires time.Time) error
	DeleteSession(ctx context.Context, id string) error
	DeleteUserSessions(ctx context.Context, userID string) error
	DeleteExpiredSessions(ctx context.Context, now time.Time) (int, error)

	// --- external API settings ---
	GetExternalAPISettings(ctx context.Context) (core.ExternalAPISettings, error)
	PutExternalAPISettings(ctx context.Context, s core.ExternalAPISettings) error

	// --- api keys ---
	CreateAPIKey(ctx context.Context, k *core.APIKey) error
	GetAPIKey(ctx context.Context, id string) (*core.APIKey, error)
	GetAPIKeyByPrefix(ctx context.Context, prefix string) (*core.APIKey, error)
	ListAPIKeys(ctx context.Context, f APIKeyFilter) ([]core.APIKey, error)
	UpdateAPIKey(ctx context.Context, k *core.APIKey) error
	SetAPIKeySecret(ctx context.Context, id, prefix, secretHash string) error
	DeleteAPIKey(ctx context.Context, id string) error
	TouchAPIKey(ctx context.Context, id string, at time.Time) error

	AppendAPIKeyEvent(ctx context.Context, e *core.APIKeyEvent) error
	ListAPIKeyEvents(ctx context.Context, apiKeyID string, limit int) ([]core.APIKeyEvent, error)
	TrimAPIKeyEvents(ctx context.Context, keep int) (int, error)
}

// StateOpts carries the side effects that accompany a state change.
type StateOpts struct {
	WorkerID   *string
	Result     json.RawMessage
	StartedAt  *time.Time
	EndedAt    *time.Time
	ScheduleAt *time.Time // for SCHEDULED transitions (retry backoff, durable sleep)
	BumpRun    bool       // increment run_count
	ClearLease bool
	// Force skips the transition rule table. Reserved for the janitor.
	Force bool
}
