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

	// --- events & automations ---
	AppendEvent(ctx context.Context, e *core.Event) error
	ListEvents(ctx context.Context, limit int) ([]core.Event, error)
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

	Close() error
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
