package core

import (
	"encoding/json"
	"time"
)

// Priority bounds. Higher numbers are dispatched first. The band is deliberately
// small so an operator can reason about it: 0 background, 50 normal, 100 urgent.
const (
	PriorityMin      = 0
	PriorityNormal   = 50
	PriorityMax      = 100
	PriorityUnpinned = 1 << 30 // sentinel for "not manually pinned to the front"
)

// Flow is a registered unit of work. Registration is performed by workers on
// startup; the server keeps the catalogue so the UI and API can list what is
// runnable even when no worker is currently online.
type Flow struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Version     string            `json:"version"`
	Description string            `json:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	// ParamsSchema is a compact, reflection-derived description of the flow's
	// parameter struct, published by the worker. The console's Flows page uses it
	// to render a typed quick-run form. Nil when the flow declared no schema.
	ParamsSchema json.RawMessage `json:"params_schema,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// ScheduleKind selects how Deployment.Schedule is interpreted.
type ScheduleKind string

const (
	ScheduleNone     ScheduleKind = ""
	ScheduleCron     ScheduleKind = "cron"
	ScheduleInterval ScheduleKind = "interval"
)

// Deployment binds a flow to parameters, a schedule, a work queue and a default
// priority. It is the object an operator triggers, not the flow itself.
type Deployment struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	FlowName     string          `json:"flow_name"`
	Description  string          `json:"description,omitempty"`
	Parameters   json.RawMessage `json:"parameters,omitempty"`
	WorkQueue    string          `json:"work_queue"`
	Priority     int             `json:"priority"`
	ScheduleKind ScheduleKind    `json:"schedule_kind,omitempty"`
	// Schedule holds a 5- or 6-field cron expression when ScheduleKind is
	// "cron", or a Go duration string such as "15m" when it is "interval".
	Schedule string   `json:"schedule,omitempty"`
	Timezone string   `json:"timezone,omitempty"`
	Paused   bool     `json:"paused"`
	Tags     []string `json:"tags,omitempty"`
	// Retries and RetryDelay are the flow-level defaults applied to runs
	// created from this deployment.
	Retries    int           `json:"retries"`
	RetryDelay time.Duration `json:"retry_delay"`
	Timeout    time.Duration `json:"timeout"`
	// CatchUp controls whether the scheduler backfills missed windows after
	// downtime. Most CMP work wants false: run once, now, not eight times.
	CatchUp   bool      `json:"catchup"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// WorkQueue is a named lane with its own concurrency limit and pause switch.
// Workers poll one or more queues; operators throttle a queue to protect a
// downstream system (a vCenter, a billing API) without touching the flows.
type WorkQueue struct {
	Name             string `json:"name"`
	Description      string `json:"description,omitempty"`
	ConcurrencyLimit *int   `json:"concurrency_limit,omitempty"`
	Paused           bool   `json:"paused"`

	// Work-pool autoscaling envelope. The server exposes
	// primeflow_queue_desired_workers = clamp(ceil(ready / TargetReadyPerWorker),
	// MinWorkers, MaxWorkers) for a KEDA ScaledObject or an HPA to act on;
	// PrimeFlow itself never starts or stops workers. Owner is free text so an
	// external team's pool is attributable. PoolType is "pull" (only mode) or the
	// reserved "push".
	MinWorkers           int    `json:"min_workers"`
	MaxWorkers           *int   `json:"max_workers,omitempty"`
	TargetReadyPerWorker int    `json:"target_ready_per_worker"`
	Owner                string `json:"owner,omitempty"`
	PoolType             string `json:"pool_type,omitempty"`

	// Push pools: PoolType == "push". The server dispatches ready runs to
	// PushEndpoint (HMAC-signed with PushSecret) instead of workers polling.
	// PushSecret is write-only — reads report HasPushSecret instead.
	PushEndpoint  string `json:"push_endpoint,omitempty"`
	PushSecret    string `json:"-"`
	HasPushSecret bool   `json:"has_push_secret,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// FlowRun is one execution of a flow.
type FlowRun struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	FlowName     string          `json:"flow_name"`
	DeploymentID *string         `json:"deployment_id,omitempty"`
	Parameters   json.RawMessage `json:"parameters,omitempty"`
	WorkQueue    string          `json:"work_queue"`

	State        StateType `json:"state"`
	StateName    string    `json:"state_name"`
	StateMessage string    `json:"state_message,omitempty"`

	// Priority is the dispatch weight; QueuePosition, when set, pins the run
	// ahead of everything unpinned regardless of priority. Both are operator
	// controllable at runtime.
	Priority      int  `json:"priority"`
	QueuePosition *int `json:"queue_position,omitempty"`

	ScheduledAt time.Time  `json:"scheduled_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	EndedAt     *time.Time `json:"ended_at,omitempty"`

	// RunCount counts worker attempts; Retries is the budget copied from the
	// deployment or flow at creation time.
	RunCount   int           `json:"run_count"`
	Retries    int           `json:"retries"`
	RetryDelay time.Duration `json:"retry_delay"`
	Timeout    time.Duration `json:"timeout"`

	// Lease fields. A worker holds a run by extending LeaseExpiresAt with
	// heartbeats; the janitor crashes runs whose lease has lapsed.
	WorkerID       *string    `json:"worker_id,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`

	Result        json.RawMessage `json:"result,omitempty"`
	Tags          []string        `json:"tags,omitempty"`
	CancelRequest bool            `json:"cancel_requested"`

	// Sub-flow lineage. ParentRunID and ParentTaskKey are set when this run was
	// started by another run's RunDeployment / RunDeploymentAndWait. TraceContext
	// carries the parent's W3C traceparent so spans nest across the boundary.
	ParentRunID   *string `json:"parent_run_id,omitempty"`
	ParentTaskKey *string `json:"parent_task_key,omitempty"`
	TraceContext  string  `json:"-"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TaskRun is a checkpoint inside a flow run. Its persisted Result is what makes
// execution durable: when a flow re-runs after a crash, completed task runs are
// replayed from storage instead of re-executing.
type TaskRun struct {
	ID        string `json:"id"`
	FlowRunID string `json:"flow_run_id"`
	// TaskKey is deterministic within a flow run: either an explicit key the
	// author supplied or "<task name>-<ordinal>".
	TaskKey  string `json:"task_key"`
	TaskName string `json:"task_name"`

	State        StateType `json:"state"`
	StateName    string    `json:"state_name"`
	StateMessage string    `json:"state_message,omitempty"`

	Result     json.RawMessage `json:"result,omitempty"`
	RunCount   int             `json:"run_count"`
	Retries    int             `json:"retries"`
	CacheKey   *string         `json:"cache_key,omitempty"`
	CacheUntil *time.Time      `json:"cache_expires_at,omitempty"`

	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// LogRecord is a structured log line attached to a run.
type LogRecord struct {
	ID        int64   `json:"id"`
	FlowRunID string  `json:"flow_run_id"`
	TaskRunID *string `json:"task_run_id,omitempty"`
	Level     string  `json:"level"`
	Message   string  `json:"message"`
	// RawMessage, not []byte: the column is JSONB and this struct is both the
	// API response and the worker->server wire type, so the structured fields
	// have to travel as JSON rather than as a base64 string.
	Fields    json.RawMessage `json:"fields,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

// ArtifactKind describes how the UI should render an artifact.
type ArtifactKind string

const (
	ArtifactMarkdown ArtifactKind = "markdown"
	ArtifactTable    ArtifactKind = "table"
	ArtifactLink     ArtifactKind = "link"
	ArtifactJSON     ArtifactKind = "json"
)

// Artifact is a named, human-facing output of a run: a provisioning summary, a
// metering table, a link to the VM that was created.
type Artifact struct {
	ID          string          `json:"id"`
	FlowRunID   *string         `json:"flow_run_id,omitempty"`
	TaskRunID   *string         `json:"task_run_id,omitempty"`
	Key         string          `json:"key"`
	Kind        ArtifactKind    `json:"kind"`
	Description string          `json:"description,omitempty"`
	Data        json.RawMessage `json:"data"`
	CreatedAt   time.Time       `json:"created_at"`
}

// Event is an immutable record of something that happened. Every state change
// emits one; automations subscribe to them.
type Event struct {
	ID string `json:"id"`
	// Seq is a monotonic cursor assigned by the store, used by the automation
	// evaluator to read forward without re-reading or missing rows.
	Seq          int64           `json:"seq"`
	Name         string          `json:"event"` // e.g. "flow-run.Failed"
	ResourceType string          `json:"resource_type"`
	ResourceID   string          `json:"resource_id"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	Occurred     time.Time       `json:"occurred"`
}

// ActionKind enumerates what an automation may do when it fires.
type ActionKind string

const (
	ActionRunDeployment ActionKind = "run-deployment"
	ActionCancelRun     ActionKind = "cancel-run"
	ActionSetPriority   ActionKind = "set-priority"
	ActionPauseQueue    ActionKind = "pause-queue"
	ActionResumeQueue   ActionKind = "resume-queue"
	ActionWebhook       ActionKind = "webhook"
)

// Automation is an event-driven rule: match N events of a kind inside a window,
// then perform an action.
type Automation struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Enabled     bool   `json:"enabled"`

	// Trigger matching. Empty fields match anything.
	MatchEvent      string `json:"match_event"`                // exact event name, or prefix ending in "*"
	MatchDeployment string `json:"match_deployment,omitempty"` // deployment name
	MatchFlow       string `json:"match_flow,omitempty"`       // flow name
	MatchWorkQueue  string `json:"match_work_queue,omitempty"` // work queue name
	MatchTag        string `json:"match_tag,omitempty"`        // run tag

	// Threshold/Window implement "3 failures in 10 minutes" style rules.
	Threshold int           `json:"threshold"`
	Window    time.Duration `json:"window"`

	Action       ActionKind      `json:"action"`
	ActionConfig json.RawMessage `json:"action_config,omitempty"`

	LastFiredAt *time.Time `json:"last_fired_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// Stats is the time-bucketed activity the console's Dashboard renders. Every
// bucket list is oldest-first and evenly spaced by BucketSeconds.
type Stats struct {
	WindowSeconds int `json:"window_seconds"`
	BucketSeconds int `json:"bucket_seconds"`

	FlowRuns struct {
		Total   int             `json:"total"`
		ByState map[string]int  `json:"by_state"`
		Buckets []FlowRunBucket `json:"buckets"`
	} `json:"flow_runs"`

	TaskRuns struct {
		Total     int          `json:"total"`
		Completed int          `json:"completed"`
		Failed    int          `json:"failed"`
		Buckets   []TaskBucket `json:"buckets"`
	} `json:"task_runs"`

	Events struct {
		Total   int           `json:"total"`
		Buckets []CountBucket `json:"buckets"`
	} `json:"events"`
}

// FlowRunBucket counts flow-run activity in one time slice by outcome class.
type FlowRunBucket struct {
	T         time.Time `json:"t"`
	Completed int       `json:"completed"`
	Failed    int       `json:"failed"`
	Other     int       `json:"other"`
}

// TaskBucket counts task-run completions in one time slice.
type TaskBucket struct {
	T         time.Time `json:"t"`
	Completed int       `json:"completed"`
	Failed    int       `json:"failed"`
}

// CountBucket is a plain count in one time slice.
type CountBucket struct {
	T time.Time `json:"t"`
	N int       `json:"n"`
}

// WorkerInfo is a heartbeat record so the UI can show who is online.
type WorkerInfo struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Queues        []string  `json:"queues"`
	Concurrency   int       `json:"concurrency"`
	ActiveRuns    int       `json:"active_runs"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	StartedAt     time.Time `json:"started_at"`
}

// Online reports whether the worker heartbeated recently enough to be trusted.
func (w WorkerInfo) Online(now time.Time, ttl time.Duration) bool {
	return now.Sub(w.LastHeartbeat) < ttl
}
