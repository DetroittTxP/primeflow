// Package sdk is the authoring surface of PrimeFlow.
//
// A flow is an ordinary Go function. There is no DAG to declare and no DSL to
// learn: you write normal control flow — loops, conditionals, early returns —
// and wrap the expensive or side-effecting parts in Task, which checkpoints
// their results.
//
//	sdk.Flow("provision-vm", func(ctx *sdk.Context) (any, error) {
//	    p, err := sdk.Params[ProvisionParams](ctx)
//	    if err != nil {
//	        return nil, err
//	    }
//	    vm, err := sdk.Task(ctx, "create-vm", func(ctx *sdk.Context) (VM, error) {
//	        return vcd.CreateVM(ctx, p.OrgID, p.Template)
//	    }, sdk.TaskRetries(3), sdk.TaskRetryDelay(10*time.Second))
//	    if err != nil {
//	        return nil, err
//	    }
//	    if err := sdk.Sleep(ctx, "settle", 2*time.Minute); err != nil {
//	        return nil, err
//	    }
//	    return vm, sdk.Do(ctx, "register-metering", func(ctx *sdk.Context) error {
//	        return metering.Register(ctx, vm.ID)
//	    })
//	}, sdk.Retries(2))
//
// Durability comes from those checkpoints. If the worker is killed halfway
// through, the run is rescheduled and the function is executed again from the
// top — but every task that already completed returns its stored result
// instead of running again. Expensive work is never repeated, and you never
// have to write resume logic by hand.
//
// The cost of that model is one rule: code outside a Task may run more than
// once. Keep side effects inside tasks and the rule takes care of itself.
package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ---------------------------------------------------------------- errors ---

// PermanentError wraps an error that must not be retried, however much retry
// budget is left. Use it for validation failures and 4xx responses.
type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

// Permanent marks err as non-retryable.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return PermanentError{Err: err}
}

// IsPermanent reports whether err was marked non-retryable.
func IsPermanent(err error) bool {
	var p PermanentError
	return errors.As(err, &p)
}

// SuspendError unwinds a flow without failing it. The run returns to the queue
// with a scheduled time of Until and resumes from its last checkpoint. Sleep
// and WaitUntil raise it; you can also raise it directly to wait for an
// external condition without holding a worker slot.
type SuspendError struct {
	Until  time.Time
	Reason string
}

func (e SuspendError) Error() string {
	return fmt.Sprintf("suspended until %s: %s", e.Until.Format(time.RFC3339), e.Reason)
}

// IsSuspend reports whether err is a suspension request and returns it.
func IsSuspend(err error) (SuspendError, bool) {
	var s SuspendError
	ok := errors.As(err, &s)
	return s, ok
}

// ErrCancelled is returned from checkpoints once an operator cancels the run.
var ErrCancelled = errors.New("run cancelled")

// ------------------------------------------------------------- runtime ------

// CheckpointStatus mirrors the persisted task-run state the engine stores.
type CheckpointStatus string

const (
	CheckpointRunning   CheckpointStatus = "RUNNING"
	CheckpointCompleted CheckpointStatus = "COMPLETED"
	CheckpointFailed    CheckpointStatus = "FAILED"
)

// Checkpoint is a persisted task result.
type Checkpoint struct {
	Key      string
	Name     string
	Status   CheckpointStatus
	Result   json.RawMessage
	Message  string
	Attempt  int
	Retries  int
	CacheKey string
	CacheTTL time.Duration
	Started  *time.Time
	Ended    *time.Time
}

// LogEntry is one structured log line produced by user code.
type LogEntry struct {
	Level   string
	Message string
	Fields  map[string]any
	TaskKey string
	At      time.Time
}

// ArtifactSpec is a human-facing output attached to the run.
type ArtifactSpec struct {
	Key         string
	Kind        string
	Description string
	Data        json.RawMessage
	TaskKey     string
}

// TriggerOptions customise a deployment triggered from inside a flow.
type TriggerOptions struct {
	Priority  int
	WorkQueue string
	Delay     time.Duration
	Tags      []string
	// IdempotencyKey, when set, makes the trigger safe to replay: a second
	// trigger with the same key returns the existing run instead of a new one.
	// RunDeploymentAndWait sets it so a parent's replay never fans out twice.
	IdempotencyKey string
	// ParentTaskKey records which of the parent's checkpoints spawned the child,
	// for the console's sub-flow view.
	ParentTaskKey string
}

// RunState is a point-in-time view of another run, used by RunDeploymentAndWait
// to decide whether to keep waiting.
type RunState struct {
	Status  string // COMPLETED | FAILED | CANCELLED | RUNNING | SCHEDULED | ...
	Result  json.RawMessage
	Message string
}

// Terminal reports whether the run has reached a state it will not leave on its
// own.
func (s RunState) Terminal() bool {
	switch s.Status {
	case "COMPLETED", "FAILED", "CANCELLED":
		return true
	}
	return false
}

// Runtime is what the engine supplies to a running flow. Authors never
// implement it; it is exported so the engine (and tests) can.
type Runtime interface {
	// LoadCheckpoint returns a stored checkpoint for key, or nil if none.
	LoadCheckpoint(ctx context.Context, key string) (*Checkpoint, error)
	// SaveCheckpoint persists a checkpoint.
	SaveCheckpoint(ctx context.Context, cp Checkpoint) error
	// LookupCache finds a completed result stored under a cross-run cache key.
	LookupCache(ctx context.Context, cacheKey string) (json.RawMessage, bool, error)
	// Log accepts a log line; implementations buffer and flush.
	Log(e LogEntry)
	// Artifact records a run output.
	Artifact(ctx context.Context, a ArtifactSpec) error
	// TriggerDeployment schedules another deployment and returns its run id.
	TriggerDeployment(ctx context.Context, name string, params json.RawMessage, o TriggerOptions) (string, error)
	// GetRunState reads another run's current state, for RunDeploymentAndWait.
	GetRunState(ctx context.Context, runID string) (RunState, error)
}

// ------------------------------------------------------------- context ------

// RunInfo describes the run the flow is executing under.
type RunInfo struct {
	RunID        string
	RunName      string
	FlowName     string
	DeploymentID string
	WorkQueue    string
	Priority     int
	Attempt      int
	Tags         []string
	ScheduledAt  time.Time
}

// Context is passed to every flow and task. It satisfies context.Context, so
// it can be handed straight to any library that takes one; cancelling the run
// from the UI cancels it.
type Context struct {
	ctx context.Context
	rt  Runtime
	run RunInfo

	mu       sync.Mutex
	ordinals map[string]int

	// suspendThreshold is how long a Sleep must be before the run gives up its
	// worker slot instead of blocking.
	suspendThreshold time.Duration
}

// NewContext builds a flow context. The engine calls this; tests may too.
func NewContext(ctx context.Context, rt Runtime, run RunInfo) *Context {
	return &Context{
		ctx:              ctx,
		rt:               rt,
		run:              run,
		ordinals:         map[string]int{},
		suspendThreshold: 30 * time.Second,
	}
}

// SetSuspendThreshold changes when Sleep suspends rather than blocks.
func (c *Context) SetSuspendThreshold(d time.Duration) { c.suspendThreshold = d }

func (c *Context) Deadline() (time.Time, bool) { return c.ctx.Deadline() }
func (c *Context) Done() <-chan struct{}       { return c.ctx.Done() }
func (c *Context) Err() error                  { return c.ctx.Err() }
func (c *Context) Value(k any) any             { return c.ctx.Value(k) }

// Run returns metadata about the current run.
func (c *Context) Run() RunInfo { return c.run }

// Runtime exposes the engine bridge, for advanced use.
func (c *Context) Runtime() Runtime { return c.rt }

// nextKey produces a deterministic key for an unnamed task occurrence. Because
// it depends only on the order tasks are reached, a replay after a crash lands
// on the same keys and therefore finds the same checkpoints.
func (c *Context) nextKey(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ordinals[name]++
	return fmt.Sprintf("%s-%d", name, c.ordinals[name])
}

// ---------------------------------------------------------------- logging ---

func (c *Context) log(level, msg string, kv []any) {
	fields := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			k = fmt.Sprint(kv[i])
		}
		fields[k] = kv[i+1]
	}
	c.rt.Log(LogEntry{Level: level, Message: msg, Fields: fields, At: time.Now().UTC()})
}

// Debug logs at DEBUG level. Extra arguments are key/value pairs.
func (c *Context) Debug(msg string, kv ...any) { c.log("DEBUG", msg, kv) }

// Info logs at INFO level.
func (c *Context) Info(msg string, kv ...any) { c.log("INFO", msg, kv) }

// Warn logs at WARN level.
func (c *Context) Warn(msg string, kv ...any) { c.log("WARN", msg, kv) }

// Error logs at ERROR level.
func (c *Context) Error(msg string, kv ...any) { c.log("ERROR", msg, kv) }

// -------------------------------------------------------------- artifacts ---

// Artifact attaches an arbitrary JSON output to the run.
func (c *Context) Artifact(key, kind, description string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return c.rt.Artifact(c.ctx, ArtifactSpec{Key: key, Kind: kind, Description: description, Data: raw})
}

// Markdown attaches a markdown artifact — the usual way to leave a readable
// summary of what a run did.
func (c *Context) Markdown(key, md string) error {
	return c.Artifact(key, "markdown", "", md)
}

// Table attaches a tabular artifact. rows should marshal to an array of objects.
func (c *Context) Table(key string, rows any) error {
	return c.Artifact(key, "table", "", rows)
}

// Link attaches a labelled URL, e.g. the console page for a VM just created.
func (c *Context) Link(key, url, description string) error {
	return c.Artifact(key, "link", description, url)
}

// ----------------------------------------------------------- parameters -----

type paramsKey struct{}

// WithParams stores the run parameters on the context. The engine calls it.
func WithParams(ctx context.Context, raw json.RawMessage) context.Context {
	return context.WithValue(ctx, paramsKey{}, raw)
}

// RawParams returns the run's parameters as stored.
func (c *Context) RawParams() json.RawMessage {
	raw, _ := c.ctx.Value(paramsKey{}).(json.RawMessage)
	return raw
}

// Params decodes the run parameters into T. A run with no parameters yields
// the zero value and no error, so optional-parameter flows stay simple.
func Params[T any](c *Context) (T, error) {
	var out T
	raw := c.RawParams()
	if len(raw) == 0 || string(raw) == "null" {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, Permanent(fmt.Errorf("decode parameters: %w", err))
	}
	return out, nil
}

// --------------------------------------------------------- triggering -------

// RunDeployment schedules another deployment and returns the new run id. It
// does not wait for it; use it to fan out work or hand off to a different
// queue.
func (c *Context) RunDeployment(name string, params any, opts ...TriggerOption) (string, error) {
	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			return "", err
		}
		raw = b
	}
	o := TriggerOptions{}
	for _, f := range opts {
		f(&o)
	}
	return c.rt.TriggerDeployment(c.ctx, name, raw, o)
}

// ChildResult is what a completed sub-flow returns to its parent.
type ChildResult struct {
	RunID  string
	Status string
	Result json.RawMessage
}

// Into unmarshals the child's result into v.
func (r ChildResult) Into(v any) error {
	if len(r.Result) == 0 || string(r.Result) == "null" {
		return nil
	}
	return json.Unmarshal(r.Result, v)
}

// RunDeploymentAndWait triggers another deployment and blocks — durably — until
// it finishes, returning its result. It is a checkpointed operation: the trigger
// fires exactly once no matter how often the parent replays, and while the child
// runs the parent releases its worker slot (it is rescheduled the moment the
// child settles, or re-polls every 30s as a backstop).
//
// A failed or cancelled child returns a permanent error, so the parent fails
// without burning its own retry budget on a child that will not succeed.
func (c *Context) RunDeploymentAndWait(name string, params any, opts ...TriggerOption) (ChildResult, error) {
	key := c.nextKey("subflow:" + name)

	cp, err := c.rt.LoadCheckpoint(c.ctx, key)
	if err != nil {
		return ChildResult{}, err
	}
	// Result carries the child run id whatever the checkpoint's status says, so
	// a replay recovers the id it already triggered and never fans out twice.
	var childID string
	var settled bool
	if cp != nil {
		if len(cp.Result) > 0 {
			_ = json.Unmarshal(cp.Result, &childID)
		}
		settled = cp.Status == CheckpointCompleted || cp.Status == CheckpointFailed
	}

	if childID == "" {
		var raw json.RawMessage
		if params != nil {
			b, mErr := json.Marshal(params)
			if mErr != nil {
				return ChildResult{}, mErr
			}
			raw = b
		}
		o := TriggerOptions{
			IdempotencyKey: "child:" + c.run.RunID + ":" + key,
			ParentTaskKey:  key,
		}
		for _, f := range opts {
			f(&o)
		}
		id, tErr := c.rt.TriggerDeployment(c.ctx, name, raw, o)
		if tErr != nil {
			return ChildResult{}, tErr
		}
		childID = id
		idRaw, _ := json.Marshal(childID)
		if sErr := c.rt.SaveCheckpoint(c.ctx, Checkpoint{
			Key: key, Name: "subflow:" + name, Status: CheckpointRunning, Result: idRaw,
		}); sErr != nil {
			return ChildResult{}, sErr
		}
	}

	st, err := c.rt.GetRunState(c.ctx, childID)
	if err != nil {
		return ChildResult{}, err
	}
	switch {
	case st.Status == "COMPLETED":
		if !settled {
			c.settleSubflow(key, name, childID, CheckpointCompleted, "")
		}
		return ChildResult{RunID: childID, Status: st.Status, Result: st.Result}, nil
	case st.Terminal(): // FAILED or CANCELLED
		if !settled {
			c.settleSubflow(key, name, childID, CheckpointFailed,
				fmt.Sprintf("sub-flow %s: %s", st.Status, st.Message))
		}
		return ChildResult{RunID: childID, Status: st.Status},
			Permanent(fmt.Errorf("sub-flow %q %s: %s", name, st.Status, st.Message))
	default:
		return ChildResult{RunID: childID, Status: st.Status},
			Suspend(time.Now().Add(30*time.Second), "waiting for sub-flow "+name)
	}
}

// settleSubflow closes the checkpoint that RunDeploymentAndWait opened when it
// triggered the child, so a lane the parent has stopped waiting on stops
// reading RUNNING in the console once the child lands.
//
// It records a display fact, not a memo of the child's result: Result stays the
// child run id so replay still recovers it, and the caller re-reads the child's
// state on every replay rather than trusting this status. Started is left unset
// on purpose — the engine emits a task span and metric only for a terminal
// checkpoint carrying one, and the trigger's start time does not survive the
// parent's suspension, so there is no honest duration to report. A save that
// fails costs only the displayed status, which is not worth failing an
// otherwise finished wait over.
func (c *Context) settleSubflow(key, name, childID string, status CheckpointStatus, msg string) {
	idRaw, err := json.Marshal(childID)
	if err != nil {
		return
	}
	ended := time.Now().UTC()
	_ = c.rt.SaveCheckpoint(c.ctx, Checkpoint{
		Key: key, Name: "subflow:" + name, Status: status, Result: idRaw,
		Message: msg, Ended: &ended,
	})
}

// TriggerOption customises RunDeployment.
type TriggerOption func(*TriggerOptions)

// TriggerPriority overrides the deployment's default priority.
func TriggerPriority(p int) TriggerOption { return func(o *TriggerOptions) { o.Priority = p } }

// TriggerQueue overrides the deployment's work queue.
func TriggerQueue(q string) TriggerOption { return func(o *TriggerOptions) { o.WorkQueue = q } }

// TriggerDelay schedules the run in the future.
func TriggerDelay(d time.Duration) TriggerOption { return func(o *TriggerOptions) { o.Delay = d } }

// TriggerTags adds tags to the new run.
func TriggerTags(t ...string) TriggerOption { return func(o *TriggerOptions) { o.Tags = t } }

// TriggerIdempotencyKey makes a bare RunDeployment safe to replay: a second
// trigger carrying the same key returns the run the first one created instead
// of starting another. RunDeploymentAndWait sets one for itself; a flow that
// fans out with RunDeployment and waits for the group needs to set its own.
func TriggerIdempotencyKey(k string) TriggerOption {
	return func(o *TriggerOptions) { o.IdempotencyKey = k }
}
