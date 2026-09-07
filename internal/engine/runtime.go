package engine

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/pkg/sdk"
)

// runtimeBridge adapts the store to the narrow interface user code sees.
//
// Log lines are buffered rather than written one at a time: a chatty flow would
// otherwise turn every log call into a network round trip, and logs are the
// one thing a flow is allowed to lose a tail of if the process dies hard.
type runtimeBridge struct {
	eng       *Engine
	runID     string
	flowName  string
	logBuffer []core.LogRecord
	mu        sync.Mutex
}

const logFlushSize = 64

// LoadCheckpoint returns the persisted result of a task key, if any.
func (b *runtimeBridge) LoadCheckpoint(ctx context.Context, key string) (*sdk.Checkpoint, error) {
	tr, err := b.eng.store.GetTaskRun(ctx, b.runID, key)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &sdk.Checkpoint{
		Key:     tr.TaskKey,
		Name:    tr.TaskName,
		Status:  sdk.CheckpointStatus(tr.State),
		Result:  tr.Result,
		Message: tr.StateMessage,
		Attempt: tr.RunCount,
		Retries: tr.Retries,
	}, nil
}

// SaveCheckpoint writes a task checkpoint.
func (b *runtimeBridge) SaveCheckpoint(ctx context.Context, cp sdk.Checkpoint) error {
	tr := &core.TaskRun{
		ID:           uuid.NewString(),
		FlowRunID:    b.runID,
		TaskKey:      cp.Key,
		TaskName:     cp.Name,
		State:        core.StateType(cp.Status),
		StateName:    string(cp.Status),
		StateMessage: truncate(cp.Message, 4000),
		Result:       cp.Result,
		RunCount:     cp.Attempt,
		Retries:      cp.Retries,
		StartedAt:    cp.Started,
		EndedAt:      cp.Ended,
	}
	if cp.CacheKey != "" {
		ck := cp.CacheKey
		tr.CacheKey = &ck
		if cp.CacheTTL > 0 {
			until := time.Now().UTC().Add(cp.CacheTTL)
			tr.CacheUntil = &until
		}
	}
	return b.eng.store.UpsertTaskRun(ctx, tr)
}

// LookupCache finds a completed result under a cross-run cache key.
func (b *runtimeBridge) LookupCache(ctx context.Context, cacheKey string) (json.RawMessage, bool, error) {
	return b.eng.store.FindCachedResult(ctx, cacheKey, time.Now().UTC())
}

// Log buffers a line, flushing when the buffer fills.
func (b *runtimeBridge) Log(e sdk.LogEntry) {
	var fields []byte
	if len(e.Fields) > 0 {
		fields, _ = json.Marshal(e.Fields)
	}
	at := e.At
	if at.IsZero() {
		at = time.Now().UTC()
	}
	rec := core.LogRecord{
		FlowRunID: b.runID,
		Level:     e.Level,
		Message:   truncate(e.Message, 8000),
		Fields:    fields,
		Timestamp: at,
	}

	b.mu.Lock()
	b.logBuffer = append(b.logBuffer, rec)
	full := len(b.logBuffer) >= logFlushSize
	b.mu.Unlock()

	if full {
		b.Flush(context.Background())
	}
}

// Flush writes buffered log lines.
func (b *runtimeBridge) Flush(ctx context.Context) {
	b.mu.Lock()
	batch := b.logBuffer
	b.logBuffer = nil
	b.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	if err := b.eng.store.AppendLogs(ctx, batch); err != nil {
		b.eng.log.Warn("flush logs failed", "run", b.runID, "err", err)
	}
}

// Artifact records a run output.
func (b *runtimeBridge) Artifact(ctx context.Context, a sdk.ArtifactSpec) error {
	kind := core.ArtifactKind(a.Kind)
	if kind == "" {
		kind = core.ArtifactJSON
	}
	runID := b.runID
	return b.eng.store.CreateArtifact(ctx, &core.Artifact{
		ID: uuid.NewString(), FlowRunID: &runID, Key: a.Key, Kind: kind,
		Description: a.Description, Data: a.Data,
	})
}

// TriggerDeployment schedules another deployment from inside a flow.
func (b *runtimeBridge) TriggerDeployment(ctx context.Context, name string, params json.RawMessage, o sdk.TriggerOptions) (string, error) {
	d, err := b.eng.store.GetDeploymentByName(ctx, name)
	if err != nil {
		return "", err
	}
	in := store.CreateRunInput{
		FlowName:     d.FlowName,
		DeploymentID: &d.ID,
		Parameters:   params,
		WorkQueue:    d.WorkQueue,
		Priority:     d.Priority,
		ScheduledAt:  time.Now().UTC().Add(o.Delay),
		Retries:      d.Retries,
		RetryDelay:   d.RetryDelay,
		Timeout:      d.Timeout,
		Tags:         append(append([]string{}, d.Tags...), o.Tags...),
	}
	if len(params) == 0 {
		in.Parameters = d.Parameters
	}
	if o.Priority > 0 {
		in.Priority = o.Priority
	}
	if o.WorkQueue != "" {
		in.WorkQueue = o.WorkQueue
	}
	run, err := b.eng.store.CreateFlowRun(ctx, in)
	if err != nil {
		return "", err
	}
	b.eng.events.FlowRunStateChanged(ctx, run)
	b.eng.events.WorkAvailable(ctx, run.WorkQueue)
	return run.ID, nil
}

var _ sdk.Runtime = (*runtimeBridge)(nil)

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
