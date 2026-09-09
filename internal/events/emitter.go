// Package events records what happened and fans it out.
//
// Every state change writes a durable row and publishes a best-effort message.
// The row is what automations evaluate and what the audit view reads; the
// message is only there to make the UI feel live.
package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/DetroittTxP/primeflow/internal/bus"
	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/store"
)

// Emitter writes events to the store and publishes them on the bus.
//
// A nil *Emitter is a valid no-op, and so is one built without a store or a
// bus. A worker that reaches the orchestrator over its API rather than over a
// database connection has neither, and the server records the transition on its
// behalf; making the absent emitter safe is what lets that worker exist.
type Emitter struct {
	store store.Store
	bus   bus.Bus
	log   *slog.Logger
}

// New builds an emitter.
func New(s store.Store, b bus.Bus, log *slog.Logger) *Emitter {
	if log == nil {
		log = slog.Default()
	}
	return &Emitter{store: s, bus: b, log: log}
}

// FlowRunPayload is the event body for flow-run events. Automations match on
// these fields, so it carries every field a rule can filter by.
type FlowRunPayload struct {
	RunID        string          `json:"run_id"`
	RunName      string          `json:"run_name"`
	FlowName     string          `json:"flow_name"`
	DeploymentID string          `json:"deployment_id,omitempty"`
	WorkQueue    string          `json:"work_queue"`
	State        core.StateType  `json:"state"`
	StateMessage string          `json:"state_message,omitempty"`
	Priority     int             `json:"priority"`
	Tags         []string        `json:"tags,omitempty"`
	Attempt      int             `json:"attempt"`
	Result       json.RawMessage `json:"result,omitempty"`
	// WorkerID records who drove the transition. The run row clears worker_id
	// when a run settles, so without this the event log is the only place that
	// could say which worker executed a finished run — and it could not.
	WorkerID string `json:"worker_id,omitempty"`
}

// FlowRunStateChanged records a transition. Event names read
// "flow-run.<State>", e.g. "flow-run.FAILED", which is what automation rules
// match against (a trailing "*" wildcard is supported there).
func (e *Emitter) FlowRunStateChanged(ctx context.Context, r *core.FlowRun) {
	e.FlowRunStateChangedBy(ctx, r, "")
}

// FlowRunStateChangedBy records a transition and attributes it to a worker.
// The server uses it on the worker API, where it knows the caller's identity
// from the credential and the run row may already have released its lease.
func (e *Emitter) FlowRunStateChangedBy(ctx context.Context, r *core.FlowRun, workerID string) {
	if e == nil {
		return
	}
	p := FlowRunPayload{
		RunID: r.ID, RunName: r.Name, FlowName: r.FlowName, WorkQueue: r.WorkQueue,
		State: r.State, StateMessage: r.StateMessage, Priority: r.Priority,
		Tags: r.Tags, Attempt: r.RunCount, Result: r.Result,
	}
	if r.DeploymentID != nil {
		p.DeploymentID = *r.DeploymentID
	}
	switch {
	case workerID != "":
		p.WorkerID = workerID
	case r.WorkerID != nil:
		p.WorkerID = *r.WorkerID
	}
	raw, _ := json.Marshal(p)
	e.Emit(ctx, "flow-run."+string(r.State), "flow-run", r.ID, raw)
}

// Emit writes one event.
func (e *Emitter) Emit(ctx context.Context, name, resourceType, resourceID string, payload json.RawMessage) {
	if e == nil || e.store == nil {
		return
	}
	ev := &core.Event{
		ID: uuid.NewString(), Name: name, ResourceType: resourceType,
		ResourceID: resourceID, Payload: payload, Occurred: time.Now().UTC(),
	}
	if err := e.store.AppendEvent(ctx, ev); err != nil {
		e.log.Warn("append event failed", "event", name, "err", err)
		return
	}
	if e.bus != nil {
		if err := e.bus.Publish(ctx, bus.TopicEvents, ev); err != nil {
			e.log.Debug("publish event failed", "event", name, "err", err)
		}
	}
}

// WorkAvailable nudges workers polling a queue. Losing this only costs latency.
func (e *Emitter) WorkAvailable(ctx context.Context, queue string) {
	if e == nil || e.bus == nil {
		return
	}
	if err := e.bus.Publish(ctx, bus.TopicWork, map[string]string{"queue": queue}); err != nil {
		e.log.Debug("publish work notice failed", "queue", queue, "err", err)
	}
}

// Cancel asks whichever worker holds the run to stop it.
func (e *Emitter) Cancel(ctx context.Context, runID string) {
	if e == nil || e.bus == nil {
		return
	}
	_ = e.bus.Publish(ctx, bus.TopicControl, bus.ControlMessage{Action: "cancel", FlowRunID: runID})
}
