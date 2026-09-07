package core

import (
	"fmt"
	"time"
)

// StateType is the coarse lifecycle phase of a flow run or task run.
// The set mirrors Prefect's canonical state types so operators moving from
// Prefect find familiar semantics, but the transition rules below are ours.
type StateType string

const (
	// StateScheduled means the run is known to the system and will start at
	// (or after) its scheduled time. Runs waiting in a work queue and runs
	// sleeping through a durable pause both live here.
	StateScheduled StateType = "SCHEDULED"
	// StatePending means a worker has claimed the run and is preparing it.
	StatePending StateType = "PENDING"
	// StateRunning means user code is executing under an active lease.
	StateRunning StateType = "RUNNING"
	// StateCompleted is terminal success.
	StateCompleted StateType = "COMPLETED"
	// StateFailed is terminal failure after retries were exhausted.
	StateFailed StateType = "FAILED"
	// StateCrashed means the worker died or the lease expired mid-run.
	// Crashed runs are eligible for automatic rescheduling.
	StateCrashed StateType = "CRASHED"
	// StateCancelling means cancellation was requested; the worker is winding down.
	StateCancelling StateType = "CANCELLING"
	// StateCancelled is terminal cancellation.
	StateCancelled StateType = "CANCELLED"
	// StatePaused means the run is suspended awaiting an explicit resume.
	StatePaused StateType = "PAUSED"
)

// AllStates lists every state type in lifecycle order.
func AllStates() []StateType {
	return []StateType{
		StateScheduled, StatePending, StateRunning, StateCompleted,
		StateFailed, StateCrashed, StateCancelling, StateCancelled, StatePaused,
	}
}

// IsTerminal reports whether no further transition is expected without
// operator intervention.
func (s StateType) IsTerminal() bool {
	switch s {
	case StateCompleted, StateFailed, StateCancelled:
		return true
	}
	return false
}

// IsFinished reports whether the run is no longer occupying a worker slot.
// Crashed runs are finished but not terminal: the janitor may retry them.
func (s StateType) IsFinished() bool {
	return s.IsTerminal() || s == StateCrashed
}

// Valid reports whether s is a known state type.
func (s StateType) Valid() bool {
	for _, k := range AllStates() {
		if k == s {
			return true
		}
	}
	return false
}

// allowedTransitions is the orchestration rule table. A transition that is not
// listed is rejected by the server, which keeps a buggy or malicious worker
// from writing nonsense into the run history.
var allowedTransitions = map[StateType][]StateType{
	StateScheduled:  {StatePending, StateRunning, StateCancelling, StateCancelled, StatePaused, StateScheduled},
	StatePending:    {StateRunning, StateScheduled, StateCrashed, StateCancelling, StateCancelled, StateFailed},
	StateRunning:    {StateCompleted, StateFailed, StateCrashed, StateCancelling, StateCancelled, StateScheduled, StatePaused},
	StatePaused:     {StateScheduled, StateRunning, StateCancelled, StateCancelling},
	StateCancelling: {StateCancelled, StateCompleted, StateFailed, StateCrashed},
	StateCrashed:    {StateScheduled, StateFailed, StateCancelled},
	StateCompleted:  {StateScheduled}, // manual re-run
	StateFailed:     {StateScheduled}, // manual retry
	StateCancelled:  {StateScheduled}, // manual re-run
}

// CanTransition reports whether from -> to is permitted.
func CanTransition(from, to StateType) bool {
	for _, k := range allowedTransitions[from] {
		if k == to {
			return true
		}
	}
	return false
}

// ErrInvalidTransition is returned when a state change violates the rule table.
type ErrInvalidTransition struct {
	From StateType
	To   StateType
}

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("invalid state transition %s -> %s", e.From, e.To)
}

// State is a recorded state observation with the detail an operator needs to
// understand why the run moved.
type State struct {
	Type      StateType `json:"type"`
	Name      string    `json:"name,omitempty"`    // human label, e.g. "AwaitingRetry"
	Message   string    `json:"message,omitempty"` // failure text or reason
	Timestamp time.Time `json:"timestamp"`
}

// NewState builds a state observation stamped with the current time.
func NewState(t StateType, name, msg string) State {
	if name == "" {
		name = string(t)
	}
	return State{Type: t, Name: name, Message: msg, Timestamp: time.Now().UTC()}
}
