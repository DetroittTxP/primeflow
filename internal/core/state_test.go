package core

import "testing"

func TestTransitionRules(t *testing.T) {
	allowed := [][2]StateType{
		{StateScheduled, StatePending},
		{StatePending, StateRunning},
		{StateRunning, StateCompleted},
		{StateRunning, StateFailed},
		{StateRunning, StateScheduled}, // retry or durable suspend
		{StateRunning, StateCrashed},
		{StateCrashed, StateScheduled},
		{StateFailed, StateScheduled}, // manual retry
		{StateRunning, StateCancelling},
		{StateCancelling, StateCancelled},
	}
	for _, c := range allowed {
		if !CanTransition(c[0], c[1]) {
			t.Errorf("%s -> %s should be allowed", c[0], c[1])
		}
	}

	forbidden := [][2]StateType{
		{StateCompleted, StateRunning},
		{StateCancelled, StateRunning},
		{StateScheduled, StateCompleted},
		{StateFailed, StateCompleted},
	}
	for _, c := range forbidden {
		if CanTransition(c[0], c[1]) {
			t.Errorf("%s -> %s should be rejected", c[0], c[1])
		}
	}
}

func TestTerminalAndFinished(t *testing.T) {
	for _, s := range []StateType{StateCompleted, StateFailed, StateCancelled} {
		if !s.IsTerminal() {
			t.Errorf("%s should be terminal", s)
		}
	}
	// A crashed run is finished (it holds no worker) but not terminal, because
	// the janitor may still resume it.
	if StateCrashed.IsTerminal() {
		t.Error("CRASHED must not be terminal")
	}
	if !StateCrashed.IsFinished() {
		t.Error("CRASHED must count as finished")
	}
	if StateRunning.IsFinished() {
		t.Error("RUNNING must not count as finished")
	}
}

func TestStateValidity(t *testing.T) {
	if !StateRunning.Valid() {
		t.Error("RUNNING should be valid")
	}
	if StateType("NONSENSE").Valid() {
		t.Error("unknown state should be invalid")
	}
	if len(AllStates()) != 9 {
		t.Errorf("expected 9 states, got %d", len(AllStates()))
	}
}
