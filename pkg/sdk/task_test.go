package sdk_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

// fakeRuntime is an in-memory Runtime. It stands in for the engine so the SDK's
// checkpoint semantics can be tested without a database.
type fakeRuntime struct {
	mu        sync.Mutex
	cps       map[string]sdk.Checkpoint
	cache     map[string]json.RawMessage
	runStates map[string]sdk.RunState
	logs      []sdk.LogEntry
	arts      []sdk.ArtifactSpec
	triggers  []sdk.TriggerOptions
	saveNo    int
}

func newFake() *fakeRuntime {
	return &fakeRuntime{
		cps:       map[string]sdk.Checkpoint{},
		cache:     map[string]json.RawMessage{},
		runStates: map[string]sdk.RunState{},
	}
}

func (f *fakeRuntime) LoadCheckpoint(_ context.Context, key string) (*sdk.Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp, ok := f.cps[key]
	if !ok {
		return nil, nil
	}
	return &cp, nil
}

func (f *fakeRuntime) SaveCheckpoint(_ context.Context, cp sdk.Checkpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveNo++
	f.cps[cp.Key] = cp
	if cp.CacheKey != "" && cp.Status == sdk.CheckpointCompleted {
		f.cache[cp.CacheKey] = cp.Result
	}
	return nil
}

func (f *fakeRuntime) LookupCache(_ context.Context, key string) (json.RawMessage, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.cache[key]
	return v, ok, nil
}

func (f *fakeRuntime) Log(e sdk.LogEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, e)
}

func (f *fakeRuntime) Artifact(_ context.Context, a sdk.ArtifactSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.arts = append(f.arts, a)
	return nil
}

func (f *fakeRuntime) TriggerDeployment(_ context.Context, _ string, _ json.RawMessage, o sdk.TriggerOptions) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.triggers = append(f.triggers, o)
	return "child-1", nil
}

func (f *fakeRuntime) GetRunState(_ context.Context, runID string) (sdk.RunState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if st, ok := f.runStates[runID]; ok {
		return st, nil
	}
	return sdk.RunState{Status: "COMPLETED"}, nil
}

func newCtx(t *testing.T, rt sdk.Runtime, params any) *sdk.Context {
	t.Helper()
	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			t.Fatal(err)
		}
		raw = b
	}
	return sdk.NewContext(sdk.WithParams(context.Background(), raw), rt, sdk.RunInfo{RunID: "run-1"})
}

// TestTaskCheckpointReplay is the core durability guarantee: re-running a flow
// against the same checkpoints must not re-execute completed work.
func TestTaskCheckpointReplay(t *testing.T) {
	rt := newFake()
	calls := 0

	flow := func(c *sdk.Context) (int, error) {
		return sdk.Task(c, "expensive", func(*sdk.Context) (int, error) {
			calls++
			return 42, nil
		})
	}

	got, err := flow(newCtx(t, rt, nil))
	if err != nil || got != 42 {
		t.Fatalf("first run: got %d, err %v", got, err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}

	// Second execution simulates a replay after a crash.
	got, err = flow(newCtx(t, rt, nil))
	if err != nil || got != 42 {
		t.Fatalf("replay: got %d, err %v", got, err)
	}
	if calls != 1 {
		t.Fatalf("replay re-executed the task: %d calls", calls)
	}
}

// TestTaskOrdinalKeys checks that unnamed tasks get stable keys derived from
// reach order, which is what makes replay line up.
func TestTaskOrdinalKeys(t *testing.T) {
	rt := newFake()
	calls := map[int]int{}

	flow := func(c *sdk.Context) error {
		for i := 0; i < 3; i++ {
			n := i
			if _, err := sdk.Task(c, "step", func(*sdk.Context) (int, error) {
				calls[n]++
				return n, nil
			}); err != nil {
				return err
			}
		}
		return nil
	}

	if err := flow(newCtx(t, rt, nil)); err != nil {
		t.Fatal(err)
	}
	if err := flow(newCtx(t, rt, nil)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if calls[i] != 1 {
			t.Fatalf("step %d executed %d times, want 1", i, calls[i])
		}
	}
	for _, k := range []string{"step-1", "step-2", "step-3"} {
		if _, ok := rt.cps[k]; !ok {
			t.Fatalf("missing checkpoint %q; have %v", k, keys(rt.cps))
		}
	}
}

func TestTaskRetriesThenSucceeds(t *testing.T) {
	rt := newFake()
	attempts := 0

	got, err := sdk.Task(newCtx(t, rt, nil), "flaky", func(*sdk.Context) (string, error) {
		attempts++
		if attempts < 3 {
			return "", errors.New("transient")
		}
		return "ok", nil
	}, sdk.TaskRetries(4), sdk.TaskRetryDelay(time.Millisecond))
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if got != "ok" || attempts != 3 {
		t.Fatalf("got %q after %d attempts", got, attempts)
	}
}

func TestTaskRetriesExhausted(t *testing.T) {
	rt := newFake()
	attempts := 0

	_, err := sdk.Task(newCtx(t, rt, nil), "always-fails", func(*sdk.Context) (int, error) {
		attempts++
		return 0, errors.New("nope")
	}, sdk.TaskRetries(2), sdk.TaskRetryDelay(time.Millisecond))
	if err == nil {
		t.Fatal("expected failure")
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts (1 + 2 retries), got %d", attempts)
	}
	if cp := rt.cps["always-fails-1"]; cp.Status != sdk.CheckpointFailed {
		t.Fatalf("expected a FAILED checkpoint, got %q", cp.Status)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	rt := newFake()
	attempts := 0

	_, err := sdk.Task(newCtx(t, rt, nil), "bad-input", func(*sdk.Context) (int, error) {
		attempts++
		return 0, sdk.Permanent(errors.New("missing org_name"))
	}, sdk.TaskRetries(5), sdk.TaskRetryDelay(time.Millisecond))
	if err == nil || !sdk.IsPermanent(err) {
		t.Fatalf("expected a permanent error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("permanent error retried: %d attempts", attempts)
	}
}

func TestPanicBecomesPermanentError(t *testing.T) {
	rt := newFake()
	_, err := sdk.Task(newCtx(t, rt, nil), "boom", func(*sdk.Context) (int, error) {
		panic("kaboom")
	}, sdk.TaskRetries(3), sdk.TaskRetryDelay(time.Millisecond))
	if err == nil || !sdk.IsPermanent(err) {
		t.Fatalf("expected a permanent error from the panic, got %v", err)
	}
}

func TestCrossRunCache(t *testing.T) {
	rt := newFake()
	calls := 0
	fn := func(*sdk.Context) (string, error) {
		calls++
		return "urn:org:acme", nil
	}

	// Two *different* runs, sharing a cache key.
	c1 := sdk.NewContext(context.Background(), rt, sdk.RunInfo{RunID: "run-1"})
	c2 := sdk.NewContext(context.Background(), rt, sdk.RunInfo{RunID: "run-2"})

	if _, err := sdk.Task(c1, "lookup", fn, sdk.TaskCache("org:acme", time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, err := sdk.Task(c2, "lookup", fn, sdk.TaskCache("org:acme", time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if got != "urn:org:acme" {
		t.Fatalf("cache miss: got %q", got)
	}
	if calls != 1 {
		t.Fatalf("cache did not prevent re-execution: %d calls", calls)
	}
}

func TestSleepSuspendsWhenLong(t *testing.T) {
	rt := newFake()
	c := newCtx(t, rt, nil)
	c.SetSuspendThreshold(time.Second)

	err := sdk.Sleep(c, "settle", time.Hour)
	s, ok := sdk.IsSuspend(err)
	if !ok {
		t.Fatalf("expected a suspension, got %v", err)
	}
	if time.Until(s.Until) < 59*time.Minute {
		t.Fatalf("wake time too soon: %s", s.Until)
	}

	// On resume the wake time is replayed from the checkpoint rather than
	// restarting the clock.
	c2 := newCtx(t, rt, nil)
	c2.SetSuspendThreshold(time.Second)
	err2 := sdk.Sleep(c2, "settle", time.Hour)
	s2, ok := sdk.IsSuspend(err2)
	if !ok {
		t.Fatalf("expected a suspension on replay, got %v", err2)
	}
	if !s2.Until.Equal(s.Until) {
		t.Fatalf("wake time drifted on replay: %s vs %s", s2.Until, s.Until)
	}
}

func TestSleepBlocksWhenShort(t *testing.T) {
	rt := newFake()
	c := newCtx(t, rt, nil)
	c.SetSuspendThreshold(time.Second)

	start := time.Now()
	if err := sdk.Sleep(c, "brief", 30*time.Millisecond); err != nil {
		t.Fatalf("short sleep should block, not suspend: %v", err)
	}
	if time.Since(start) < 25*time.Millisecond {
		t.Fatal("sleep returned too early")
	}
}

func TestParamsDecode(t *testing.T) {
	type P struct {
		Org string `json:"org"`
		N   int    `json:"n"`
	}
	rt := newFake()
	c := newCtx(t, rt, P{Org: "acme", N: 7})
	p, err := sdk.Params[P](c)
	if err != nil {
		t.Fatal(err)
	}
	if p.Org != "acme" || p.N != 7 {
		t.Fatalf("bad decode: %+v", p)
	}

	// A run with no parameters yields the zero value, not an error.
	empty, err := sdk.Params[P](newCtx(t, rt, nil))
	if err != nil {
		t.Fatalf("empty params should not error: %v", err)
	}
	if empty.Org != "" {
		t.Fatalf("expected zero value, got %+v", empty)
	}
}

func TestEphemeralTaskIsNotPersisted(t *testing.T) {
	rt := newFake()
	calls := 0
	c := newCtx(t, rt, nil)
	for i := 0; i < 2; i++ {
		if _, err := sdk.Task(c, "cheap", func(*sdk.Context) (int, error) {
			calls++
			return 1, nil
		}, sdk.TaskKey("cheap"), sdk.TaskEphemeral()); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("ephemeral task was memoised: %d calls", calls)
	}
	if len(rt.cps) != 0 {
		t.Fatalf("ephemeral task wrote checkpoints: %v", keys(rt.cps))
	}
}

func TestContextCancellationStopsTasks(t *testing.T) {
	rt := newFake()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := sdk.NewContext(ctx, rt, sdk.RunInfo{RunID: "run-1"})

	_, err := sdk.Task(c, "never", func(*sdk.Context) (int, error) {
		t.Fatal("task body ran after cancellation")
		return 0, nil
	})
	if !errors.Is(err, sdk.ErrCancelled) {
		t.Fatalf("expected ErrCancelled, got %v", err)
	}
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
