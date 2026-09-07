package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// TaskOptions configure one checkpointed step.
type TaskOptions struct {
	// Key overrides the deterministic key. Supply one when a task runs inside
	// a loop whose iteration order or count can change between attempts —
	// then the key is derived from the data, not the position.
	Key string
	// Retries is how many extra attempts this task gets on failure.
	Retries int
	// RetryDelay is the base backoff. Attempts back off exponentially with a
	// cap of one minute unless RetryDelayMax says otherwise.
	RetryDelay    time.Duration
	RetryDelayMax time.Duration
	// Timeout bounds a single attempt.
	Timeout time.Duration
	// CacheKey shares a completed result across flow runs. Two runs computing
	// the same cache key execute the work once.
	CacheKey string
	CacheTTL time.Duration
	// Ephemeral skips persistence entirely: the task always executes and its
	// result is never stored. Use for cheap, pure computation where a
	// database round trip would cost more than re-running.
	Ephemeral bool
}

// TaskOption mutates TaskOptions.
type TaskOption func(*TaskOptions)

// TaskKey pins an explicit checkpoint key.
func TaskKey(k string) TaskOption { return func(o *TaskOptions) { o.Key = k } }

// TaskRetries sets the per-task retry budget.
func TaskRetries(n int) TaskOption { return func(o *TaskOptions) { o.Retries = n } }

// TaskRetryDelay sets the base backoff between attempts.
func TaskRetryDelay(d time.Duration) TaskOption { return func(o *TaskOptions) { o.RetryDelay = d } }

// TaskRetryDelayMax caps the exponential backoff.
func TaskRetryDelayMax(d time.Duration) TaskOption {
	return func(o *TaskOptions) { o.RetryDelayMax = d }
}

// TaskTimeout bounds a single attempt.
func TaskTimeout(d time.Duration) TaskOption { return func(o *TaskOptions) { o.Timeout = d } }

// TaskCache shares the result across runs under key for ttl. A zero ttl means
// the cache entry never expires.
func TaskCache(key string, ttl time.Duration) TaskOption {
	return func(o *TaskOptions) { o.CacheKey, o.CacheTTL = key, ttl }
}

// TaskEphemeral disables persistence for this task.
func TaskEphemeral() TaskOption { return func(o *TaskOptions) { o.Ephemeral = true } }

// Task runs fn as a durable checkpoint and returns its typed result.
//
// On the first attempt the function executes and its result is persisted. On
// any later execution of the same flow run — a retry, or a replay after the
// worker crashed — the stored result is returned and fn is not called. That is
// the whole durability contract, and it is why side effects belong inside
// tasks rather than between them.
func Task[T any](c *Context, name string, fn func(*Context) (T, error), opts ...TaskOption) (T, error) {
	var zero T

	o := TaskOptions{RetryDelay: time.Second, RetryDelayMax: time.Minute}
	for _, f := range opts {
		f(&o)
	}
	if o.Timeout < 0 {
		o.Timeout = 0
	}

	// Cancellation is observed at every checkpoint boundary so a cancelled run
	// stops between steps rather than mid-side-effect.
	select {
	case <-c.ctx.Done():
		return zero, fmt.Errorf("%w: %v", ErrCancelled, c.ctx.Err())
	default:
	}

	if o.Ephemeral {
		var n int
		return runAttempts(c, name, "", fn, o, &n)
	}

	key := o.Key
	if key == "" {
		key = c.nextKey(name)
	}

	// 1. Has this exact step already completed in this run?
	if cp, err := c.rt.LoadCheckpoint(c.ctx, key); err != nil {
		return zero, fmt.Errorf("load checkpoint %q: %w", key, err)
	} else if cp != nil && cp.Status == CheckpointCompleted {
		var out T
		if len(cp.Result) > 0 {
			if err := json.Unmarshal(cp.Result, &out); err != nil {
				return zero, fmt.Errorf("replay checkpoint %q: %w", key, err)
			}
		}
		c.rt.Log(LogEntry{
			Level: "DEBUG", TaskKey: key, At: time.Now().UTC(),
			Message: "restored from checkpoint", Fields: map[string]any{"task": name},
		})
		return out, nil
	}

	// 2. Has an equivalent step completed in some other run?
	if o.CacheKey != "" {
		if raw, ok, err := c.rt.LookupCache(c.ctx, o.CacheKey); err == nil && ok {
			var out T
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &out); err == nil {
					_ = c.rt.SaveCheckpoint(c.ctx, Checkpoint{
						Key: key, Name: name, Status: CheckpointCompleted, Result: raw,
						Message: "cache hit", Attempt: 0, CacheKey: o.CacheKey, CacheTTL: o.CacheTTL,
					})
					return out, nil
				}
			}
		}
	}

	started := time.Now().UTC()
	_ = c.rt.SaveCheckpoint(c.ctx, Checkpoint{
		Key: key, Name: name, Status: CheckpointRunning, Retries: o.Retries, Started: &started,
	})

	var attempts int
	out, err := runAttempts(c, name, key, fn, o, &attempts)
	ended := time.Now().UTC()

	if err != nil {
		if s, ok := IsSuspend(err); ok {
			// A suspension is not a failure. Leave the checkpoint open so the
			// step re-enters on resume.
			_ = s
			return zero, err
		}
		_ = c.rt.SaveCheckpoint(c.ctx, Checkpoint{
			Key: key, Name: name, Status: CheckpointFailed, Message: err.Error(),
			Attempt: attempts, Retries: o.Retries, Started: &started, Ended: &ended,
		})
		return zero, err
	}

	raw, mErr := json.Marshal(out)
	if mErr != nil {
		return zero, fmt.Errorf("encode result of task %q: %w", name, mErr)
	}
	if err := c.rt.SaveCheckpoint(c.ctx, Checkpoint{
		Key: key, Name: name, Status: CheckpointCompleted, Result: raw,
		Attempt: attempts, Retries: o.Retries, CacheKey: o.CacheKey, CacheTTL: o.CacheTTL,
		Started: &started, Ended: &ended,
	}); err != nil {
		// The work succeeded but we could not record it. Failing here is the
		// safe choice: silently losing the checkpoint would make a later
		// replay repeat a side effect.
		return zero, fmt.Errorf("persist checkpoint %q: %w", key, err)
	}
	return out, nil
}

// runAttempts executes fn with the retry and timeout policy in o, recording how
// many attempts it took so the checkpoint (and the UI) can show it.
func runAttempts[T any](c *Context, name, key string, fn func(*Context) (T, error), o TaskOptions, attempts *int) (T, error) {
	var zero T
	var lastErr error

	for attempt := 0; attempt <= o.Retries; attempt++ {
		*attempts = attempt + 1
		if attempt > 0 {
			delay := backoff(o.RetryDelay, o.RetryDelayMax, attempt)
			c.rt.Log(LogEntry{
				Level: "WARN", TaskKey: key, At: time.Now().UTC(),
				Message: fmt.Sprintf("task %q failed, retrying in %s", name, delay),
				Fields:  map[string]any{"task": name, "attempt": attempt, "error": lastErr.Error()},
			})
			select {
			case <-time.After(delay):
			case <-c.ctx.Done():
				return zero, fmt.Errorf("%w: %v", ErrCancelled, c.ctx.Err())
			}
		}

		out, err := callOnce(c, fn, o.Timeout)
		if err == nil {
			return out, nil
		}
		if _, ok := IsSuspend(err); ok {
			return zero, err // suspension bypasses retry logic
		}
		if IsPermanent(err) || errors.Is(err, ErrCancelled) {
			return zero, err
		}
		lastErr = err
	}
	return zero, fmt.Errorf("task %q failed after %d attempt(s): %w", name, o.Retries+1, lastErr)
}

// callOnce runs fn once, applying a per-attempt timeout and converting a panic
// into a permanent error so a bug in user code fails the run cleanly instead of
// taking the worker down.
func callOnce[T any](c *Context, fn func(*Context) (T, error), timeout time.Duration) (out T, err error) {
	inner := c
	if timeout > 0 {
		ctx, cancel := context.WithTimeout(c.ctx, timeout)
		defer cancel()
		inner = c.derive(ctx)
	}
	defer func() {
		if r := recover(); r != nil {
			var zero T
			out = zero
			err = Permanent(fmt.Errorf("panic in task: %v", r))
		}
	}()
	return fn(inner)
}

// derive returns a shallow copy of c bound to a different context. The ordinal
// map is shared so key generation stays consistent.
func (c *Context) derive(ctx context.Context) *Context {
	cp := &Context{
		ctx: ctx, rt: c.rt, run: c.run,
		ordinals: c.ordinals, suspendThreshold: c.suspendThreshold,
	}
	return cp
}

// backoff returns base * 2^(attempt-1), capped at max.
func backoff(base, max time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if max <= 0 {
		max = time.Minute
	}
	d := base
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

// Do is Task for a step that returns no value.
func Do(c *Context, name string, fn func(*Context) error, opts ...TaskOption) error {
	_, err := Task(c, name, func(ctx *Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, opts...)
	return err
}

// ------------------------------------------------------------- waiting ------

// Sleep pauses the flow for d.
//
// Short sleeps block in place. Long ones (over the suspend threshold, 30s by
// default) release the worker: the run is rescheduled for the wake time and
// resumes from its checkpoints. A flow can therefore wait hours for a VM to
// finish building without occupying a worker slot the whole time.
func Sleep(c *Context, key string, d time.Duration) error {
	return WaitUntil(c, key, time.Now().UTC().Add(d))
}

// WaitUntil suspends or blocks until t, as Sleep does.
func WaitUntil(c *Context, key string, t time.Time) error {
	if key == "" {
		key = c.nextKey("wait")
	} else {
		key = "wait:" + key
	}
	// The wake time is itself a checkpoint, so a replay does not restart the
	// clock.
	wake, err := Task(c, "wait", func(*Context) (time.Time, error) {
		return t.UTC(), nil
	}, TaskKey(key))
	if err != nil {
		return err
	}

	remaining := time.Until(wake)
	if remaining <= 0 {
		return nil
	}
	if remaining > c.suspendThreshold {
		return SuspendError{Until: wake, Reason: "durable wait"}
	}
	select {
	case <-time.After(remaining):
		return nil
	case <-c.ctx.Done():
		return fmt.Errorf("%w: %v", ErrCancelled, c.ctx.Err())
	}
}

// Suspend releases the worker until t without recording a checkpoint. Use it
// when the wake condition is external, e.g. polling a ticket system.
func Suspend(until time.Time, reason string) error {
	return SuspendError{Until: until.UTC(), Reason: reason}
}
