// Package scheduler materialises scheduled runs and reclaims abandoned ones.
//
// Both loops are singletons elected through the store, so running three server
// replicas behind a load balancer does not produce three copies of every
// scheduled run. Materialisation is additionally idempotent on
// (deployment, instant), which makes a lost election harmless rather than
// duplicating work.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "time/tzdata" // embed the zone database so containers need no tzdata package

	"github.com/robfig/cron/v3"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/store"
)

// Config tunes the scheduling loops.
type Config struct {
	// Holder identifies this replica in the leader election.
	Holder string
	// Interval is how often schedules are materialised and leases checked.
	Interval time.Duration
	// Lookahead is how far ahead runs are created. Creating them early is what
	// lets an operator see, reprioritise or cancel tomorrow's work today.
	Lookahead time.Duration
	// LeaderTTL is the leadership lease.
	LeaderTTL time.Duration
	// MaxPerCycle bounds how many runs one deployment may materialise per
	// cycle, so a misconfigured every-second cron cannot flood the table.
	MaxPerCycle int
}

func (c *Config) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = 10 * time.Second
	}
	if c.Lookahead <= 0 {
		c.Lookahead = time.Hour
	}
	if c.LeaderTTL <= 0 {
		c.LeaderTTL = 30 * time.Second
	}
	if c.MaxPerCycle <= 0 {
		c.MaxPerCycle = 50
	}
}

// Scheduler runs the materialisation and janitor loops.
type Scheduler struct {
	store  store.Store
	events *events.Emitter
	log    *slog.Logger
	cfg    Config

	lastSweep time.Time // rate-limits the housekeeping sweep
}

// New builds a scheduler.
func New(s store.Store, em *events.Emitter, log *slog.Logger, cfg Config) *Scheduler {
	cfg.applyDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{store: s, events: em, log: log, cfg: cfg}
}

// Run blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	s.log.Info("scheduler starting", "interval", s.cfg.Interval, "lookahead", s.cfg.Lookahead)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			lead, err := s.store.AcquireLeadership(ctx, "scheduler", s.cfg.Holder, s.cfg.LeaderTTL)
			if err != nil {
				s.log.Warn("leader election failed", "err", err)
				continue
			}
			if !lead {
				continue
			}
			s.materialise(ctx)
			s.reclaim(ctx)
			s.sweep(ctx)
		}
	}
}

// sweep is periodic housekeeping the leader runs at most every 30 minutes:
// expired login sessions and an over-long API-key audit trail. Both are
// best-effort — a failure is logged and retried next window.
func (s *Scheduler) sweep(ctx context.Context) {
	if time.Since(s.lastSweep) < 30*time.Minute {
		return
	}
	s.lastSweep = time.Now()
	if n, err := s.store.DeleteExpiredSessions(ctx, time.Now().UTC()); err != nil {
		s.log.Warn("session sweep failed", "err", err)
	} else if n > 0 {
		s.log.Debug("pruned expired sessions", "count", n)
	}
	if n, err := s.store.TrimAPIKeyEvents(ctx, 200); err != nil {
		s.log.Warn("api-key event trim failed", "err", err)
	} else if n > 0 {
		s.log.Debug("trimmed api-key events", "count", n)
	}
	if lr, err := s.store.GetLogRetention(ctx); err == nil && lr.Enabled && lr.MaxAgeHours > 0 {
		cutoff := time.Now().Add(-time.Duration(lr.MaxAgeHours) * time.Hour)
		if n, more, err := s.store.DeleteLogsOlderThan(ctx, cutoff, 100000); err != nil {
			s.log.Warn("log retention sweep failed", "err", err)
		} else if n > 0 {
			s.log.Info("pruned old logs", "count", n, "more_pending", more, "older_than", cutoff)
		}
	}
}

// materialise creates the runs each schedule calls for inside the lookahead.
func (s *Scheduler) materialise(ctx context.Context) {
	deps, err := s.store.ListDeployments(ctx)
	if err != nil {
		s.log.Warn("list deployments failed", "err", err)
		return
	}
	now := time.Now().UTC()
	horizon := now.Add(s.cfg.Lookahead)

	for i := range deps {
		d := deps[i]
		if d.Paused || d.ScheduleKind == core.ScheduleNone || d.Schedule == "" {
			continue
		}
		times, err := NextRuns(d, now, horizon, s.cfg.MaxPerCycle)
		if err != nil {
			s.log.Warn("bad schedule", "deployment", d.Name, "schedule", d.Schedule, "err", err)
			continue
		}
		for _, at := range times {
			in := store.CreateRunInput{
				FlowName:       d.FlowName,
				DeploymentID:   &d.ID,
				Parameters:     d.Parameters,
				WorkQueue:      d.WorkQueue,
				Priority:       d.Priority,
				ScheduledAt:    at,
				Retries:        d.Retries,
				RetryDelay:     d.RetryDelay,
				Timeout:        d.Timeout,
				Tags:           d.Tags,
				IdempotencyKey: fmt.Sprintf("%s@%d", d.ID, at.Unix()),
			}
			run, err := s.store.CreateFlowRun(ctx, in)
			switch {
			case err == nil:
				s.log.Debug("scheduled run", "deployment", d.Name, "at", at)
				s.events.FlowRunStateChanged(ctx, run)
				if !at.After(now) {
					s.events.WorkAvailable(ctx, run.WorkQueue)
				}
			case isConflict(err):
				// Already materialised on a previous cycle. Expected.
			default:
				s.log.Warn("create scheduled run failed", "deployment", d.Name, "err", err)
			}
		}
	}
}

// reclaim crashes runs whose worker went silent, then decides retry or fail.
func (s *Scheduler) reclaim(ctx context.Context) {
	now := time.Now().UTC()
	crashed, err := s.store.ReclaimExpiredLeases(ctx, now)
	if err != nil {
		s.log.Warn("reclaim leases failed", "err", err)
		return
	}
	for i := range crashed {
		r := crashed[i]
		s.log.Warn("run lease expired", "run", r.ID, "flow", r.FlowName, "attempt", r.RunCount)
		s.events.FlowRunStateChanged(ctx, &r)

		// A crash is not the flow's fault, so it gets one extra attempt beyond
		// the configured retry budget before it is called failed.
		if r.RunCount <= r.Retries+1 {
			delay := r.RetryDelay
			if delay <= 0 {
				delay = 10 * time.Second
			}
			at := now.Add(delay)
			updated, err := s.store.SetFlowRunState(ctx, r.ID,
				core.NewState(core.StateScheduled, "AwaitingRetry", "resuming after worker crash"),
				store.StateOpts{ScheduleAt: &at, ClearLease: true, Force: true})
			if err != nil {
				s.log.Warn("requeue crashed run failed", "run", r.ID, "err", err)
				continue
			}
			s.events.FlowRunStateChanged(ctx, updated)
			s.events.WorkAvailable(ctx, updated.WorkQueue)
			continue
		}

		ended := now
		updated, err := s.store.SetFlowRunState(ctx, r.ID,
			core.NewState(core.StateFailed, "Failed", "worker crashed and retry budget is exhausted"),
			store.StateOpts{EndedAt: &ended, ClearLease: true, Force: true})
		if err == nil {
			s.events.FlowRunStateChanged(ctx, updated)
		}
	}
}

// ------------------------------------------------------------ schedules ----

// cronParser accepts both the 5-field standard form and the 6-field form with
// leading seconds, plus descriptors like "@hourly".
var cronParser = cron.NewParser(
	cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// NextRuns returns the instants a deployment should run at in (now, horizon].
//
// For interval schedules the phase is anchored to the deployment's creation
// time, so "every 15 minutes" keeps the same offsets across restarts instead of
// drifting each time the process comes up.
func NextRuns(d core.Deployment, now, horizon time.Time, max int) ([]time.Time, error) {
	loc := time.UTC
	if d.Timezone != "" {
		l, err := time.LoadLocation(d.Timezone)
		if err != nil {
			return nil, fmt.Errorf("unknown timezone %q: %w", d.Timezone, err)
		}
		loc = l
	}

	var out []time.Time
	switch d.ScheduleKind {
	case core.ScheduleCron:
		sched, err := cronParser.Parse(d.Schedule)
		if err != nil {
			return nil, err
		}
		t := now.In(loc)
		for len(out) < max {
			t = sched.Next(t)
			if t.After(horizon) {
				break
			}
			out = append(out, t.UTC())
		}

	case core.ScheduleInterval:
		every, err := time.ParseDuration(d.Schedule)
		if err != nil {
			return nil, fmt.Errorf("interval %q: %w", d.Schedule, err)
		}
		if every < time.Second {
			return nil, fmt.Errorf("interval %q is too short (minimum 1s)", d.Schedule)
		}
		anchor := d.CreatedAt.UTC()
		if anchor.IsZero() {
			anchor = now
		}
		// First tick strictly after now, on the anchor's phase.
		elapsed := now.Sub(anchor)
		n := int64(elapsed / every)
		t := anchor.Add(time.Duration(n+1) * every)
		for len(out) < max && !t.After(horizon) {
			if t.After(now) {
				out = append(out, t.UTC())
			}
			t = t.Add(every)
		}

	default:
		return nil, nil
	}

	// Without catch-up, only the soonest instant is materialised, so downtime
	// produces one run rather than a thundering herd of missed windows.
	if !d.CatchUp && len(out) > 1 {
		out = out[:1]
	}
	return out, nil
}

func isConflict(err error) bool { return errors.Is(err, store.ErrConflict) }
