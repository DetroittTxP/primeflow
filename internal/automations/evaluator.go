// Package automations turns events into actions.
//
// The evaluator reads the event log forward from a cursor rather than
// subscribing to the bus, so a restart or a dropped message costs nothing: it
// simply resumes where it left off. Like the scheduler it is a leader-elected
// singleton, because firing the same rule from three replicas would trigger
// three remediations.
package automations

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/events"
	"github.com/primex/primeflow/internal/store"
)

// Config tunes the evaluator.
type Config struct {
	Holder    string
	Interval  time.Duration
	LeaderTTL time.Duration
	BatchSize int
	// HTTPTimeout bounds webhook actions.
	HTTPTimeout time.Duration
}

func (c *Config) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = 3 * time.Second
	}
	if c.LeaderTTL <= 0 {
		c.LeaderTTL = 30 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 200
	}
	if c.HTTPTimeout <= 0 {
		c.HTTPTimeout = 10 * time.Second
	}
}

// Evaluator matches events against rules and performs their actions.
type Evaluator struct {
	store  store.Store
	events *events.Emitter
	log    *slog.Logger
	cfg    Config
	http   *http.Client
	cursor int64
}

// New builds an evaluator.
func New(s store.Store, em *events.Emitter, log *slog.Logger, cfg Config) *Evaluator {
	cfg.applyDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Evaluator{
		store: s, events: em, log: log, cfg: cfg,
		http: &http.Client{Timeout: cfg.HTTPTimeout},
	}
}

// Run blocks until ctx is cancelled.
func (e *Evaluator) Run(ctx context.Context) error {
	// Start at the head of the log: a newly started evaluator should react to
	// what happens next, not replay a month of history as if it were live.
	if seq, err := e.store.MaxEventSeq(ctx); err == nil {
		e.cursor = seq
	}
	t := time.NewTicker(e.cfg.Interval)
	defer t.Stop()
	e.log.Info("automation evaluator starting", "cursor", e.cursor)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			lead, err := e.store.AcquireLeadership(ctx, "automations", e.cfg.Holder, e.cfg.LeaderTTL)
			if err != nil || !lead {
				continue
			}
			e.tick(ctx)
		}
	}
}

func (e *Evaluator) tick(ctx context.Context) {
	evs, err := e.store.ListEventsAfter(ctx, e.cursor, e.cfg.BatchSize)
	if err != nil {
		e.log.Warn("read events failed", "err", err)
		return
	}
	if len(evs) == 0 {
		return
	}
	rules, err := e.store.ListAutomations(ctx)
	if err != nil {
		e.log.Warn("list automations failed", "err", err)
		return
	}
	for _, ev := range evs {
		for i := range rules {
			r := rules[i]
			if !r.Enabled || !matches(r, ev) {
				continue
			}
			if r.Threshold > 1 {
				since := ev.Occurred.Add(-r.Window)
				if r.Window <= 0 {
					since = ev.Occurred.Add(-time.Hour)
				}
				n, err := e.store.CountEvents(ctx, r.MatchEvent, ev.ResourceType, since)
				if err != nil || n < r.Threshold {
					continue
				}
				// Debounce: do not fire again for the same burst.
				if r.LastFiredAt != nil && r.LastFiredAt.After(since) {
					continue
				}
			}
			e.fire(ctx, r, ev)
		}
		e.cursor = ev.Seq
	}
}

// matches decides whether an event satisfies a rule's trigger.
func matches(r core.Automation, ev core.Event) bool {
	if r.MatchEvent != "" {
		if strings.HasSuffix(r.MatchEvent, "*") {
			if !strings.HasPrefix(ev.Name, strings.TrimSuffix(r.MatchEvent, "*")) {
				return false
			}
		} else if ev.Name != r.MatchEvent {
			return false
		}
	}
	if r.MatchDeployment == "" && r.MatchFlow == "" && r.MatchWorkQueue == "" && r.MatchTag == "" {
		return true
	}
	var p events.FlowRunPayload
	if json.Unmarshal(ev.Payload, &p) != nil {
		return false
	}
	if r.MatchDeployment != "" && p.DeploymentID != r.MatchDeployment {
		return false
	}
	if r.MatchFlow != "" && p.FlowName != r.MatchFlow {
		return false
	}
	if r.MatchWorkQueue != "" && p.WorkQueue != r.MatchWorkQueue {
		return false
	}
	if r.MatchTag != "" {
		found := false
		for _, t := range p.Tags {
			if t == r.MatchTag {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// runDeploymentConfig is the action_config shape for ActionRunDeployment.
type runDeploymentConfig struct {
	Deployment string          `json:"deployment"`
	Parameters json.RawMessage `json:"parameters,omitempty"`
	Priority   int             `json:"priority,omitempty"`
	WorkQueue  string          `json:"work_queue,omitempty"`
	DelayMS    int64           `json:"delay_ms,omitempty"`
	// PassEvent includes the triggering event in the new run's parameters
	// under "_event", so a remediation flow can see what set it off.
	PassEvent bool `json:"pass_event,omitempty"`
}

type setPriorityConfig struct {
	Priority int  `json:"priority"`
	ToFront  bool `json:"to_front,omitempty"`
}

type queueConfig struct {
	Queue string `json:"queue"`
}

type webhookConfig struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// fire performs a rule's action.
func (e *Evaluator) fire(ctx context.Context, r core.Automation, ev core.Event) {
	e.log.Info("automation fired", "rule", r.Name, "action", r.Action, "event", ev.Name)
	var err error
	switch r.Action {
	case core.ActionRunDeployment:
		err = e.actRunDeployment(ctx, r, ev)
	case core.ActionCancelRun:
		err = e.actCancelRun(ctx, ev)
	case core.ActionSetPriority:
		err = e.actSetPriority(ctx, r, ev)
	case core.ActionPauseQueue:
		err = e.actQueuePause(ctx, r, ev, true)
	case core.ActionResumeQueue:
		err = e.actQueuePause(ctx, r, ev, false)
	case core.ActionWebhook:
		err = e.actWebhook(ctx, r, ev)
	default:
		err = fmt.Errorf("unknown action %q", r.Action)
	}
	if err != nil {
		e.log.Warn("automation action failed", "rule", r.Name, "err", err)
		return
	}
	_ = e.store.TouchAutomation(ctx, r.ID, time.Now().UTC())
	payload, _ := json.Marshal(map[string]any{
		"rule": r.Name, "action": r.Action, "trigger_event": ev.Name, "trigger_resource": ev.ResourceID,
	})
	e.events.Emit(ctx, "automation.fired", "automation", r.ID, payload)
}

func (e *Evaluator) actRunDeployment(ctx context.Context, r core.Automation, ev core.Event) error {
	var c runDeploymentConfig
	if err := json.Unmarshal(r.ActionConfig, &c); err != nil {
		return fmt.Errorf("action config: %w", err)
	}
	if c.Deployment == "" {
		return fmt.Errorf("action config needs a deployment name")
	}
	d, err := e.store.GetDeploymentByName(ctx, c.Deployment)
	if err != nil {
		return err
	}
	params := c.Parameters
	if len(params) == 0 {
		params = d.Parameters
	}
	if c.PassEvent {
		merged := map[string]json.RawMessage{}
		if len(params) > 0 {
			_ = json.Unmarshal(params, &merged)
		}
		merged["_event"] = ev.Payload
		if b, err := json.Marshal(merged); err == nil {
			params = b
		}
	}
	priority := d.Priority
	if c.Priority > 0 {
		priority = c.Priority
	}
	queue := d.WorkQueue
	if c.WorkQueue != "" {
		queue = c.WorkQueue
	}
	run, err := e.store.CreateFlowRun(ctx, store.CreateRunInput{
		FlowName:     d.FlowName,
		DeploymentID: &d.ID,
		Parameters:   params,
		WorkQueue:    queue,
		Priority:     priority,
		ScheduledAt:  time.Now().UTC().Add(time.Duration(c.DelayMS) * time.Millisecond),
		Retries:      d.Retries,
		RetryDelay:   d.RetryDelay,
		Timeout:      d.Timeout,
		Tags:         append(append([]string{}, d.Tags...), "automation:"+r.Name),
	})
	if err != nil {
		return err
	}
	e.events.FlowRunStateChanged(ctx, run)
	e.events.WorkAvailable(ctx, run.WorkQueue)
	return nil
}

func (e *Evaluator) actCancelRun(ctx context.Context, ev core.Event) error {
	if ev.ResourceType != "flow-run" {
		return fmt.Errorf("cancel action needs a flow-run event, got %q", ev.ResourceType)
	}
	run, err := e.store.RequestCancel(ctx, ev.ResourceID)
	if err != nil {
		return err
	}
	e.events.Cancel(ctx, run.ID)
	e.events.FlowRunStateChanged(ctx, run)
	return nil
}

func (e *Evaluator) actSetPriority(ctx context.Context, r core.Automation, ev core.Event) error {
	var c setPriorityConfig
	if err := json.Unmarshal(r.ActionConfig, &c); err != nil {
		return fmt.Errorf("action config: %w", err)
	}
	if ev.ResourceType != "flow-run" {
		return fmt.Errorf("set-priority needs a flow-run event")
	}
	if _, err := e.store.SetRunPriority(ctx, ev.ResourceID, c.Priority); err != nil {
		return err
	}
	if c.ToFront {
		if _, err := e.store.MoveRunToFront(ctx, ev.ResourceID); err != nil {
			return err
		}
	}
	return nil
}

func (e *Evaluator) actQueuePause(ctx context.Context, r core.Automation, ev core.Event, paused bool) error {
	var c queueConfig
	_ = json.Unmarshal(r.ActionConfig, &c)
	queue := c.Queue
	if queue == "" {
		var p events.FlowRunPayload
		if json.Unmarshal(ev.Payload, &p) == nil {
			queue = p.WorkQueue
		}
	}
	if queue == "" {
		return fmt.Errorf("no queue to act on")
	}
	return e.store.SetQueuePaused(ctx, queue, paused)
}

func (e *Evaluator) actWebhook(ctx context.Context, r core.Automation, ev core.Event) error {
	var c webhookConfig
	if err := json.Unmarshal(r.ActionConfig, &c); err != nil {
		return fmt.Errorf("action config: %w", err)
	}
	if c.URL == "" {
		return fmt.Errorf("webhook action needs a url")
	}
	method := c.Method
	if method == "" {
		method = http.MethodPost
	}
	body, _ := json.Marshal(map[string]any{
		"rule":  r.Name,
		"event": ev,
	})
	req, err := http.NewRequestWithContext(ctx, method, c.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range c.Headers {
		req.Header.Set(k, v)
	}
	resp, err := e.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned %s", resp.Status)
	}
	return nil
}
