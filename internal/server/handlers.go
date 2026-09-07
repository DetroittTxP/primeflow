package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/primex/primeflow/internal/bus"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/metrics"
	"github.com/primex/primeflow/internal/store"
)

// ----------------------------------------------------------- health etc ---

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if _, err := s.store.ListWorkQueues(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]any{"status": "degraded", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": time.Now().UTC()})
}

func (s *Server) listFlows(w http.ResponseWriter, r *http.Request) {
	fs, err := s.store.ListFlows(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, fs)
}

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	ws, err := s.store.ListWorkers(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	type row struct {
		core.WorkerInfo
		Online bool `json:"online"`
	}
	now := time.Now().UTC()
	out := make([]row, len(ws))
	for i, wk := range ws {
		out[i] = row{WorkerInfo: wk, Online: wk.Online(now, 90*time.Second)}
	}
	writeJSON(w, http.StatusOK, out)
}

// summary powers the dashboard header: one round trip instead of six.
func (s *Server) summary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	counts := map[string]int{}
	for _, st := range core.AllStates() {
		n, err := s.store.CountFlowRuns(ctx, store.FlowRunFilter{States: []core.StateType{st}})
		if err != nil {
			fail(w, err)
			return
		}
		counts[string(st)] = n
	}
	queues, err := s.store.QueueStats(ctx)
	if err != nil {
		fail(w, err)
		return
	}
	workers, err := s.store.ListWorkers(ctx)
	if err != nil {
		fail(w, err)
		return
	}
	now := time.Now().UTC()
	online := 0
	for _, wk := range workers {
		if wk.Online(now, 90*time.Second) {
			online++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"states":         counts,
		"queues":         queues,
		"workers_online": online,
		"workers_total":  len(workers),
	})
}

// ---------------------------------------------------------- deployments ---

// deploymentBody is the wire shape for creating a deployment. Durations are
// strings ("30s", "5m") because that is what a human writes in a config file.
type deploymentBody struct {
	Name         string          `json:"name"`
	FlowName     string          `json:"flow_name"`
	Description  string          `json:"description,omitempty"`
	Parameters   json.RawMessage `json:"parameters,omitempty"`
	WorkQueue    string          `json:"work_queue,omitempty"`
	Priority     int             `json:"priority,omitempty"`
	ScheduleKind string          `json:"schedule_kind,omitempty"`
	Schedule     string          `json:"schedule,omitempty"`
	Timezone     string          `json:"timezone,omitempty"`
	Tags         []string        `json:"tags,omitempty"`
	Retries      int             `json:"retries,omitempty"`
	RetryDelay   string          `json:"retry_delay,omitempty"`
	Timeout      string          `json:"timeout,omitempty"`
	CatchUp      bool            `json:"catchup,omitempty"`
	Paused       bool            `json:"paused,omitempty"`
}

func parseDur(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

func (s *Server) listDeployments(w http.ResponseWriter, r *http.Request) {
	ds, err := s.store.ListDeployments(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ds)
}

func (s *Server) upsertDeployment(w http.ResponseWriter, r *http.Request) {
	var b deploymentBody
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if b.Name == "" || b.FlowName == "" {
		writeErr(w, http.StatusBadRequest, errors.New("name and flow_name are required"))
		return
	}
	retryDelay, err := parseDur(b.RetryDelay)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("retry_delay: %w", err))
		return
	}
	timeout, err := parseDur(b.Timeout)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("timeout: %w", err))
		return
	}
	kind := core.ScheduleKind(b.ScheduleKind)
	if kind != core.ScheduleNone && kind != core.ScheduleCron && kind != core.ScheduleInterval {
		writeErr(w, http.StatusBadRequest,
			fmt.Errorf("schedule_kind must be empty, %q or %q", core.ScheduleCron, core.ScheduleInterval))
		return
	}
	priority := b.Priority
	if priority == 0 {
		priority = core.PriorityNormal
	}

	d := &core.Deployment{
		ID: newID(), Name: b.Name, FlowName: b.FlowName, Description: b.Description,
		Parameters: b.Parameters, WorkQueue: b.WorkQueue, Priority: priority,
		ScheduleKind: kind, Schedule: b.Schedule, Timezone: b.Timezone, Tags: b.Tags,
		Retries: b.Retries, RetryDelay: retryDelay, Timeout: timeout,
		CatchUp: b.CatchUp, Paused: b.Paused,
	}
	// Validate the schedule now rather than letting the scheduler log about it
	// every ten seconds forever.
	if kind != core.ScheduleNone {
		if _, err := validateSchedule(*d); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
	}
	if err := s.store.UpsertDeployment(r.Context(), d); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) getDeployment(w http.ResponseWriter, r *http.Request) {
	d, err := s.store.GetDeployment(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

func (s *Server) deleteDeployment(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteDeployment(r.Context(), r.PathValue("id")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) pauseDeployment(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := s.store.SetDeploymentPaused(r.Context(), r.PathValue("id"), paused); err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"paused": paused})
	}
}

// triggerBody customises a manual or webhook-driven run.
type triggerBody struct {
	Parameters json.RawMessage `json:"parameters,omitempty"`
	Priority   int             `json:"priority,omitempty"`
	WorkQueue  string          `json:"work_queue,omitempty"`
	Delay      string          `json:"delay,omitempty"`
	Tags       []string        `json:"tags,omitempty"`
	Name       string          `json:"name,omitempty"`
}

func (s *Server) runDeployment(w http.ResponseWriter, r *http.Request) {
	d, err := s.store.GetDeployment(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	var b triggerBody
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	run, err := s.triggerDeployment(r, d, b)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, run)
}

func (s *Server) triggerDeployment(r *http.Request, d *core.Deployment, b triggerBody) (*core.FlowRun, error) {
	delay, err := parseDur(b.Delay)
	if err != nil {
		return nil, err
	}
	params := b.Parameters
	if len(params) == 0 {
		params = d.Parameters
	}
	priority := d.Priority
	if b.Priority > 0 {
		priority = b.Priority
	}
	queue := d.WorkQueue
	if b.WorkQueue != "" {
		queue = b.WorkQueue
	}
	run, err := s.store.CreateFlowRun(r.Context(), store.CreateRunInput{
		Name: b.Name, FlowName: d.FlowName, DeploymentID: &d.ID, Parameters: params,
		WorkQueue: queue, Priority: priority,
		ScheduledAt: time.Now().UTC().Add(delay),
		Retries:     d.Retries, RetryDelay: d.RetryDelay, Timeout: d.Timeout,
		Tags: append(append([]string{}, d.Tags...), b.Tags...),
	})
	if err != nil {
		return nil, err
	}
	s.events.FlowRunStateChanged(r.Context(), run)
	s.events.WorkAvailable(r.Context(), run.WorkQueue)
	return run, nil
}

// webhook triggers a deployment by name from an external system. The request
// body, whatever its shape, becomes the run's parameters — which is what makes
// this useful for wiring vCenter or a billing system straight into a flow.
func (s *Server) webhook(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("deployment")
	d, err := s.store.GetDeploymentByName(r.Context(), name)
	if err != nil {
		fail(w, err)
		return
	}
	var raw json.RawMessage
	dec := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := dec.Decode(&raw); err != nil {
		raw = nil
	}
	body := triggerBody{Parameters: raw, Tags: []string{"webhook"}}
	if p := intParam(r, "priority", 0); p > 0 {
		body.Priority = p
	}
	run, err := s.triggerDeployment(r, d, body)
	if err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "webhook.received", "deployment", d.ID, raw)
	writeJSON(w, http.StatusAccepted, map[string]string{"run_id": run.ID, "run_name": run.Name})
}

// ----------------------------------------------------------------- runs ---

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	f := store.FlowRunFilter{
		WorkQueues:   csvParam(r, "queue"),
		FlowNames:    csvParam(r, "flow"),
		DeploymentID: r.URL.Query().Get("deployment_id"),
		Tag:          r.URL.Query().Get("tag"),
		Search:       r.URL.Query().Get("search"),
		Limit:        intParam(r, "limit", 50),
		Offset:       intParam(r, "offset", 0),
	}
	for _, st := range csvParam(r, "state") {
		t := core.StateType(st)
		if !t.Valid() {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("unknown state %q", st))
			return
		}
		f.States = append(f.States, t)
	}
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("since: %w", err))
			return
		}
		f.Since = &t
	}
	runs, err := s.store.ListFlowRuns(r.Context(), f)
	if err != nil {
		fail(w, err)
		return
	}
	total, err := s.store.CountFlowRuns(r.Context(), f)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"total": total, "runs": runs})
}

// createRunBody starts an ad-hoc run of a registered flow, with no deployment.
type createRunBody struct {
	FlowName   string          `json:"flow_name"`
	Name       string          `json:"name,omitempty"`
	Parameters json.RawMessage `json:"parameters,omitempty"`
	WorkQueue  string          `json:"work_queue,omitempty"`
	Priority   int             `json:"priority,omitempty"`
	Delay      string          `json:"delay,omitempty"`
	Retries    int             `json:"retries,omitempty"`
	RetryDelay string          `json:"retry_delay,omitempty"`
	Timeout    string          `json:"timeout,omitempty"`
	Tags       []string        `json:"tags,omitempty"`
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	var b createRunBody
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if b.FlowName == "" {
		writeErr(w, http.StatusBadRequest, errors.New("flow_name is required"))
		return
	}
	delay, err := parseDur(b.Delay)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("delay: %w", err))
		return
	}
	retryDelay, err := parseDur(b.RetryDelay)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("retry_delay: %w", err))
		return
	}
	timeout, err := parseDur(b.Timeout)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("timeout: %w", err))
		return
	}
	priority := b.Priority
	if priority == 0 {
		priority = core.PriorityNormal
	}
	run, err := s.store.CreateFlowRun(r.Context(), store.CreateRunInput{
		Name: b.Name, FlowName: b.FlowName, Parameters: b.Parameters,
		WorkQueue: b.WorkQueue, Priority: priority,
		ScheduledAt: time.Now().UTC().Add(delay),
		Retries:     b.Retries, RetryDelay: retryDelay, Timeout: timeout, Tags: b.Tags,
	})
	if err != nil {
		fail(w, err)
		return
	}
	s.events.FlowRunStateChanged(r.Context(), run)
	s.events.WorkAvailable(r.Context(), run.WorkQueue)
	writeJSON(w, http.StatusAccepted, run)
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.GetFlowRun(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) runTasks(w http.ResponseWriter, r *http.Request) {
	ts, err := s.store.ListTaskRuns(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ts)
}

func (s *Server) runLogs(w http.ResponseWriter, r *http.Request) {
	after := int64(intParam(r, "after", 0))
	logs, err := s.store.ListLogs(r.Context(), r.PathValue("id"), after, intParam(r, "limit", 500))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, logs)
}

func (s *Server) runArtifacts(w http.ResponseWriter, r *http.Request) {
	as, err := s.store.ListArtifacts(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, as)
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.RequestCancel(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	// Tell whichever worker holds it to stop now rather than at its next poll.
	s.events.Cancel(r.Context(), run.ID)
	s.events.FlowRunStateChanged(r.Context(), run)
	writeJSON(w, http.StatusOK, run)
}

// retryRun puts a finished run back on the queue immediately. Completed task
// checkpoints survive, so the retry resumes rather than repeating work.
func (s *Server) retryRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.RescheduleRun(r.Context(), r.PathValue("id"), time.Now().UTC())
	if err != nil {
		fail(w, err)
		return
	}
	s.events.FlowRunStateChanged(r.Context(), run)
	s.events.WorkAvailable(r.Context(), run.WorkQueue)
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) rescheduleRun(w http.ResponseWriter, r *http.Request) {
	var b struct {
		At    string `json:"at,omitempty"`
		Delay string `json:"delay,omitempty"`
	}
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	at := time.Now().UTC()
	switch {
	case b.At != "":
		t, err := time.Parse(time.RFC3339, b.At)
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("at: %w", err))
			return
		}
		at = t.UTC()
	case b.Delay != "":
		d, err := parseDur(b.Delay)
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("delay: %w", err))
			return
		}
		at = at.Add(d)
	}
	run, err := s.store.RescheduleRun(r.Context(), r.PathValue("id"), at)
	if err != nil {
		fail(w, err)
		return
	}
	s.events.FlowRunStateChanged(r.Context(), run)
	writeJSON(w, http.StatusOK, run)
}

// ------------------------------------------------- operator queue controls ---

func (s *Server) setPriority(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Priority int `json:"priority"`
	}
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if b.Priority < core.PriorityMin || b.Priority > core.PriorityMax {
		writeErr(w, http.StatusBadRequest,
			fmt.Errorf("priority must be between %d and %d", core.PriorityMin, core.PriorityMax))
		return
	}
	run, err := s.store.SetRunPriority(r.Context(), r.PathValue("id"), b.Priority)
	if err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "flow-run.priority-changed", "flow-run", run.ID,
		mustJSON(map[string]any{"priority": run.Priority}))
	s.events.WorkAvailable(r.Context(), run.WorkQueue)
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) moveFront(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.MoveRunToFront(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "flow-run.moved-to-front", "flow-run", run.ID, nil)
	s.events.WorkAvailable(r.Context(), run.WorkQueue)
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) moveBack(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.MoveRunToBack(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "flow-run.moved-to-back", "flow-run", run.ID, nil)
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) unpin(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.ClearRunPin(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) moveQueue(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Queue string `json:"queue"`
	}
	if err := decode(r, &b); err != nil || b.Queue == "" {
		writeErr(w, http.StatusBadRequest, errors.New("queue is required"))
		return
	}
	run, err := s.store.MoveRunToQueue(r.Context(), r.PathValue("id"), b.Queue)
	if err != nil {
		fail(w, err)
		return
	}
	s.events.WorkAvailable(r.Context(), run.WorkQueue)
	writeJSON(w, http.StatusOK, run)
}

// --------------------------------------------------------------- queues ---

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request) {
	qs, err := s.store.QueueStats(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, qs)
}

func (s *Server) upsertQueue(w http.ResponseWriter, r *http.Request) {
	// Pointer fields: nil means "keep the stored value" so a small PATCH-style
	// body (just concurrency_limit, say) does not reset the autoscaling envelope.
	var b struct {
		Name                 string  `json:"name"`
		Description          *string `json:"description,omitempty"`
		ConcurrencyLimit     *int    `json:"concurrency_limit,omitempty"`
		Paused               *bool   `json:"paused,omitempty"`
		MinWorkers           *int    `json:"min_workers,omitempty"`
		MaxWorkers           *int    `json:"max_workers,omitempty"`
		TargetReadyPerWorker *int    `json:"target_ready_per_worker,omitempty"`
		Owner                *string `json:"owner,omitempty"`
		PoolType             *string `json:"pool_type,omitempty"`
		ClearMaxWorkers      bool    `json:"clear_max_workers,omitempty"`
	}
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if b.Name == "" {
		writeErr(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	q, err := s.store.GetWorkQueue(r.Context(), b.Name)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			fail(w, err)
			return
		}
		q = &core.WorkQueue{Name: b.Name, TargetReadyPerWorker: 5, PoolType: "pull"}
	}
	if b.Description != nil {
		q.Description = *b.Description
	}
	if b.ConcurrencyLimit != nil {
		q.ConcurrencyLimit = b.ConcurrencyLimit
	}
	if b.Paused != nil {
		q.Paused = *b.Paused
	}
	if b.MinWorkers != nil {
		q.MinWorkers = *b.MinWorkers
	}
	if b.ClearMaxWorkers {
		q.MaxWorkers = nil
	} else if b.MaxWorkers != nil {
		q.MaxWorkers = b.MaxWorkers
	}
	if b.TargetReadyPerWorker != nil {
		q.TargetReadyPerWorker = *b.TargetReadyPerWorker
	}
	if b.Owner != nil {
		q.Owner = *b.Owner
	}
	if b.PoolType != nil {
		if *b.PoolType != "pull" && *b.PoolType != "push" {
			writeErr(w, http.StatusBadRequest, errors.New(`pool_type must be "pull" or "push"`))
			return
		}
		q.PoolType = *b.PoolType
	}
	if q.MinWorkers < 0 || (q.MaxWorkers != nil && *q.MaxWorkers < q.MinWorkers) {
		writeErr(w, http.StatusBadRequest, errors.New("min_workers must be >= 0 and <= max_workers"))
		return
	}
	if err := s.store.UpsertWorkQueue(r.Context(), q); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, q)
}

// getQueue returns one work pool plus its live workers and the computed
// autoscaling target — the exact number a KEDA ScaledObject or HPA should scale
// the worker Deployment to.
func (s *Server) getQueue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	stats, err := s.store.QueueStats(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	var qs *store.QueueStat
	for i := range stats {
		if stats[i].Name == name {
			qs = &stats[i]
			break
		}
	}
	if qs == nil {
		writeErr(w, http.StatusNotFound, store.ErrNotFound)
		return
	}
	workers, _ := s.store.ListWorkers(r.Context())
	now := time.Now().UTC()
	type wk struct {
		core.WorkerInfo
		Online bool `json:"online"`
	}
	var mine []wk
	for _, x := range workers {
		for _, q := range x.Queues {
			if q == name {
				mine = append(mine, wk{WorkerInfo: x, Online: x.Online(now, 90*time.Second)})
				break
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"queue":           qs,
		"workers":         mine,
		"desired_workers": metrics.DesiredWorkers(qs.Ready, qs.TargetReadyPerWorker, qs.MinWorkers, qs.MaxWorkers),
	})
}

// queuePending shows the queue in dispatch order — the view an operator needs
// before deciding what to promote.
func (s *Server) queuePending(w http.ResponseWriter, r *http.Request) {
	runs, err := s.store.PendingInQueue(r.Context(), r.PathValue("name"), intParam(r, "limit", 100))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}

func (s *Server) pauseQueue(paused bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if err := s.store.SetQueuePaused(r.Context(), name, paused); err != nil {
			fail(w, err)
			return
		}
		verb := "resumed"
		if paused {
			verb = "paused"
		}
		s.events.Emit(r.Context(), "work-queue."+verb, "work-queue", name, nil)
		if !paused {
			s.events.WorkAvailable(r.Context(), name)
		}
		writeJSON(w, http.StatusOK, map[string]any{"queue": name, "paused": paused})
	}
}

// -------------------------------------------------- events & automations ---

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	evs, err := s.store.ListEvents(r.Context(), intParam(r, "limit", 100))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, evs)
}

type automationBody struct {
	Name            string          `json:"name"`
	Description     string          `json:"description,omitempty"`
	Enabled         *bool           `json:"enabled,omitempty"`
	MatchEvent      string          `json:"match_event,omitempty"`
	MatchDeployment string          `json:"match_deployment,omitempty"`
	MatchFlow       string          `json:"match_flow,omitempty"`
	MatchWorkQueue  string          `json:"match_work_queue,omitempty"`
	MatchTag        string          `json:"match_tag,omitempty"`
	Threshold       int             `json:"threshold,omitempty"`
	Window          string          `json:"window,omitempty"`
	Action          string          `json:"action"`
	ActionConfig    json.RawMessage `json:"action_config,omitempty"`
}

func (s *Server) listAutomations(w http.ResponseWriter, r *http.Request) {
	as, err := s.store.ListAutomations(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, as)
}

func (s *Server) upsertAutomation(w http.ResponseWriter, r *http.Request) {
	var b automationBody
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if b.Name == "" || b.Action == "" {
		writeErr(w, http.StatusBadRequest, errors.New("name and action are required"))
		return
	}
	window, err := parseDur(b.Window)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("window: %w", err))
		return
	}
	enabled := true
	if b.Enabled != nil {
		enabled = *b.Enabled
	}
	a := &core.Automation{
		ID: newID(), Name: b.Name, Description: b.Description, Enabled: enabled,
		MatchEvent: b.MatchEvent, MatchDeployment: b.MatchDeployment, MatchFlow: b.MatchFlow,
		MatchWorkQueue: b.MatchWorkQueue, MatchTag: b.MatchTag,
		Threshold: b.Threshold, Window: window,
		Action: core.ActionKind(b.Action), ActionConfig: b.ActionConfig,
	}
	if err := s.store.UpsertAutomation(r.Context(), a); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) deleteAutomation(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteAutomation(r.Context(), r.PathValue("id")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// stream is a Server-Sent Events feed of the event bus, which is what keeps the
// console live without polling.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, errors.New("streaming unsupported"))
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")

	ctx := r.Context()
	ch := make(chan []byte, 64)

	if s.bus != nil {
		_ = s.bus.Subscribe(ctx, []string{bus.TopicEvents}, func(m bus.Message) {
			select {
			case ch <- m.Payload:
			default: // a slow reader is dropped rather than allowed to back up
			}
		})
	}

	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case payload := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", payload)
			flusher.Flush()
		}
	}
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}
