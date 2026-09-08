// Worker API: the execution path expressed over HTTP.
//
// These routes exist so a worker can run somewhere that reaches the
// orchestrator through its API rather than through a PostgreSQL connection — a
// site VM allowed nothing but outbound 443. They are deliberately thin. Every
// orchestration rule already lives in the store: the transition table, the
// queue pause flag, the concurrency cap, the dispatch ordering. A handler here
// that restated one of those would be a second copy to keep in step, and a
// handler that relaxed one would hand a remote worker authority the local one
// never had.
//
// Three things never come from the request body, because moving the trust
// boundary off the database is the entire point:
//
//   - the caller's worker id, resolved from the credential
//   - StateOpts.Force, refused outright — it belongs to the janitor, and the
//     janitor runs centrally
//   - the instant a cache lookup is measured against, which is server time, so
//     a site with a skewed clock cannot extend a cache entry's life
//
// AppendEvent is deliberately absent and must stay that way. Automations read
// the event log and act on it — run-deployment, cancel-run, set-priority,
// pause-queue. A worker able to write events could forge a failure against
// another site's flow and trip an automation that pauses that site's lane. The
// server emits on the worker's behalf instead, which is a security boundary
// rather than a convenience.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

// workerIDHeader carries the caller's worker identity.
//
// Resolving it from a header is the interim: the operator API's machine
// credential is a single shared admin token, so there is nothing better to bind
// to yet. Pool-scoped worker credentials replace this, and when they do only
// workerIdentity changes — every enforcement point below already reads from it
// rather than from the request body.
const workerIDHeader = "X-PrimeFlow-Worker-ID"

// workerIdentity is who the server believes is calling.
type workerIdentity struct {
	// WorkerID is used for leases and lease renewal. It is never read from a
	// body: a worker must not be able to claim work as another worker.
	WorkerID string
	// Pools bounds which lanes this caller may touch. A nil slice means
	// unrestricted, which is what a shared admin token gets today; a
	// pool-scoped credential will populate it.
	Pools []string
}

// mayUse reports whether this caller is allowed to act on a lane.
func (id workerIdentity) mayUse(queue string) bool {
	if id.Pools == nil {
		return true
	}
	for _, p := range id.Pools {
		if p == queue {
			return true
		}
	}
	return false
}

// allowed narrows a requested queue list to the lanes this caller may poll.
func (id workerIdentity) allowed(queues []string) []string {
	if id.Pools == nil {
		return queues
	}
	out := make([]string, 0, len(queues))
	for _, q := range queues {
		if id.mayUse(q) {
			out = append(out, q)
		}
	}
	return out
}

func (s *Server) workerIdentity(r *http.Request) (workerIdentity, error) {
	wid := r.Header.Get(workerIDHeader)
	if wid == "" {
		return workerIdentity{}, fmt.Errorf("%s is required", workerIDHeader)
	}
	return workerIdentity{WorkerID: wid}, nil
}

// workerAuth resolves the caller and writes the 400 itself when it cannot.
func (s *Server) workerAuth(w http.ResponseWriter, r *http.Request) (workerIdentity, bool) {
	id, err := s.workerIdentity(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return workerIdentity{}, false
	}
	return id, true
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("decode body: %w", err))
		return false
	}
	return true
}

// --------------------------------------------------------------- dispatch ---

type workerLeaseBody struct {
	Queues []string `json:"queues"`
	Max    int      `json:"max"`
	// Lease is a Go duration; empty means the store's default.
	Lease string `json:"lease,omitempty"`
}

func (s *Server) workerLease(w http.ResponseWriter, r *http.Request) {
	id, ok := s.workerAuth(w, r)
	if !ok {
		return
	}
	var b workerLeaseBody
	if !decodeBody(w, r, &b) {
		return
	}
	lease, err := parseDur(b.Lease)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("lease: %w", err))
		return
	}
	queues := id.allowed(b.Queues)
	if len(queues) == 0 {
		// Not an error: a worker whose lanes are all out of scope simply has
		// nothing to do, the same answer it gets from an empty queue.
		writeJSON(w, http.StatusOK, []core.FlowRun{})
		return
	}
	runs, err := s.store.LeaseFlowRuns(r.Context(), store.LeaseRequest{
		WorkerID: id.WorkerID, // from the credential, never the body
		Queues:   queues,
		Max:      b.Max,
		LeaseFor: lease,
	})
	if err != nil {
		fail(w, err)
		return
	}
	if runs == nil {
		runs = []core.FlowRun{}
	}
	writeJSON(w, http.StatusOK, runs)
}

type workerHeartbeatBody struct {
	Name        string    `json:"name"`
	Queues      []string  `json:"queues"`
	Concurrency int       `json:"concurrency"`
	ActiveRuns  int       `json:"active_runs"`
	StartedAt   time.Time `json:"started_at"`
}

func (s *Server) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	id, ok := s.workerAuth(w, r)
	if !ok {
		return
	}
	var b workerHeartbeatBody
	if !decodeBody(w, r, &b) {
		return
	}
	info := &core.WorkerInfo{
		ID: id.WorkerID, Name: b.Name, Queues: b.Queues,
		Concurrency: b.Concurrency, ActiveRuns: b.ActiveRuns,
		StartedAt: b.StartedAt, LastHeartbeat: time.Now().UTC(),
	}
	if err := s.store.HeartbeatWorker(r.Context(), info); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

type workerLeaseRenewBody struct {
	Lease string `json:"lease,omitempty"`
}

func (s *Server) workerRenewLease(w http.ResponseWriter, r *http.Request) {
	id, ok := s.workerAuth(w, r)
	if !ok {
		return
	}
	var b workerLeaseRenewBody
	if !decodeBody(w, r, &b) {
		return
	}
	lease, err := parseDur(b.Lease)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("lease: %w", err))
		return
	}
	if lease <= 0 {
		lease = time.Minute
	}
	// RenewLease is conditional on ownership in SQL, so a worker cannot extend
	// a lease it no longer holds; ErrNotFound is how it learns that.
	if err := s.store.RenewLease(r.Context(), r.PathValue("id"), id.WorkerID, lease); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------- run lifecycle ---

type workerStateBody struct {
	State      core.StateType  `json:"state"`
	StateName  string          `json:"state_name,omitempty"`
	Message    string          `json:"state_message,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	StartedAt  *time.Time      `json:"started_at,omitempty"`
	EndedAt    *time.Time      `json:"ended_at,omitempty"`
	ScheduleAt *time.Time      `json:"schedule_at,omitempty"`
	BumpRun    bool            `json:"bump_run,omitempty"`
	ClearLease bool            `json:"clear_lease,omitempty"`
	// Force is accepted only so it can be refused with a clear message rather
	// than silently ignored.
	Force bool `json:"force,omitempty"`
}

func (s *Server) workerSetRunState(w http.ResponseWriter, r *http.Request) {
	id, ok := s.workerAuth(w, r)
	if !ok {
		return
	}
	var b workerStateBody
	if !decodeBody(w, r, &b) {
		return
	}
	if b.Force {
		writeErr(w, http.StatusForbidden,
			errors.New("force is reserved for the janitor and cannot be requested by a worker"))
		return
	}
	if !b.State.Valid() {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("unknown state %q", b.State))
		return
	}
	st := core.NewState(b.State, b.StateName, b.Message)
	opts := store.StateOpts{
		Result: b.Result, StartedAt: b.StartedAt, EndedAt: b.EndedAt,
		ScheduleAt: b.ScheduleAt, BumpRun: b.BumpRun, ClearLease: b.ClearLease,
	}
	// The write is conditional on this worker still holding the run. The lease
	// already recorded the owner, so there is nothing to stamp — and stamping
	// would collide with ClearLease on the terminal transitions anyway.
	opts.RequireWorkerID = &id.WorkerID

	updated, err := s.store.SetFlowRunState(r.Context(), r.PathValue("id"), st, opts)
	if err != nil {
		fail(w, err)
		return
	}
	// The worker has no event log and no bus, so the server records the
	// transition for it — and, because it knows who called, attributes it.
	s.events.FlowRunStateChangedBy(r.Context(), updated, id.WorkerID)
	if updated.State == core.StateScheduled {
		// A run handed back to the lane frees capacity; without this nudge the
		// next dispatcher waits for its poll.
		s.events.WorkAvailable(r.Context(), updated.WorkQueue)
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *Server) workerGetRun(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.workerAuth(w, r); !ok {
		return
	}
	run, err := s.store.GetFlowRun(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

type workerCreateRunBody struct {
	Name           string          `json:"name,omitempty"`
	FlowName       string          `json:"flow_name"`
	DeploymentID   *string         `json:"deployment_id,omitempty"`
	Parameters     json.RawMessage `json:"parameters,omitempty"`
	WorkQueue      string          `json:"work_queue,omitempty"`
	Priority       int             `json:"priority,omitempty"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty"`
	Retries        int             `json:"retries,omitempty"`
	RetryDelay     string          `json:"retry_delay,omitempty"`
	Timeout        string          `json:"timeout,omitempty"`
	Tags           []string        `json:"tags,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	ParentRunID    *string         `json:"parent_run_id,omitempty"`
	ParentTaskKey  string          `json:"parent_task_key,omitempty"`
	TraceContext   string          `json:"trace_context,omitempty"`
}

// workerCreateRun is how a flow fans out to a sub-flow. It mirrors what the
// engine's runtime bridge does locally, including the event and the wake-up.
func (s *Server) workerCreateRun(w http.ResponseWriter, r *http.Request) {
	id, ok := s.workerAuth(w, r)
	if !ok {
		return
	}
	var b workerCreateRunBody
	if !decodeBody(w, r, &b) {
		return
	}
	if b.FlowName == "" {
		writeErr(w, http.StatusBadRequest, errors.New("flow_name is required"))
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
	in := store.CreateRunInput{
		Name: b.Name, FlowName: b.FlowName, DeploymentID: b.DeploymentID,
		Parameters: b.Parameters, WorkQueue: b.WorkQueue, Priority: b.Priority,
		Retries: b.Retries, RetryDelay: retryDelay, Timeout: timeout,
		Tags: b.Tags, IdempotencyKey: b.IdempotencyKey,
		ParentRunID: b.ParentRunID, ParentTaskKey: b.ParentTaskKey,
		TraceContext: b.TraceContext,
	}
	if b.ScheduledAt != nil {
		in.ScheduledAt = *b.ScheduledAt
	}
	if in.WorkQueue != "" && !id.mayUse(in.WorkQueue) {
		writeErr(w, http.StatusForbidden, fmt.Errorf("queue %q is out of scope for this worker", in.WorkQueue))
		return
	}
	run, err := s.store.CreateFlowRun(r.Context(), in)
	if err != nil && !errors.Is(err, store.ErrConflict) {
		fail(w, err)
		return
	}
	if run == nil {
		fail(w, store.ErrConflict)
		return
	}
	s.events.FlowRunStateChangedBy(r.Context(), run, id.WorkerID)
	s.events.WorkAvailable(r.Context(), run.WorkQueue)
	writeJSON(w, http.StatusOK, run)
}

// workerResumeRun wakes a suspended parent whose children have all settled.
func (s *Server) workerResumeRun(w http.ResponseWriter, r *http.Request) {
	id, ok := s.workerAuth(w, r)
	if !ok {
		return
	}
	var b struct {
		At *time.Time `json:"at,omitempty"`
	}
	if r.ContentLength > 0 && !decodeBody(w, r, &b) {
		return
	}
	at := time.Now().UTC()
	if b.At != nil {
		at = *b.At
	}
	woken, err := s.store.ResumeSuspendedRun(r.Context(), r.PathValue("id"), at)
	if err != nil {
		fail(w, err)
		return
	}
	s.events.FlowRunStateChangedBy(r.Context(), woken, id.WorkerID)
	s.events.WorkAvailable(r.Context(), woken.WorkQueue)
	writeJSON(w, http.StatusOK, woken)
}

// workerClaimPushRun takes one dispatched run SCHEDULED -> PENDING for a push
// receiver, which then drives it exactly as a leased run.
func (s *Server) workerClaimPushRun(w http.ResponseWriter, r *http.Request) {
	id, ok := s.workerAuth(w, r)
	if !ok {
		return
	}
	var b workerLeaseRenewBody
	if r.ContentLength > 0 && !decodeBody(w, r, &b) {
		return
	}
	lease, err := parseDur(b.Lease)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("lease: %w", err))
		return
	}
	if lease <= 0 {
		lease = time.Minute
	}
	run, err := s.store.ClaimPushRun(r.Context(), r.PathValue("id"), id.WorkerID, lease)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

// ----------------------------------------------------- durable checkpoints ---

func (s *Server) workerGetTaskRun(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.workerAuth(w, r); !ok {
		return
	}
	tr, err := s.store.GetTaskRun(r.Context(), r.PathValue("id"), r.PathValue("key"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tr)
}

func (s *Server) workerPutTaskRun(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.workerAuth(w, r); !ok {
		return
	}
	var tr core.TaskRun
	if !decodeBody(w, r, &tr) {
		return
	}
	// The path is authoritative: a checkpoint cannot be written into a run or
	// under a key other than the one addressed.
	tr.FlowRunID = r.PathValue("id")
	tr.TaskKey = r.PathValue("key")
	if err := s.store.UpsertTaskRun(r.Context(), &tr); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tr)
}

// workerCachedResult answers a cross-run cache lookup. The freshness instant is
// server time: a worker must not be able to revive an expired entry by claiming
// an earlier "now".
func (s *Server) workerCachedResult(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.workerAuth(w, r); !ok {
		return
	}
	raw, hit, err := s.store.FindCachedResult(r.Context(), r.PathValue("key"), time.Now().UTC())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Hit    bool            `json:"hit"`
		Result json.RawMessage `json:"result,omitempty"`
	}{Hit: hit, Result: raw})
}

// ---------------------------------------------- what a running flow writes ---

func (s *Server) workerAppendLogs(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.workerAuth(w, r); !ok {
		return
	}
	var b struct {
		Records []core.LogRecord `json:"records"`
	}
	if !decodeBody(w, r, &b) {
		return
	}
	runID := r.PathValue("id")
	for i := range b.Records {
		b.Records[i].FlowRunID = runID
	}
	if err := s.store.AppendLogs(r.Context(), b.Records); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) workerCreateArtifact(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.workerAuth(w, r); !ok {
		return
	}
	var a core.Artifact
	if !decodeBody(w, r, &a) {
		return
	}
	runID := r.PathValue("id")
	a.FlowRunID = &runID
	if err := s.store.CreateArtifact(r.Context(), &a); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

// ------------------------------------------------- catalogue and lookups ---

func (s *Server) workerUpsertFlow(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.workerAuth(w, r); !ok {
		return
	}
	var f core.Flow
	if !decodeBody(w, r, &f) {
		return
	}
	if f.Name == "" {
		writeErr(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	if err := s.store.UpsertFlow(r.Context(), &f); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, f)
}

// workerEnsureQueue creates a lane if it is missing and leaves an existing one
// exactly as it is — never an upsert, which would reset the operator's
// concurrency limit, pause switch and autoscaling envelope on every restart.
func (s *Server) workerEnsureQueue(w http.ResponseWriter, r *http.Request) {
	id, ok := s.workerAuth(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if !id.mayUse(name) {
		writeErr(w, http.StatusForbidden, fmt.Errorf("queue %q is out of scope for this worker", name))
		return
	}
	if err := s.store.EnsureWorkQueue(r.Context(), name); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) workerDeploymentByName(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.workerAuth(w, r); !ok {
		return
	}
	d, err := s.store.GetDeploymentByName(r.Context(), r.PathValue("name"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

// workerAncestors backs the sub-flow recursion guard.
func (s *Server) workerAncestors(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.workerAuth(w, r); !ok {
		return
	}
	depth := 8
	if v := r.URL.Query().Get("max_depth"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &depth); err != nil || depth <= 0 {
			writeErr(w, http.StatusBadRequest, errors.New("max_depth must be a positive integer"))
			return
		}
	}
	ids, err := s.store.AncestorDeploymentIDs(r.Context(), r.PathValue("id"), depth)
	if err != nil {
		fail(w, err)
		return
	}
	if ids == nil {
		ids = []string{}
	}
	writeJSON(w, http.StatusOK, ids)
}

func (s *Server) workerUnfinishedChildren(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.workerAuth(w, r); !ok {
		return
	}
	n, err := s.store.CountUnfinishedChildren(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Unfinished int `json:"unfinished"`
	}{n})
}

// registerWorkerRoutes mounts the worker API. Kept in one function so the whole
// surface a remote worker is allowed to reach can be read at a glance — and so
// it is obvious that AppendEvent is not on it.
func (s *Server) registerWorkerRoutes(mux *http.ServeMux) {
	const p = "/api/v1/worker"

	mux.HandleFunc("POST "+p+"/lease", s.workerLease)
	mux.HandleFunc("POST "+p+"/heartbeat", s.workerHeartbeat)
	mux.HandleFunc("POST "+p+"/runs/{id}/renew", s.workerRenewLease)

	mux.HandleFunc("GET "+p+"/runs/{id}", s.workerGetRun)
	mux.HandleFunc("POST "+p+"/runs", s.workerCreateRun)
	mux.HandleFunc("POST "+p+"/runs/{id}/state", s.workerSetRunState)
	mux.HandleFunc("POST "+p+"/runs/{id}/resume", s.workerResumeRun)
	mux.HandleFunc("POST "+p+"/runs/{id}/claim", s.workerClaimPushRun)

	mux.HandleFunc("GET "+p+"/runs/{id}/tasks/{key}", s.workerGetTaskRun)
	mux.HandleFunc("PUT "+p+"/runs/{id}/tasks/{key}", s.workerPutTaskRun)
	mux.HandleFunc("GET "+p+"/cache/{key}", s.workerCachedResult)

	mux.HandleFunc("POST "+p+"/runs/{id}/logs", s.workerAppendLogs)
	mux.HandleFunc("POST "+p+"/runs/{id}/artifacts", s.workerCreateArtifact)

	mux.HandleFunc("POST "+p+"/flows", s.workerUpsertFlow)
	mux.HandleFunc("POST "+p+"/queues/{name}", s.workerEnsureQueue)
	mux.HandleFunc("GET "+p+"/deployments/by-name/{name}", s.workerDeploymentByName)
	mux.HandleFunc("GET "+p+"/runs/{id}/ancestors", s.workerAncestors)
	mux.HandleFunc("GET "+p+"/runs/{id}/children/unfinished", s.workerUnfinishedChildren)
}
