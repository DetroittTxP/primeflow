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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/primex/primeflow/internal/apiauth"
	"github.com/primex/primeflow/internal/bus"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

// workerIDHeader names which worker in a pool is calling.
//
// It is a label, not authority. Authority comes from the credential: a
// pool-scoped API key with the api-worker role. A key is a site's credential,
// so the workers sharing one are a single trust domain and the id only has to
// tell them apart in lease records and the console.
const workerIDHeader = "X-PrimeFlow-Worker-ID"

// workerIdentity is who the server believes is calling, and what it may touch.
type workerIdentity struct {
	// WorkerID is recorded on leases and checked by the ownership precondition.
	// It is never read from a body: a worker must not be able to claim work as
	// another worker.
	WorkerID string
	// Pools bounds which lanes this caller may act on. Empty means
	// unrestricted, which is what an operator session or the shared admin
	// bearer gets — those are already trusted with everything.
	Pools []string
	// KeyID, when set, is the API key behind the call, for the audit trail.
	KeyID string
}

// mayUse reports whether this caller is allowed to act on a lane.
func (id workerIdentity) mayUse(queue string) bool {
	if len(id.Pools) == 0 {
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
	if len(id.Pools) == 0 {
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

type workerIdentityKey struct{}

// workerFrom returns the identity wk resolved for this request.
func workerFrom(r *http.Request) workerIdentity {
	id, _ := r.Context().Value(workerIdentityKey{}).(workerIdentity)
	return id
}

// wk is the enforcement chain for one worker route: resolve the caller, check
// the scope its credential carries, and hand the identity down in the context.
//
// Two credentials are accepted. A pool-scoped api-worker key is the one a
// remote site uses, and it goes through the same controls the External API
// applies — active, unexpired, IP allow-list, mutual TLS, rate limit, audit.
// An operator session or the shared admin bearer is also accepted and is
// unrestricted, which is what keeps a locally connected worker and the console
// working exactly as before.
func (s *Server) wk(scope apiauth.Scope, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := s.resolveWorker(w, r, scope)
		if !ok {
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), workerIdentityKey{}, id)))
	}
}

// wkRun additionally refuses a run that sits on a lane outside the caller's
// pools. Without it a key scoped to one site could drive a run belonging to
// another simply by knowing its id. Callers with no pool restriction skip the
// lookup entirely, so the common path costs nothing.
func (s *Server) wkRun(scope apiauth.Scope, next http.HandlerFunc) http.HandlerFunc {
	return s.wk(scope, func(w http.ResponseWriter, r *http.Request) {
		id := workerFrom(r)
		if len(id.Pools) > 0 {
			run, err := s.store.GetFlowRun(r.Context(), r.PathValue("id"))
			if err != nil {
				fail(w, err)
				return
			}
			if !id.mayUse(run.WorkQueue) {
				s.denyWorker(r, id, "pool-denied:"+run.WorkQueue)
				writeErr(w, http.StatusForbidden,
					fmt.Errorf("run %s is on queue %q, which is out of scope for this key", run.ID, run.WorkQueue))
				return
			}
		}
		next(w, r)
	})
}

// resolveWorker identifies the caller and writes the failure itself when it
// cannot. It returns false once anything has been written.
func (s *Server) resolveWorker(w http.ResponseWriter, r *http.Request, scope apiauth.Scope) (workerIdentity, bool) {
	wid := strings.TrimSpace(r.Header.Get(workerIDHeader))
	if wid == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("%s is required", workerIDHeader))
		return workerIdentity{}, false
	}

	secret := bearerOrHeader(r)
	if !strings.HasPrefix(secret, apiauth.SecretPrefixLabel) {
		// No worker key presented. withAuth has already established an
		// operator session or the admin bearer, both unrestricted.
		return workerIdentity{WorkerID: wid}, true
	}

	prefix := apiauth.PrefixOf(secret)
	key, err := s.store.GetAPIKeyByPrefix(r.Context(), prefix)
	if err != nil || !apiauth.SecretMatches(key.SecretHash, secret) {
		writeErr(w, http.StatusUnauthorized, errors.New("invalid API key"))
		return workerIdentity{}, false
	}
	now := time.Now().UTC()
	if !key.Active || key.Expired(now) {
		writeErr(w, http.StatusUnauthorized, errors.New("API key is inactive or expired"))
		return workerIdentity{}, false
	}

	clientIP := s.clientIPAddr(r)
	if len(key.IPAllowlist) > 0 {
		nets := apiauth.ParseCIDRs(strings.Join(key.IPAllowlist, ","))
		if clientIP == nil || !apiauth.IPInAny(clientIP, nets) {
			s.denyKey(r, key.ID, "ip-not-allowlisted", ipString(clientIP))
			writeErr(w, http.StatusUnauthorized, errors.New("client address is not allow-listed for this key"))
			return workerIdentity{}, false
		}
	}
	if key.RequireMTLS {
		verified := apiauth.ViaTrustedProxy(r, s.cfg.TrustedProxyCIDRs) &&
			strings.EqualFold(r.Header.Get("X-SSL-Client-Verify"), "SUCCESS")
		if !verified {
			s.denyKey(r, key.ID, "mtls-not-verified", ipString(clientIP))
			writeErr(w, http.StatusUnauthorized, errors.New("this key requires a verified client certificate"))
			return workerIdentity{}, false
		}
	}

	role, _ := apiauth.LookupRole(key.Role)
	if scope != "" && !role.Has(scope) {
		s.denyKey(r, key.ID, "scope-denied:"+string(scope), ipString(clientIP))
		writeErr(w, http.StatusForbidden,
			errors.New("this key's role ("+role.Label+") lacks the "+string(scope)+" scope"))
		return workerIdentity{}, false
	}

	if cfg, err := s.store.GetExternalAPISettings(r.Context()); err == nil {
		limit := cfg.DefaultRateLimitPerMin
		if key.RateLimitPerMin != nil {
			limit = *key.RateLimitPerMin
		}
		if ok, retry := s.rateLimiter.Allow(key.ID, limit); !ok {
			w.Header().Set("Retry-After", secondsString(retry))
			writeErr(w, http.StatusTooManyRequests, errors.New("rate limit exceeded"))
			return workerIdentity{}, false
		}
	}

	_ = s.store.TouchAPIKey(r.Context(), key.ID, now)
	return workerIdentity{WorkerID: wid, Pools: key.Pools, KeyID: key.ID}, true
}

// denyWorker records a pool refusal against the key that made it, so an
// operator can see a site reaching outside its lanes.
func (s *Server) denyWorker(r *http.Request, id workerIdentity, reason string) {
	if id.KeyID == "" {
		return
	}
	s.denyKey(r, id.KeyID, reason, ipString(s.clientIPAddr(r)))
}

func ipString(ip net.IP) string {
	if ip == nil {
		return ""
	}
	return ip.String()
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
	id := workerFrom(r)
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
	// Holding lists the runs this worker believes it still owns. Sending them
	// with the heartbeat is what makes a worker's request rate independent of
	// how many flows it is running.
	Holding []string `json:"holding,omitempty"`
	// Lease is a Go duration for the renewal; empty means the default.
	Lease string `json:"lease,omitempty"`
}

// workerHeartbeatReply answers all three questions a worker asks each interval:
// am I still alive to you, do I still hold these, and has anyone asked me to
// stop.
type workerHeartbeatReply struct {
	Worker *core.WorkerInfo `json:"worker"`
	// Renewed and Lost partition Holding. A lost run was reclaimed elsewhere and
	// must be abandoned rather than finished.
	Renewed []string `json:"renewed"`
	Lost    []string `json:"lost"`
	// Cancelling names held runs an operator has asked to stop.
	Cancelling []string `json:"cancelling"`
}

func (s *Server) workerHeartbeat(w http.ResponseWriter, r *http.Request) {
	id := workerFrom(r)
	var b workerHeartbeatBody
	if !decodeBody(w, r, &b) {
		return
	}
	// Two different calls land on this route: the liveness beat, which carries
	// who the worker is, and the batched lease renewal, which carries only the
	// runs it holds. Registering from the second would write back the fields it
	// never sends, so a remote worker would lose its name, its lanes and its
	// concurrency in the console for exactly as long as it is busy. A worker
	// always names itself, so the name is what tells the two apart.
	var info *core.WorkerInfo
	if b.Name != "" {
		info = &core.WorkerInfo{
			ID: id.WorkerID, Name: b.Name, Queues: b.Queues,
			Concurrency: b.Concurrency, ActiveRuns: b.ActiveRuns,
			StartedAt: b.StartedAt, LastHeartbeat: time.Now().UTC(),
		}
		if err := s.store.HeartbeatWorker(r.Context(), info); err != nil {
			fail(w, err)
			return
		}
	}

	reply := workerHeartbeatReply{Worker: info, Renewed: []string{}, Lost: []string{}, Cancelling: []string{}}
	if len(b.Holding) > 0 {
		lease, err := parseDur(b.Lease)
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("lease: %w", err))
			return
		}
		if lease <= 0 {
			lease = time.Minute
		}
		renewed, cancelling, err := s.store.RenewLeases(r.Context(), id.WorkerID, b.Holding, lease)
		if err != nil {
			fail(w, err)
			return
		}
		held := make(map[string]bool, len(renewed))
		for _, rid := range renewed {
			held[rid] = true
		}
		reply.Renewed = append(reply.Renewed, renewed...)
		reply.Cancelling = append(reply.Cancelling, cancelling...)
		for _, rid := range b.Holding {
			if !held[rid] {
				reply.Lost = append(reply.Lost, rid)
			}
		}
	}
	writeJSON(w, http.StatusOK, reply)
}

type workerLeaseRenewBody struct {
	Lease string `json:"lease,omitempty"`
}

func (s *Server) workerRenewLease(w http.ResponseWriter, r *http.Request) {
	id := workerFrom(r)
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
	// Resume says this SCHEDULED transition did not consume an attempt (a
	// durable suspension, or a flow the worker does not have registered). A
	// worker can already decline to fail a run at all, so trusting it here
	// grants nothing it did not already have.
	Resume     bool `json:"resume,omitempty"`
	ClearLease bool `json:"clear_lease,omitempty"`
	// Force is accepted only so it can be refused with a clear message rather
	// than silently ignored.
	Force bool `json:"force,omitempty"`
}

func (s *Server) workerSetRunState(w http.ResponseWriter, r *http.Request) {
	id := workerFrom(r)
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
		Resume: b.Resume,
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
	id := workerFrom(r)
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
	id := workerFrom(r)
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
	id := workerFrom(r)
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
	tr, err := s.store.GetTaskRun(r.Context(), r.PathValue("id"), r.PathValue("key"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tr)
}

func (s *Server) workerPutTaskRun(w http.ResponseWriter, r *http.Request) {
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
	id := workerFrom(r)
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
	d, err := s.store.GetDeploymentByName(r.Context(), r.PathValue("name"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

// workerAncestors backs the sub-flow recursion guard.
func (s *Server) workerAncestors(w http.ResponseWriter, r *http.Request) {
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
	n, err := s.store.CountUnfinishedChildren(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Unfinished int `json:"unfinished"`
	}{n})
}

// workerStream is the wake-up channel, carried on the same 443 the rest of the
// worker API uses.
//
// A site cannot reach NATS or Redis, so without this a remote worker learns
// about new work only on its next poll. The bus contract does not change: every
// message here is a hint, delivery is never required for correctness, and a
// worker that loses the stream converges on its poll interval. That is why a
// slow reader is dropped rather than allowed to back up, and why nothing is
// replayed on reconnect — the queue in Postgres is still the truth.
//
// Messages are filtered to the credential's pools. Work notices carry the lane
// name, so that is a string comparison. A cancellation names only a run, so a
// pool-scoped caller costs one lookup — cancellations are rare, and telling a
// site that some run it cannot see was cancelled would leak the existence of
// another site's work.
func (s *Server) workerStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, errors.New("streaming unsupported"))
		return
	}
	id := workerFrom(r)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")

	ctx := r.Context()
	type framed struct {
		event   string
		payload []byte
	}
	ch := make(chan framed, 64)

	if s.bus != nil {
		_ = s.bus.Subscribe(ctx, []string{bus.TopicWork, bus.TopicControl}, func(m bus.Message) {
			f := framed{payload: m.Payload}
			switch m.Topic {
			case bus.TopicWork:
				var p struct {
					Queue string `json:"queue"`
				}
				if json.Unmarshal(m.Payload, &p) != nil || !id.mayUse(p.Queue) {
					return
				}
				f.event = "work"
			case bus.TopicControl:
				var c bus.ControlMessage
				if json.Unmarshal(m.Payload, &c) != nil || c.FlowRunID == "" {
					return
				}
				if len(id.Pools) > 0 {
					run, err := s.store.GetFlowRun(ctx, c.FlowRunID)
					if err != nil || !id.mayUse(run.WorkQueue) {
						return
					}
				}
				f.event = "control"
			default:
				return
			}
			select {
			case ch <- f:
			default: // a reader that cannot keep up falls back to polling
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
		case f := <-ch:
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", f.event, f.payload)
			flusher.Flush()
		}
	}
}

// registerWorkerRoutes mounts the worker API. Kept in one function so the whole
// surface a remote worker is allowed to reach can be read at a glance — and so
// it is obvious that AppendEvent is not on it.
func (s *Server) registerWorkerRoutes(mux *http.ServeMux) {
	const p = "/api/v1/worker"
	const (
		lease  = apiauth.ScopeWorkerLease
		report = apiauth.ScopeWorkerReport
		reads  = apiauth.ScopeReadRuns
		deps   = apiauth.ScopeReadDeployments
	)

	mux.HandleFunc("POST "+p+"/lease", s.wk(lease, s.workerLease))
	mux.HandleFunc("GET "+p+"/stream", s.wk(lease, s.workerStream))
	mux.HandleFunc("POST "+p+"/heartbeat", s.wk(report, s.workerHeartbeat))
	mux.HandleFunc("POST "+p+"/runs/{id}/renew", s.wkRun(report, s.workerRenewLease))

	mux.HandleFunc("GET "+p+"/runs/{id}", s.wkRun(reads, s.workerGetRun))
	mux.HandleFunc("POST "+p+"/runs", s.wk(report, s.workerCreateRun))
	mux.HandleFunc("POST "+p+"/runs/{id}/state", s.wkRun(report, s.workerSetRunState))
	mux.HandleFunc("POST "+p+"/runs/{id}/resume", s.wkRun(report, s.workerResumeRun))
	mux.HandleFunc("POST "+p+"/runs/{id}/claim", s.wkRun(report, s.workerClaimPushRun))

	mux.HandleFunc("GET "+p+"/runs/{id}/tasks/{key}", s.wkRun(reads, s.workerGetTaskRun))
	mux.HandleFunc("PUT "+p+"/runs/{id}/tasks/{key}", s.wkRun(report, s.workerPutTaskRun))
	mux.HandleFunc("GET "+p+"/cache/{key}", s.wk(reads, s.workerCachedResult))

	mux.HandleFunc("POST "+p+"/runs/{id}/logs", s.wkRun(report, s.workerAppendLogs))
	mux.HandleFunc("POST "+p+"/runs/{id}/artifacts", s.wkRun(report, s.workerCreateArtifact))

	mux.HandleFunc("POST "+p+"/flows", s.wk(report, s.workerUpsertFlow))
	mux.HandleFunc("POST "+p+"/queues/{name}", s.wk(report, s.workerEnsureQueue))
	mux.HandleFunc("GET "+p+"/deployments/by-name/{name}", s.wk(deps, s.workerDeploymentByName))
	mux.HandleFunc("GET "+p+"/runs/{id}/ancestors", s.wkRun(reads, s.workerAncestors))
	mux.HandleFunc("GET "+p+"/runs/{id}/children/unfinished", s.wkRun(reads, s.workerUnfinishedChildren))
}
