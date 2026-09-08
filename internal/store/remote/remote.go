// Package remote implements store.WorkerStore over the server's worker API, so
// a worker can execute flows from somewhere that has no route to the database —
// a site VM allowed nothing but outbound 443.
//
// It is the client half of internal/server/worker.go and carries no rules of
// its own. Every decision that matters — whether a run may be leased, whether a
// transition is legal, whether a cache entry is still fresh — is made by the
// server, and this package's job is to ask clearly and to report the answer
// faithfully. In particular it does not retry a 4xx: a refusal is an answer,
// and repeating it would only turn a clear rejection into a slow one.
package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

// Config describes how to reach the orchestrator.
type Config struct {
	// BaseURL is the server root, with or without a trailing /api/v1.
	BaseURL string
	// Token is the api-worker key. It is sent as X-API-Key.
	Token string
	// WorkerID identifies this process in lease records. It is a label: the
	// key is what carries authority.
	WorkerID string
	// Timeout bounds one attempt. Default 30s.
	Timeout time.Duration
	// MaxAttempts bounds retries of a retryable failure. Default 4.
	MaxAttempts int
	Logger      *slog.Logger
	// HTTPClient overrides the default client (tests, custom transports).
	HTTPClient *http.Client
}

// Store speaks the worker API. It satisfies store.WorkerStore and nothing more:
// a worker that tried to read the user table would not compile.
type Store struct {
	base     string
	token    string
	workerID string
	attempts int
	log      *slog.Logger
	hc       *http.Client
}

var _ store.WorkerStore = (*Store)(nil)

// New builds a client. It validates the URL so a typo fails at start-up rather
// than on the first lease.
func New(cfg Config) (*Store, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("remote: BaseURL is required")
	}
	if cfg.Token == "" {
		return nil, errors.New("remote: Token is required")
	}
	if cfg.WorkerID == "" {
		return nil, errors.New("remote: WorkerID is required")
	}
	base := strings.TrimRight(cfg.BaseURL, "/")
	base = strings.TrimSuffix(base, "/api/v1")
	if _, err := url.Parse(base); err != nil {
		return nil, fmt.Errorf("remote: bad BaseURL %q: %w", cfg.BaseURL, err)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 4
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: cfg.Timeout}
	}
	return &Store{
		base: base + "/api/v1/worker", token: cfg.Token, workerID: cfg.WorkerID,
		attempts: cfg.MaxAttempts, log: cfg.Logger, hc: hc,
	}, nil
}

// Close satisfies the callers that close a store. There is no pool to drain.
func (s *Store) Close() error { return nil }

// ------------------------------------------------------------- transport ---

// apiError carries a status the caller may want to distinguish.
type apiError struct {
	Status int
	Msg    string
}

func (e *apiError) Error() string { return fmt.Sprintf("worker api: %d: %s", e.Status, e.Msg) }

// mapStatus turns the server's answer into the sentinel errors the engine
// already reacts to, so a remote worker takes exactly the same code paths a
// local one does.
func mapStatus(status int, msg string) error {
	switch status {
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", store.ErrNotFound, msg)
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", store.ErrConflict, msg)
	}
	return &apiError{Status: status, Msg: msg}
}

// retryable reports whether another attempt could plausibly succeed. A 4xx
// never can: the server has understood and refused.
func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

// do performs one call with retries. out may be nil for responses with no body
// worth keeping.
func (s *Store) do(ctx context.Context, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		var err error
		if body, err = json.Marshal(in); err != nil {
			return fmt.Errorf("remote: encode %s %s: %w", method, path, err)
		}
	}

	var lastErr error
	for attempt := 1; attempt <= s.attempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff(attempt)):
			}
		}
		status, raw, err := s.attempt(ctx, method, path, body)
		switch {
		case err != nil:
			// Transport failure: the site's link, not the server's answer.
			lastErr = fmt.Errorf("remote: %s %s: %w", method, path, err)
			if ctx.Err() != nil {
				return ctx.Err()
			}
		case status >= 200 && status < 300:
			if out == nil || len(raw) == 0 {
				return nil
			}
			if err := json.Unmarshal(raw, out); err != nil {
				return fmt.Errorf("remote: decode %s %s: %w", method, path, err)
			}
			return nil
		case retryable(status):
			lastErr = mapStatus(status, errText(raw))
		default:
			return mapStatus(status, errText(raw))
		}
		s.log.Debug("worker api retry", "method", method, "path", path,
			"attempt", attempt, "err", lastErr)
	}
	return lastErr
}

func (s *Store) attempt(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.base+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-API-Key", s.token)
	req.Header.Set("X-PrimeFlow-Worker-ID", s.workerID)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, raw, nil
}

// backoff grows exponentially with jitter, so a fleet that loses the link does
// not come back in lockstep.
func backoff(attempt int) time.Duration {
	d := time.Duration(1<<uint(attempt-1)) * 250 * time.Millisecond
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d + time.Duration(rand.Int63n(int64(d/2+1)))
}

func errText(raw []byte) string {
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(raw, &e) == nil && e.Error != "" {
		return e.Error
	}
	return strings.TrimSpace(string(raw))
}

func esc(s string) string { return url.PathEscape(s) }

func dur(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	return d.String()
}

// --------------------------------------------------------------- dispatch ---

func (s *Store) LeaseFlowRuns(ctx context.Context, req store.LeaseRequest) ([]core.FlowRun, error) {
	// WorkerID is deliberately not sent: the server takes it from the
	// credential, and a body field would only invite disagreement.
	body := map[string]any{"queues": req.Queues, "max": req.Max, "lease": dur(req.LeaseFor)}
	var out []core.FlowRun
	if err := s.do(ctx, http.MethodPost, "/lease", body, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) RenewLease(ctx context.Context, runID, workerID string, d time.Duration) error {
	return s.do(ctx, http.MethodPost, "/runs/"+esc(runID)+"/renew",
		map[string]any{"lease": dur(d)}, nil)
}

func (s *Store) HeartbeatWorker(ctx context.Context, w *core.WorkerInfo) error {
	return s.do(ctx, http.MethodPost, "/heartbeat", map[string]any{
		"name": w.Name, "queues": w.Queues, "concurrency": w.Concurrency,
		"active_runs": w.ActiveRuns, "started_at": w.StartedAt,
	}, nil)
}

// ------------------------------------------------------------- catalogue ---

func (s *Store) UpsertFlow(ctx context.Context, f *core.Flow) error {
	return s.do(ctx, http.MethodPost, "/flows", f, f)
}

func (s *Store) EnsureWorkQueue(ctx context.Context, name string) error {
	return s.do(ctx, http.MethodPost, "/queues/"+esc(name), nil, nil)
}

// ---------------------------------------------------------- run lifecycle ---

func (s *Store) SetFlowRunState(ctx context.Context, id string, st core.State, opts store.StateOpts) (*core.FlowRun, error) {
	if opts.Force {
		// The server refuses it anyway; failing here says so without a round
		// trip and without the caller wondering which layer objected.
		return nil, errors.New("remote: force is reserved for the janitor and cannot be requested by a worker")
	}
	body := map[string]any{
		"state": string(st.Type), "state_name": st.Name, "state_message": st.Message,
		"bump_run": opts.BumpRun, "clear_lease": opts.ClearLease,
	}
	if len(opts.Result) > 0 {
		body["result"] = opts.Result
	}
	if opts.StartedAt != nil {
		body["started_at"] = opts.StartedAt.UTC()
	}
	if opts.EndedAt != nil {
		body["ended_at"] = opts.EndedAt.UTC()
	}
	if opts.ScheduleAt != nil {
		body["schedule_at"] = opts.ScheduleAt.UTC()
	}
	var out core.FlowRun
	if err := s.do(ctx, http.MethodPost, "/runs/"+esc(id)+"/state", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Store) GetFlowRun(ctx context.Context, id string) (*core.FlowRun, error) {
	var out core.FlowRun
	if err := s.do(ctx, http.MethodGet, "/runs/"+esc(id), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Store) CreateFlowRun(ctx context.Context, in store.CreateRunInput) (*core.FlowRun, error) {
	body := map[string]any{
		"name": in.Name, "flow_name": in.FlowName, "work_queue": in.WorkQueue,
		"priority": in.Priority, "retries": in.Retries,
		"retry_delay": dur(in.RetryDelay), "timeout": dur(in.Timeout),
		"tags": in.Tags, "idempotency_key": in.IdempotencyKey,
		"parent_task_key": in.ParentTaskKey, "trace_context": in.TraceContext,
	}
	if len(in.Parameters) > 0 {
		body["parameters"] = in.Parameters
	}
	if in.DeploymentID != nil {
		body["deployment_id"] = *in.DeploymentID
	}
	if in.ParentRunID != nil {
		body["parent_run_id"] = *in.ParentRunID
	}
	if !in.ScheduledAt.IsZero() {
		body["scheduled_at"] = in.ScheduledAt.UTC()
	}
	var out core.FlowRun
	if err := s.do(ctx, http.MethodPost, "/runs", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Store) ResumeSuspendedRun(ctx context.Context, runID string, at time.Time) (*core.FlowRun, error) {
	var out core.FlowRun
	if err := s.do(ctx, http.MethodPost, "/runs/"+esc(runID)+"/resume",
		map[string]any{"at": at.UTC()}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Store) ClaimPushRun(ctx context.Context, runID, workerID string, leaseFor time.Duration) (*core.FlowRun, error) {
	var out core.FlowRun
	if err := s.do(ctx, http.MethodPost, "/runs/"+esc(runID)+"/claim",
		map[string]any{"lease": dur(leaseFor)}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ----------------------------------------------------- durable checkpoints ---

func (s *Store) GetTaskRun(ctx context.Context, flowRunID, taskKey string) (*core.TaskRun, error) {
	var out core.TaskRun
	if err := s.do(ctx, http.MethodGet,
		"/runs/"+esc(flowRunID)+"/tasks/"+esc(taskKey), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Store) UpsertTaskRun(ctx context.Context, tr *core.TaskRun) error {
	return s.do(ctx, http.MethodPut,
		"/runs/"+esc(tr.FlowRunID)+"/tasks/"+esc(tr.TaskKey), tr, tr)
}

// FindCachedResult ignores the caller's now: the server answers on its own
// clock, which is what stops a skewed site from reviving an expired entry.
func (s *Store) FindCachedResult(ctx context.Context, cacheKey string, _ time.Time) (json.RawMessage, bool, error) {
	var out struct {
		Hit    bool            `json:"hit"`
		Result json.RawMessage `json:"result"`
	}
	if err := s.do(ctx, http.MethodGet, "/cache/"+esc(cacheKey), nil, &out); err != nil {
		return nil, false, err
	}
	return out.Result, out.Hit, nil
}

// ---------------------------------------------- what a running flow writes ---

func (s *Store) AppendLogs(ctx context.Context, recs []core.LogRecord) error {
	if len(recs) == 0 {
		return nil
	}
	// The route is per run, and the engine flushes one run's buffer at a time.
	// Grouping defensively keeps that an implementation detail rather than a
	// contract the caller has to know.
	byRun := map[string][]core.LogRecord{}
	order := make([]string, 0, 1)
	for _, rec := range recs {
		if _, seen := byRun[rec.FlowRunID]; !seen {
			order = append(order, rec.FlowRunID)
		}
		byRun[rec.FlowRunID] = append(byRun[rec.FlowRunID], rec)
	}
	for _, runID := range order {
		if err := s.do(ctx, http.MethodPost, "/runs/"+esc(runID)+"/logs",
			map[string]any{"records": byRun[runID]}, nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) CreateArtifact(ctx context.Context, a *core.Artifact) error {
	if a.FlowRunID == nil {
		return errors.New("remote: artifact needs a flow run id")
	}
	return s.do(ctx, http.MethodPost, "/runs/"+esc(*a.FlowRunID)+"/artifacts", a, a)
}

// ------------------------------------------------------------- sub-flows ---

func (s *Store) GetDeploymentByName(ctx context.Context, name string) (*core.Deployment, error) {
	var out core.Deployment
	if err := s.do(ctx, http.MethodGet, "/deployments/by-name/"+esc(name), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Store) AncestorDeploymentIDs(ctx context.Context, runID string, maxDepth int) ([]string, error) {
	var out []string
	path := fmt.Sprintf("/runs/%s/ancestors?max_depth=%d", esc(runID), maxDepth)
	if err := s.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) CountUnfinishedChildren(ctx context.Context, parentID string) (int, error) {
	var out struct {
		Unfinished int `json:"unfinished"`
	}
	if err := s.do(ctx, http.MethodGet,
		"/runs/"+esc(parentID)+"/children/unfinished", nil, &out); err != nil {
		return 0, err
	}
	return out.Unfinished, nil
}
