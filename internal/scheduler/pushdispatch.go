package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/pushsig"
)

// pushDispatchLease is how long a run is held after dispatch, giving the receiver
// time to claim it. If it does not, the hold lapses and the run is re-dispatched.
const pushDispatchLease = 2 * time.Minute

var pushHTTP = &http.Client{Timeout: 15 * time.Second}

// pushDispatchBody is what a push endpoint receives.
type pushDispatchBody struct {
	RunID     string `json:"run_id"`
	FlowName  string `json:"flow_name"`
	WorkQueue string `json:"work_queue"`
	Attempt   int    `json:"attempt"`
}

// pushDispatch notifies the endpoint of every push work pool that has ready
// runs. The leader runs this; a run held here but never claimed is reclaimed as
// CRASHED by the janitor, exactly like an abandoned leased run.
func (s *Scheduler) pushDispatch(ctx context.Context) {
	queues, err := s.store.ListWorkQueues(ctx)
	if err != nil {
		s.log.Warn("push dispatch: list pools failed", "err", err)
		return
	}
	for _, q := range queues {
		if q.PoolType != "push" || q.PushEndpoint == "" || q.Paused {
			continue
		}
		runs, err := s.store.PushReadyRuns(ctx, q.Name, 20)
		if err != nil {
			s.log.Warn("push dispatch: ready runs failed", "pool", q.Name, "err", err)
			continue
		}
		for i := range runs {
			s.dispatchOne(ctx, q, &runs[i])
		}
	}
}

func (s *Scheduler) dispatchOne(ctx context.Context, pool core.WorkQueue, run *core.FlowRun) {
	if err := s.store.MarkPushDispatched(ctx, run.ID, pushDispatchLease); err != nil {
		return // already claimed / no longer scheduled — fine
	}
	body, _ := json.Marshal(pushDispatchBody{
		RunID: run.ID, FlowName: run.FlowName, WorkQueue: run.WorkQueue, Attempt: run.RunCount,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pool.PushEndpoint, bytes.NewReader(body))
	if err != nil {
		_ = s.store.ClearPushDispatch(ctx, run.ID)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "primeflow-push-dispatch")
	if pool.PushSecret != "" {
		req.Header.Set(pushsig.Header, pushsig.Sign(pool.PushSecret, body))
	}
	resp, err := pushHTTP.Do(req)
	if err != nil {
		s.log.Warn("push dispatch failed", "pool", pool.Name, "run", run.ID, "err", err)
		_ = s.store.ClearPushDispatch(ctx, run.ID)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		s.log.Warn("push endpoint rejected run", "pool", pool.Name, "run", run.ID, "status", resp.StatusCode)
		_ = s.store.ClearPushDispatch(ctx, run.ID)
		return
	}
	s.events.Emit(ctx, "flow-run.push-dispatched", "flow-run", run.ID,
		[]byte(fmt.Sprintf(`{"pool":%q}`, pool.Name)))
}
