package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/store"
)

func TestStatsBucketsAndTotals(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	// Three flow runs in the last hour: two completed, one failed.
	now := time.Now().UTC()
	mk := func(state core.StateType, at time.Time) {
		r := mkRun(t, st, "default", 50, at)
		end := at.Add(time.Second)
		if _, err := st.SetFlowRunState(ctx, r.ID, core.NewState(state, string(state), ""),
			store.StateOpts{StartedAt: &at, EndedAt: &end, Force: true}); err != nil {
			t.Fatalf("set state: %v", err)
		}
	}
	mk(core.StateCompleted, now.Add(-40*time.Minute))
	mk(core.StateCompleted, now.Add(-20*time.Minute))
	mk(core.StateFailed, now.Add(-10*time.Minute))

	// A couple of events.
	for i := 0; i < 4; i++ {
		if err := st.AppendEvent(ctx, &core.Event{
			ID: uuid.NewString(), Name: "flow-run.COMPLETED", ResourceType: "flow-run", ResourceID: "x",
			Occurred: now.Add(-time.Duration(i*5) * time.Minute),
		}); err != nil {
			t.Fatalf("append event: %v", err)
		}
	}

	s, err := st.Stats(ctx, 8*time.Hour, 48)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}

	if s.BucketSeconds <= 0 || len(s.FlowRuns.Buckets) < 40 {
		t.Fatalf("bucket layout looks wrong: step=%d buckets=%d", s.BucketSeconds, len(s.FlowRuns.Buckets))
	}
	// bucket_seconds * len ≈ window_seconds (± one step).
	if got := s.BucketSeconds * len(s.FlowRuns.Buckets); got < s.WindowSeconds-s.BucketSeconds || got > s.WindowSeconds+s.BucketSeconds {
		t.Fatalf("buckets do not span the window: %d vs %d", got, s.WindowSeconds)
	}

	// Bucket sums == totals.
	var c, f, o int
	for _, b := range s.FlowRuns.Buckets {
		c += b.Completed
		f += b.Failed
		o += b.Other
	}
	if c != 2 || f != 1 {
		t.Fatalf("flow-run buckets: completed=%d failed=%d (want 2/1)", c, f)
	}
	if s.FlowRuns.Total != c+f+o || s.FlowRuns.ByState["COMPLETED"] != 2 || s.FlowRuns.ByState["FAILED"] != 1 {
		t.Fatalf("by_state / total mismatch: total=%d by_state=%v", s.FlowRuns.Total, s.FlowRuns.ByState)
	}

	var en int
	for _, b := range s.Events.Buckets {
		en += b.N
	}
	if en != 4 || s.Events.Total != 4 {
		t.Fatalf("event buckets sum=%d total=%d (want 4/4)", en, s.Events.Total)
	}
}
