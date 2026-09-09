package metrics

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/store"
)

// fakeStore implements just the two methods queueCollector reads.
type fakeStore struct{}

func (fakeStore) QueueStats(context.Context) ([]store.QueueStat, error) {
	max := 10
	return []store.QueueStat{{
		WorkQueue: core.WorkQueue{
			Name: "vcd", MinWorkers: 0, MaxWorkers: &max, TargetReadyPerWorker: 5,
		},
		Ready: 20, Scheduled: 25, Running: 4,
	}}, nil
}

func (fakeStore) ListWorkers(context.Context) ([]core.WorkerInfo, error) { return nil, nil }

func TestDesiredWorkers(t *testing.T) {
	i := func(n int) *int { return &n }
	cases := []struct {
		ready, target, min int
		max                *int
		want               int
	}{
		{ready: 0, target: 5, min: 0, max: nil, want: 0},
		{ready: 0, target: 5, min: 2, max: nil, want: 2},      // min floor
		{ready: 12, target: 5, min: 0, max: nil, want: 3},     // ceil(12/5)
		{ready: 100, target: 5, min: 0, max: i(10), want: 10}, // max ceiling
		{ready: 3, target: 0, min: 0, max: nil, want: 1},      // target defaults to 5
	}
	for _, c := range cases {
		if got := DesiredWorkers(c.ready, c.target, c.min, c.max); got != c.want {
			t.Errorf("DesiredWorkers(%d,%d,%d,%v) = %d, want %d",
				c.ready, c.target, c.min, c.max, got, c.want)
		}
	}
}

func TestQueueCollectorScrapes(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(&queueCollector{store: fakeStore{}})

	const want = `
# HELP primeflow_queue_desired_workers Autoscaling target worker count, per queue.
# TYPE primeflow_queue_desired_workers gauge
primeflow_queue_desired_workers{queue="vcd"} 4
# HELP primeflow_queue_ready Scheduled runs whose time has come, per queue.
# TYPE primeflow_queue_ready gauge
primeflow_queue_ready{queue="vcd"} 20
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want),
		"primeflow_queue_ready", "primeflow_queue_desired_workers"); err != nil {
		t.Fatal(err)
	}
}
