// Package metrics is PrimeFlow's Prometheus surface.
//
// It exposes the RED signals for HTTP and flow execution, plus per-queue gauges
// computed at scrape time from the store. The most important of those is
// primeflow_queue_desired_workers — the number a KEDA ScaledObject or an HPA
// external-metric scales a worker Deployment to. PrimeFlow never launches
// workers itself; it publishes the target and lets Kubernetes act.
//
// A nil *Metrics is valid and every method on it is a no-op, so the engine and
// server can be constructed without metrics in tests.
package metrics

import (
	"context"
	"math"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

// Metrics holds the collectors and the registry they are registered on.
type Metrics struct {
	reg *prometheus.Registry

	httpRequests   *prometheus.CounterVec
	httpDuration   *prometheus.HistogramVec
	runTransitions *prometheus.CounterVec
	runDuration    prometheus.Histogram
	taskDuration   *prometheus.HistogramVec
}

// New builds the registry, registers the Go/process collectors and PrimeFlow's
// own, and wires the scrape-time queue collector to st.
func New(st store.Store) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		reg: reg,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "primeflow_http_requests_total",
			Help: "HTTP requests handled, by route, method and status class.",
		}, []string{"route", "method", "code"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "primeflow_http_request_duration_seconds",
			Help:    "HTTP request latency by route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route"}),
		runTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "primeflow_flow_run_transitions_total",
			Help: "Flow-run state transitions, by destination state.",
		}, []string{"to_state"}),
		runDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "primeflow_flow_run_duration_seconds",
			Help:    "Wall-clock duration of a flow-run attempt.",
			Buckets: []float64{.1, .5, 1, 5, 15, 60, 300, 900, 3600},
		}),
		taskDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "primeflow_task_run_duration_seconds",
			Help:    "Duration of a task attempt, by outcome.",
			Buckets: []float64{.01, .05, .1, .5, 1, 5, 15, 60, 300},
		}, []string{"outcome"}),
	}
	reg.MustRegister(m.httpRequests, m.httpDuration, m.runTransitions, m.runDuration, m.taskDuration)
	reg.MustRegister(&queueCollector{store: st})
	return m
}

// Registry exposes the registry so the server can mount promhttp against it.
func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.reg
}

// ObserveHTTP records one handled request.
func (m *Metrics) ObserveHTTP(route, method, codeClass string, d time.Duration) {
	if m == nil {
		return
	}
	if route == "" {
		route = "other"
	}
	m.httpRequests.WithLabelValues(route, method, codeClass).Inc()
	m.httpDuration.WithLabelValues(route).Observe(d.Seconds())
}

// ObserveFlowRun records a completed flow-run attempt and its terminal state.
func (m *Metrics) ObserveFlowRun(toState string, d time.Duration) {
	if m == nil {
		return
	}
	m.runTransitions.WithLabelValues(toState).Inc()
	if d > 0 {
		m.runDuration.Observe(d.Seconds())
	}
}

// ObserveTask records a task attempt. outcome is "completed", "failed" or "cached".
func (m *Metrics) ObserveTask(outcome string, d time.Duration) {
	if m == nil {
		return
	}
	m.taskDuration.WithLabelValues(outcome).Observe(d.Seconds())
}

// ------------------------------------------------------- queue collector ---

// DesiredWorkers is the autoscaling target for one queue: enough workers to burn
// down the ready backlog at target-per-worker, clamped to [min, max]. Exported so
// the API can return the same number the metric reports.
func DesiredWorkers(ready, target, minW int, maxW *int) int {
	if target <= 0 {
		target = 5
	}
	want := int(math.Ceil(float64(ready) / float64(target)))
	if want < minW {
		want = minW
	}
	if maxW != nil && want > *maxW {
		want = *maxW
	}
	if want < 0 {
		want = 0
	}
	return want
}

// queueSource is the slice of the store the scrape-time collector needs. Keeping
// it narrow means a test double only implements two methods.
type queueSource interface {
	QueueStats(ctx context.Context) ([]store.QueueStat, error)
	ListWorkers(ctx context.Context) ([]core.WorkerInfo, error)
}

// queueCollector emits per-queue and worker gauges from a live store read on
// every scrape, so the numbers are never stale and nothing has to be pushed.
type queueCollector struct {
	store queueSource
}

var (
	descReady     = prometheus.NewDesc("primeflow_queue_ready", "Scheduled runs whose time has come, per queue.", []string{"queue"}, nil)
	descScheduled = prometheus.NewDesc("primeflow_queue_scheduled", "All scheduled runs, per queue.", []string{"queue"}, nil)
	descRunning   = prometheus.NewDesc("primeflow_queue_running", "Running/pending runs, per queue.", []string{"queue"}, nil)
	descFailed    = prometheus.NewDesc("primeflow_queue_failed_24h", "Failed runs in the last 24h, per queue.", []string{"queue"}, nil)
	descDesired   = prometheus.NewDesc("primeflow_queue_desired_workers", "Autoscaling target worker count, per queue.", []string{"queue"}, nil)
	descPaused    = prometheus.NewDesc("primeflow_queue_paused", "1 if the queue is paused.", []string{"queue"}, nil)
	descWOnline   = prometheus.NewDesc("primeflow_workers_online", "Workers with a fresh heartbeat.", nil, nil)
	descWTotal    = prometheus.NewDesc("primeflow_workers_total", "Workers that have ever checked in and not aged out.", nil, nil)
)

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descReady
	ch <- descScheduled
	ch <- descRunning
	ch <- descFailed
	ch <- descDesired
	ch <- descPaused
	ch <- descWOnline
	ch <- descWTotal
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stats, err := c.store.QueueStats(ctx)
	if err == nil {
		for _, q := range stats {
			g := func(d *prometheus.Desc, v float64) {
				ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, q.Name)
			}
			g(descReady, float64(q.Ready))
			g(descScheduled, float64(q.Scheduled))
			g(descRunning, float64(q.Running))
			g(descFailed, float64(q.Failed24h))
			g(descDesired, float64(DesiredWorkers(q.Ready, q.TargetReadyPerWorker, q.MinWorkers, q.MaxWorkers)))
			paused := 0.0
			if q.Paused {
				paused = 1
			}
			g(descPaused, paused)
		}
	}

	workers, err := c.store.ListWorkers(ctx)
	if err == nil {
		now := time.Now().UTC()
		online := 0
		for _, w := range workers {
			if w.Online(now, 90*time.Second) {
				online++
			}
		}
		ch <- prometheus.MustNewConstMetric(descWOnline, prometheus.GaugeValue, float64(online))
		ch <- prometheus.MustNewConstMetric(descWTotal, prometheus.GaugeValue, float64(len(workers)))
	}
}
