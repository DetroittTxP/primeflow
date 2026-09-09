package main

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/primex/primeflow/pkg/sdk"
)

// ------------------------------------------------------------ healthcheck ---
//
// http-healthcheck probes a list of URLs and fails the run if any of them is
// unhealthy. It is a real flow rather than a demo: one checkpointed task per
// target, keyed by URL, so a run that crashes halfway resumes without
// re-probing what it already measured, and adding a target next week replays
// the ones already recorded instead of shifting every checkpoint after it.

// HealthcheckParams is what an operator (or a schedule) supplies.
type HealthcheckParams struct {
	Targets        []string `json:"targets"`
	ExpectStatus   int      `json:"expect_status,omitempty"`   // default 200
	TimeoutSeconds int      `json:"timeout_seconds,omitempty"` // per target; default 10
}

// Probe is one target's outcome. Like every checkpointed value it has to
// round-trip through JSON.
type Probe struct {
	URL       string `json:"url"`
	Status    int    `json:"status"`
	LatencyMS int64  `json:"latency_ms"`
	Healthy   bool   `json:"healthy"`
	Error     string `json:"error,omitempty"`
}

// HealthcheckResult is the run result.
type HealthcheckResult struct {
	Probes    []Probe   `json:"probes"`
	Unhealthy int       `json:"unhealthy"`
	Worker    string    `json:"worker"`
	Queue     string    `json:"queue"`
	At        time.Time `json:"at"`
}

func httpHealthcheck(c *sdk.Context) (any, error) {
	p, err := sdk.Params[HealthcheckParams](c)
	if err != nil {
		return nil, err
	}
	if len(p.Targets) == 0 {
		// Bad input is not a transient failure: Permanent skips the retry
		// budget instead of burning it on the same empty list.
		return nil, sdk.Permanent(errors.New("targets must not be empty"))
	}
	expect := p.ExpectStatus
	if expect == 0 {
		expect = http.StatusOK
	}
	timeout := time.Duration(p.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	c.Info("probing", "targets", len(p.Targets), "expect", expect)

	probes := make([]Probe, 0, len(p.Targets))
	for _, raw := range p.Targets {
		url := strings.TrimSpace(raw)
		if url == "" {
			continue
		}
		// Keyed by URL, not by position. TaskRetries here rather than at the
		// flow level so one flaky endpoint retries on its own without
		// re-probing the targets that already answered.
		probe, err := sdk.Task(c, "probe", func(c *sdk.Context) (Probe, error) {
			return probeOnce(c, url, expect, timeout), nil
		},
			sdk.TaskKey("probe:"+url),
			sdk.TaskRetries(2),
			sdk.TaskRetryDelay(3*time.Second),
			sdk.TaskTimeout(timeout+5*time.Second),
		)
		if err != nil {
			return nil, err
		}
		probes = append(probes, probe)
	}

	unhealthy := 0
	for _, pr := range probes {
		if !pr.Healthy {
			unhealthy++
		}
	}

	// Artifacts before the error, always. A run that ends failed drops its
	// result, so anything written into the result is lost exactly when you
	// most want to read it -- but artifacts survive.
	_ = c.Table("probes", probes)
	_ = c.Markdown("summary", healthcheckMarkdown(probes, unhealthy, expect, c.Run().WorkQueue))

	if unhealthy > 0 {
		// Not Permanent: an endpoint that is down now may be up on the retry,
		// and that is exactly the signal a healthcheck exists to produce.
		return nil, fmt.Errorf("%d of %d targets unhealthy", unhealthy, len(probes))
	}
	return HealthcheckResult{
		Probes:    probes,
		Unhealthy: unhealthy,
		Worker:    workerName(),
		Queue:     c.Run().WorkQueue,
		At:        time.Now().UTC(),
	}, nil
}

// probeOnce never returns an error: a target that refuses the connection is a
// recorded result, not a task failure. Returning an error here would retry the
// task and eventually fail the run before the other targets were measured.
func probeOnce(c *sdk.Context, url string, expect int, timeout time.Duration) Probe {
	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequestWithContext(c, http.MethodGet, url, nil)
	if err != nil {
		return Probe{URL: url, Error: err.Error()}
	}
	req.Header.Set("User-Agent", "primeflow-healthcheck/1")

	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		c.Warn("probe failed", "url", url, "err", err)
		return Probe{URL: url, LatencyMS: latency, Error: err.Error()}
	}
	defer resp.Body.Close()

	healthy := resp.StatusCode == expect
	if !healthy {
		c.Warn("unexpected status", "url", url, "status", resp.StatusCode, "expect", expect)
	}
	return Probe{URL: url, Status: resp.StatusCode, LatencyMS: latency, Healthy: healthy}
}

func healthcheckMarkdown(probes []Probe, unhealthy, expect int, queue string) string {
	var b strings.Builder
	if unhealthy == 0 {
		fmt.Fprintf(&b, "### All %d targets healthy\n\n", len(probes))
	} else {
		fmt.Fprintf(&b, "### %d of %d targets unhealthy\n\n", unhealthy, len(probes))
	}
	fmt.Fprintf(&b, "Expecting HTTP %d, from `%s` on pool `%s`.\n\n", expect, workerName(), queue)
	b.WriteString("| Target | Status | Latency |\n|---|---|---|\n")
	for _, p := range probes {
		status := fmt.Sprintf("%d", p.Status)
		if p.Error != "" {
			status = "`" + p.Error + "`"
		}
		mark := "ok"
		if !p.Healthy {
			mark = "**FAIL**"
		}
		fmt.Fprintf(&b, "| %s | %s %s | %dms |\n", p.URL, status, mark, p.LatencyMS)
	}
	return b.String()
}
