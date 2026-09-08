// Command primex-worker is a worked example of a PrimeFlow worker shaped
// around PrimeX: VM provisioning against VMware Cloud Director, and a nightly
// metering collection.
//
// It is deliberately runnable without a real vCD: the client calls are stubbed
// so you can start the stack, trigger the deployments and watch retries,
// durable waits and crash recovery behave.
//
//	go run ./examples/primex-worker
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/primex/primeflow/pkg/primeflow/worker"
	"github.com/primex/primeflow/pkg/sdk"
)

// ---------------------------------------------------------- provisioning ---

// ProvisionParams is what an operator (or the PrimeX API) supplies to start a
// VM build.
type ProvisionParams struct {
	OrgName  string `json:"org_name"`
	VDCName  string `json:"vdc_name"`
	Template string `json:"template"`
	Name     string `json:"name"`
	CPU      int    `json:"cpu"`
	MemoryMB int    `json:"memory_mb"`
	Owner    string `json:"owner"`
}

// VM is the shape returned by the provisioning steps. Anything a task returns
// must round-trip through JSON, because that is how it is checkpointed.
type VM struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Href      string    `json:"href"`
	CreatedAt time.Time `json:"created_at"`
}

func provisionVM(c *sdk.Context) (any, error) {
	p, err := sdk.Params[ProvisionParams](c)
	if err != nil {
		return nil, err
	}
	if p.OrgName == "" || p.Name == "" {
		// A permanent error skips the retry budget: retrying a malformed
		// request just wastes the queue.
		return nil, sdk.Permanent(errors.New("org_name and name are required"))
	}
	c.Info("provisioning requested", "org", p.OrgName, "vdc", p.VDCName, "template", p.Template)

	// 1. Look the org up. Cached across runs for an hour: every provisioning
	//    run in the same org would otherwise repeat the same lookup.
	orgID, err := sdk.Task(c, "lookup-org", func(c *sdk.Context) (string, error) {
		return vcdLookupOrg(c, p.OrgName)
	},
		sdk.TaskRetries(3),
		sdk.TaskRetryDelay(2*time.Second),
		sdk.TaskCache("vcd:org:"+p.OrgName, time.Hour),
	)
	if err != nil {
		return nil, err
	}

	// 2. Create the VM. This is the expensive, side-effecting step, so it gets
	//    its own checkpoint: a crash after this point never builds a second VM.
	vm, err := sdk.Task(c, "create-vm", func(c *sdk.Context) (VM, error) {
		c.Info("calling vCD instantiate", "org_id", orgID)
		return vcdCreateVM(c, orgID, p)
	},
		sdk.TaskRetries(2),
		sdk.TaskRetryDelay(10*time.Second),
		sdk.TaskTimeout(2*time.Minute),
	)
	if err != nil {
		return nil, err
	}
	c.Info("vm created", "vm_id", vm.ID)

	// 3. Wait for the build to settle. This releases the worker slot: the run
	//    is rescheduled for the wake time and resumes here, with steps 1 and 2
	//    replayed from their checkpoints rather than re-executed.
	if err := sdk.Sleep(c, "settle", 20*time.Second); err != nil {
		return nil, err
	}

	// 4. Poll until powered on, with a bounded number of attempts.
	if err := sdk.Do(c, "await-power-on", func(c *sdk.Context) error {
		for i := 0; i < 10; i++ {
			ok, err := vcdPoweredOn(c, vm.ID)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
			select {
			case <-time.After(2 * time.Second):
			case <-c.Done():
				return c.Err()
			}
		}
		return fmt.Errorf("vm %s did not power on in time", vm.ID)
	}, sdk.TaskRetries(1)); err != nil {
		return nil, err
	}

	// 5. Register it for metering, then hand off to a follow-up deployment if
	//    one exists. A missing deployment is not a reason to fail the build.
	if err := sdk.Do(c, "register-metering", func(c *sdk.Context) error {
		return meteringRegister(c, vm.ID, p.Owner)
	}, sdk.TaskRetries(5), sdk.TaskRetryDelay(3*time.Second)); err != nil {
		return nil, err
	}

	_ = c.Markdown("summary", fmt.Sprintf(
		"### VM provisioned\n\n- **Name:** %s\n- **ID:** `%s`\n- **Org:** %s / %s\n- **Owner:** %s\n- **Spec:** %d vCPU, %d MB\n",
		vm.Name, vm.ID, p.OrgName, p.VDCName, p.Owner, p.CPU, p.MemoryMB))
	_ = c.Link("console", vm.Href, "Open in Cloud Director")

	return vm, nil
}

// ------------------------------------------------------------- metering ---

// MeteringParams selects what to collect.
type MeteringParams struct {
	Orgs []string `json:"orgs"`
	Day  string   `json:"day,omitempty"` // YYYY-MM-DD; empty means yesterday
}

type meteringRow struct {
	Org      string  `json:"org"`
	VMCount  int     `json:"vm_count"`
	VCPUHour float64 `json:"vcpu_hours"`
	GBHour   float64 `json:"gb_hours"`
}

func collectMetering(c *sdk.Context) (any, error) {
	p, err := sdk.Params[MeteringParams](c)
	if err != nil {
		return nil, err
	}
	// The day is frozen in a checkpoint before anything uses it, because it
	// feeds every task key below. Derived inline, a run that starts at 23:59
	// and replays after midnight would compute a different day, miss every
	// checkpoint, and collect the whole night again under new keys.
	day, err := sdk.Task(c, "resolve-day", func(*sdk.Context) (string, error) {
		if p.Day != "" {
			return p.Day, nil
		}
		return time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02"), nil
	}, sdk.TaskKey("resolve-day"))
	if err != nil {
		return nil, err
	}

	orgs := p.Orgs
	if len(orgs) == 0 {
		orgs = []string{"acme", "globex", "initech"}
	}
	c.Info("collecting metering", "day", day, "orgs", len(orgs))

	rows := make([]meteringRow, 0, len(orgs))
	for _, org := range orgs {
		// The key is derived from the data, not from loop position, so adding
		// an org to the list on a later attempt does not shift every
		// checkpoint and re-run work that already succeeded.
		row, err := sdk.Task(c, "collect", func(c *sdk.Context) (meteringRow, error) {
			return vcdCollectUsage(c, org, day)
		},
			sdk.TaskKey("collect:"+org+":"+day),
			sdk.TaskRetries(4),
			sdk.TaskRetryDelay(2*time.Second),
		)
		if err != nil {
			return nil, fmt.Errorf("collecting %s: %w", org, err)
		}
		rows = append(rows, row)
	}

	if err := sdk.Do(c, "write-billing", func(c *sdk.Context) error {
		return billingWrite(c, day, rows)
	}, sdk.TaskRetries(3)); err != nil {
		return nil, err
	}

	_ = c.Table("usage-"+day, rows)
	return map[string]any{"day": day, "orgs": len(rows)}, nil
}

// -------------------------------------------------------- stubbed vCD ------
//
// Everything below stands in for the real integration. The random failures are
// intentional: they exercise the retry and checkpoint machinery so you can see
// it work before wiring the real client in.

func vcdLookupOrg(c *sdk.Context, name string) (string, error) {
	if rand.Float64() < 0.2 {
		return "", errors.New("vcd: 503 from /api/org (transient)")
	}
	c.Debug("resolved org", "name", name)
	return "urn:vcloud:org:" + name, nil
}

func vcdCreateVM(c *sdk.Context, orgID string, p ProvisionParams) (VM, error) {
	time.Sleep(500 * time.Millisecond)
	if rand.Float64() < 0.15 {
		return VM{}, errors.New("vcd: task failed — insufficient resources in vDC")
	}
	id := fmt.Sprintf("urn:vcloud:vm:%d", time.Now().UnixNano()%1e9)
	return VM{
		ID:   id,
		Name: p.Name,
		Href: "https://vcd.example.com/tenant/" + p.OrgName + "/vm/" + id,
		// A real integration returns the server's timestamp; using the
		// server's value rather than time.Now() also keeps the checkpoint
		// stable across replays.
		CreatedAt: time.Now().UTC(),
	}, nil
}

func vcdPoweredOn(c *sdk.Context, vmID string) (bool, error) {
	return rand.Float64() < 0.6, nil
}

func meteringRegister(c *sdk.Context, vmID, owner string) error {
	if rand.Float64() < 0.25 {
		return errors.New("metering: connection reset")
	}
	c.Info("registered for metering", "vm_id", vmID, "owner", owner)
	return nil
}

func vcdCollectUsage(c *sdk.Context, org, day string) (meteringRow, error) {
	if rand.Float64() < 0.2 {
		return meteringRow{}, errors.New("vcd: rate limited")
	}
	return meteringRow{
		Org: org, VMCount: 5 + rand.Intn(40),
		VCPUHour: float64(50 + rand.Intn(400)), GBHour: float64(200 + rand.Intn(2000)),
	}, nil
}

func billingWrite(c *sdk.Context, day string, rows []meteringRow) error {
	c.Info("wrote billing rows", "day", day, "rows", len(rows))
	return nil
}

// ---------------------------------------------------- fleet (sub-flows) ---

// FleetParams asks for several VMs in one org, provisioned as child runs.
type FleetParams struct {
	OrgName  string `json:"org_name"`
	Template string `json:"template"`
	Count    int    `json:"count"`
}

// provisionFleet fans out to the provision-vm-standard deployment once per VM
// and waits, durably, for every child to finish — releasing its worker slot in
// between. It is the worked example of RunDeploymentAndWait.
func provisionFleet(c *sdk.Context) (any, error) {
	p, err := sdk.Params[FleetParams](c)
	if err != nil {
		return nil, err
	}
	if p.Count <= 0 || p.Count > 20 {
		return nil, sdk.Permanent(errors.New("count must be between 1 and 20"))
	}
	c.Info("provisioning fleet", "org", p.OrgName, "count", p.Count)

	built := make([]string, 0, p.Count)
	for i := 0; i < p.Count; i++ {
		child, err := c.RunDeploymentAndWait("provision-vm-standard", ProvisionParams{
			OrgName:  p.OrgName,
			Template: p.Template,
			Name:     fmt.Sprintf("%s-fleet-%02d", p.OrgName, i+1),
			CPU:      2, MemoryMB: 4096,
		})
		if err != nil {
			return nil, err
		}
		var vm VM
		_ = child.Into(&vm)
		built = append(built, vm.ID)
	}
	_ = c.Markdown("fleet", fmt.Sprintf("### Fleet ready\n\n%d VMs for **%s**: `%v`", len(built), p.OrgName, built))
	return map[string]any{"vm_ids": built}, nil
}

// ----------------------------------------------------------- add-numbers ---

// AddParams is what an operator supplies to the add-numbers flow.
type AddParams struct {
	A int `json:"a"`
	B int `json:"b"`
}

// AddResult is the run result. Like every checkpointed value it has to
// round-trip through JSON.
type AddResult struct {
	A   int `json:"a"`
	B   int `json:"b"`
	Sum int `json:"sum"`
}

// addNumbers is the smallest useful flow: no external calls, nothing to undo.
// The arithmetic is wrapped in a Task only so it appears as its own lane on the
// console timeline -- a pure computation needs no checkpoint of its own.
func addNumbers(c *sdk.Context) (any, error) {
	p, err := sdk.Params[AddParams](c)
	if err != nil {
		return nil, err
	}
	c.Info("adding", "a", p.A, "b", p.B)

	sum, err := sdk.Task(c, "add", func(c *sdk.Context) (int, error) {
		return p.A + p.B, nil
	})
	if err != nil {
		return nil, err
	}

	_ = c.Markdown("result", fmt.Sprintf("`%d + %d` = **%d**", p.A, p.B, sum))
	return AddResult{A: p.A, B: p.B, Sum: sum}, nil
}

// -------------------------------------------------------- onboard-tenant ---

// OnboardParams drives the multi-stage pipeline.
type OnboardParams struct {
	OrgName string `json:"org_name"`
	Tier    string `json:"tier,omitempty"`    // standard | premium
	FailAt  string `json:"fail_at,omitempty"` // stage to fail on purpose
}

// onboardStages is the pipeline in order. Naming the stages in one place is
// what keeps fail_at honest: an unknown stage is rejected up front instead of
// quietly never firing.
var onboardStages = []string{
	"validate", "reserve-quota", "create-network", "attach-storage", "apply-policy", "notify",
}

// Quota is what reserve-quota books. It is cached per org and tier, so a second
// onboarding for the same tenant reuses it.
type Quota struct {
	Org      string `json:"org"`
	Tier     string `json:"tier"`
	VCPU     int    `json:"vcpu"`
	MemoryGB int    `json:"memory_gb"`
}

// Network and Storage are the two resources the middle stages build.
type Network struct {
	ID   string `json:"id"`
	CIDR string `json:"cidr"`
}

type Storage struct {
	ID       string `json:"id"`
	Datapool string `json:"datapool"`
	Attempts int    `json:"attempts"`
}

// OnboardResult is the run result.
type OnboardResult struct {
	Org     string  `json:"org"`
	Tier    string  `json:"tier"`
	Quota   Quota   `json:"quota"`
	Network Network `json:"network"`
	Storage Storage `json:"storage"`
	Stages  int     `json:"stages"`
}

// onboardTenant is six stages, each its own checkpoint, so the console shows
// where a run got to rather than only whether it finished.
//
// Every stage lands in pf_task_runs as it happens: state, attempts, duration
// and result, one lane per stage on the run's timeline. That is what makes the
// three interesting cases legible without reading a log --
//
//   - reserve-quota is cached across runs, so onboarding the same org twice
//     shows the second run reusing the first stage instead of re-reserving;
//   - attach-storage fails its first two attempts on purpose, so its lane
//     always reads x3 and the retry policy is visible rather than a matter of
//     luck;
//   - fail_at stops one named stage, leaving the stages before it COMPLETED
//     and the rest never started, which is the shape of a real partial failure.
func onboardTenant(c *sdk.Context) (any, error) {
	p, err := sdk.Params[OnboardParams](c)
	if err != nil {
		return nil, err
	}
	if p.FailAt != "" && !slices.Contains(onboardStages, p.FailAt) {
		return nil, sdk.Permanent(fmt.Errorf("fail_at %q is not a stage; want one of %v", p.FailAt, onboardStages))
	}
	tier := p.Tier
	if tier == "" {
		tier = "standard"
	}

	// 1. Cheap validation, still its own checkpoint: the console then shows it
	//    passed even on a run a later stage failed.
	org, err := sdk.Task(c, "validate", func(c *sdk.Context) (string, error) {
		if err := stageGate(p, "validate"); err != nil {
			return "", err
		}
		if p.OrgName == "" {
			return "", sdk.Permanent(errors.New("org_name is required"))
		}
		return p.OrgName, nil
	})
	if err != nil {
		return nil, err
	}
	c.Info("onboarding tenant", "org", org, "tier", tier, "stages", len(onboardStages))

	// 2. Quota is the same answer for the same org and tier, so it is cached
	//    across runs rather than merely checkpointed within one.
	quota, err := sdk.Task(c, "reserve-quota", func(c *sdk.Context) (Quota, error) {
		if err := stageGate(p, "reserve-quota"); err != nil {
			return Quota{}, err
		}
		if err := work(c, 900*time.Millisecond); err != nil {
			return Quota{}, err
		}
		q := Quota{Org: org, Tier: tier, VCPU: 16, MemoryGB: 64}
		if tier == "premium" {
			q.VCPU, q.MemoryGB = 64, 256
		}
		return q, nil
	}, sdk.TaskCache("quota:"+org+":"+tier, 10*time.Minute), sdk.TaskRetries(2))
	if err != nil {
		return nil, err
	}

	// 3. The slow stage, so there is something with real width on the timeline.
	net, err := sdk.Task(c, "create-network", func(c *sdk.Context) (Network, error) {
		if err := stageGate(p, "create-network"); err != nil {
			return Network{}, err
		}
		if err := work(c, 2*time.Second); err != nil {
			return Network{}, err
		}
		return Network{ID: fmt.Sprintf("urn:vcloud:network:%d", time.Now().UnixNano()%1e9), CIDR: "10.42.0.0/16"}, nil
	}, sdk.TaskRetries(2), sdk.TaskRetryDelay(2*time.Second))
	if err != nil {
		return nil, err
	}

	// 4. Deterministically flaky. The counter lives in this invocation, so the
	//    first two attempts always fail and the third always succeeds -- a
	//    retry you can point at, not one you wait for the dice to produce. A
	//    replay skips the stage entirely: the checkpoint is already completed.
	tries := 0
	store, err := sdk.Task(c, "attach-storage", func(c *sdk.Context) (Storage, error) {
		tries++
		if err := stageGate(p, "attach-storage"); err != nil {
			return Storage{}, err
		}
		if err := work(c, 400*time.Millisecond); err != nil {
			return Storage{}, err
		}
		if tries <= 2 {
			return Storage{}, fmt.Errorf("storage array busy, attempt %d", tries)
		}
		return Storage{ID: "urn:vcloud:disk:" + org, Datapool: "gold", Attempts: tries}, nil
	}, sdk.TaskRetries(3), sdk.TaskRetryDelay(time.Second))
	if err != nil {
		return nil, err
	}

	// 5. Policy, then 6. the notification. Do is Task without a result: it is
	//    still a checkpoint and still a lane.
	if err := sdk.Do(c, "apply-policy", func(c *sdk.Context) error {
		if err := stageGate(p, "apply-policy"); err != nil {
			return err
		}
		return work(c, 1500*time.Millisecond)
	}, sdk.TaskRetries(2)); err != nil {
		return nil, err
	}

	if err := sdk.Do(c, "notify", func(c *sdk.Context) error {
		if err := stageGate(p, "notify"); err != nil {
			return err
		}
		c.Info("tenant ready", "org", org, "network", net.ID, "storage", store.ID)
		return work(c, 200*time.Millisecond)
	}, sdk.TaskRetries(4), sdk.TaskRetryDelay(2*time.Second)); err != nil {
		return nil, err
	}

	_ = c.Markdown("onboarded", fmt.Sprintf(
		"### Tenant onboarded\n\n- **Org:** %s (%s)\n- **Quota:** %d vCPU, %d GB\n- **Network:** `%s` %s\n- **Storage:** `%s` on %s after %d attempts\n",
		org, tier, quota.VCPU, quota.MemoryGB, net.ID, net.CIDR, store.ID, store.Datapool, store.Attempts))

	return OnboardResult{
		Org: org, Tier: tier, Quota: quota, Network: net, Storage: store,
		Stages: len(onboardStages),
	}, nil
}

// stageGate is the one-line check every stage carries so fail_at can stop the
// pipeline anywhere. The error is permanent: a stage failed on request should
// not then burn the retry budget.
func stageGate(p OnboardParams, stage string) error {
	if p.FailAt == stage {
		return sdk.Permanent(fmt.Errorf("stage %q failed on purpose (fail_at)", stage))
	}
	return nil
}

// work stands in for the call a stage would really make. It honours
// cancellation, so cancelling a run stops inside the stage rather than waiting
// for the next checkpoint boundary.
func work(c *sdk.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-c.Done():
		return c.Err()
	}
}

// ------------------------------------------------------------- site-loop ---

// LoopParams tunes the per-site smoke test: how long to stay busy, and how
// finely to slice that up.
type LoopParams struct {
	Seconds     int    `json:"seconds"`      // total wall time, default 20
	StepSeconds int    `json:"step_seconds"` // one checkpoint per step, default 2
	Label       string `json:"label,omitempty"`
}

// LoopTick is one checkpointed slice of the loop. Recording the worker on
// every tick is what makes a resumed run legible: the ticks before a crash
// carry the site that started it, the ones after carry the site that finished.
type LoopTick struct {
	N      int       `json:"n"`
	Worker string    `json:"worker"`
	At     time.Time `json:"at"`
}

// LoopResult is what the run returns.
type LoopResult struct {
	Label   string  `json:"label,omitempty"`
	Worker  string  `json:"worker"`
	Queue   string  `json:"queue"`
	Ticks   int     `json:"ticks"`
	Seconds float64 `json:"seconds"`
}

// siteLoop holds a worker slot for a fixed stretch — twenty seconds by
// default — in checkpointed ticks. It is the flow to trigger at every site at
// once: the run stays RUNNING long enough to watch dispatch reach each site,
// to push a pool up against its concurrency limit, and to see which worker
// picked the work up. Because every tick is its own checkpoint, killing a site
// mid-loop resumes at the tick it reached rather than restarting the twenty
// seconds.
func siteLoop(c *sdk.Context) (any, error) {
	p, err := sdk.Params[LoopParams](c)
	if err != nil {
		return nil, err
	}
	total, step := p.Seconds, p.StepSeconds
	if total <= 0 {
		total = 20
	}
	if step <= 0 {
		step = 2
	}
	if total > 300 {
		// Past this it stops being a smoke test and starts being a lease
		// expiry, which crash recovery already covers.
		return nil, sdk.Permanent(fmt.Errorf("seconds must be 300 or less, got %d", total))
	}
	if step > total {
		step = total
	}
	ticks := (total + step - 1) / step

	who := workerName()
	started := time.Now()
	c.Info("site loop starting", "worker", who, "pool", c.Run().WorkQueue,
		"seconds", total, "ticks", ticks, "step_seconds", step)

	for i := 1; i <= ticks; i++ {
		// Keyed by position, because here position is the data: tick 3 is
		// tick 3 on a replay too, and its stored result returns instantly.
		tick, err := sdk.Task(c, "tick", func(c *sdk.Context) (LoopTick, error) {
			select {
			case <-time.After(time.Duration(step) * time.Second):
			case <-c.Done():
				return LoopTick{}, c.Err()
			}
			return LoopTick{N: i, Worker: workerName(), At: time.Now().UTC()}, nil
		}, sdk.TaskKey(fmt.Sprintf("tick:%02d", i)))
		if err != nil {
			return nil, err
		}
		c.Info("tick", "n", tick.N, "of", ticks, "worker", tick.Worker)
	}

	// Elapsed is this attempt's wall time, not the run's: a resumed run replays
	// its finished ticks from their checkpoints and only sleeps for the rest.
	elapsed := time.Since(started).Seconds()
	_ = c.Markdown("loop", fmt.Sprintf(
		"### Site loop finished\n\n- **Worker:** %s\n- **Pool:** %s\n- **Ticks:** %d × %ds\n- **Elapsed:** %.1fs\n",
		who, c.Run().WorkQueue, ticks, step, elapsed))

	return LoopResult{
		Label: p.Label, Worker: who, Queue: c.Run().WorkQueue,
		Ticks: ticks, Seconds: elapsed,
	}, nil
}

// workerName is the site this run landed on. It mirrors what the worker
// registers itself under, so a result traces back to one site VM.
func workerName() string {
	if n := os.Getenv("PRIMEFLOW_WORKER_NAME"); n != "" {
		return n
	}
	host, _ := os.Hostname()
	if host == "" {
		host = "worker"
	}
	return host
}

// ------------------------------------------------------------ site-audit ---
//
// site-audit is the deep per-site flow. Where onboard-tenant is a straight line
// of six stages, this one has the three shapes a real site operation grows into
// once it touches something it cannot simply retry:
//
//   - a fan-out whose checkpoint keys come from the data (one probe per
//     subsystem), so adding a subsystem does not renumber the others;
//   - resources that must be torn down in reverse if a later stage fails --
//     the saga below -- because a maintenance window left open and a snapshot
//     left behind are exactly what a half-finished run leaks;
//   - a durable wait long enough to hand the worker slot back, followed by
//     bounded polling, which is what "wait for it to settle, then check"
//     actually costs on a site VM.

// AuditParams drives one site's audit. Everything is optional: the defaults are
// a complete, ~40s audit, so an operator can trigger it with `{}`.
type AuditParams struct {
	Site        string   `json:"site,omitempty"`         // label for the report; the queue decides where it runs
	Subsystems  []string `json:"subsystems,omitempty"`   // default: control-plane, storage, network, telemetry
	BakeSeconds int      `json:"bake_seconds,omitempty"` // durable wait after the snapshot, default 8
	Window      string   `json:"window,omitempty"`       // YYYY-MM-DDTHH; empty means "this hour"
	FailAt      string   `json:"fail_at,omitempty"`      // stage to fail on purpose, to watch the rollback
}

// auditStages is the pipeline in order, and the whitelist fail_at is checked
// against, so a typo is rejected up front instead of quietly never firing.
var auditStages = []string{"plan", "probe", "open-window", "snapshot", "verify", "seal"}

// auditSubsystems is what a site is probed for when the caller names nothing.
var auditSubsystems = []string{"control-plane", "storage", "network", "telemetry"}

// Probe is one subsystem's reading. Attempts is carried in the result rather
// than only in the checkpoint so the report itself shows which probe was flaky.
type Probe struct {
	Subsystem string `json:"subsystem"`
	Healthy   bool   `json:"healthy"`
	LatencyMS int    `json:"latency_ms"`
	Attempts  int    `json:"attempts"`
}

// Window and Snapshot are the two resources the audit creates, and therefore
// the two things the saga has to be able to undo.
type Window struct {
	ID     string `json:"id"`
	Opened string `json:"opened"`
}

type Snapshot struct {
	ID      string `json:"id"`
	SizeGB  int    `json:"size_gb"`
	Subsyss int    `json:"subsystems"`
}

// AuditResult is the run result, and the row fleet-audit puts in its table.
type AuditResult struct {
	Site       string  `json:"site"`
	Worker     string  `json:"worker"`
	Queue      string  `json:"queue"`
	Window     string  `json:"window"`
	Probes     []Probe `json:"probes"`
	Unhealthy  int     `json:"unhealthy"`
	SnapshotID string  `json:"snapshot_id"`
	Sealed     bool    `json:"sealed"`
	Seconds    float64 `json:"seconds"`
}

// ------------------------------------------------------------------ saga ---

// saga is the list of compensations a run has earned so far, oldest first. It
// is rebuilt on every replay: the forward stages return from their checkpoints
// without re-executing, but they still push their undo, so a resumed run knows
// exactly as much about what exists as the run that created it did.
type saga struct {
	steps []sagaStep
}

type sagaStep struct {
	name string
	undo func(*sdk.Context) error
}

func (s *saga) push(name string, undo func(*sdk.Context) error) {
	s.steps = append(s.steps, sagaStep{name: name, undo: undo})
}

// unwind runs every compensation in reverse -- the last thing built is the
// first thing torn down. Each undo is itself a checkpointed task, so a rollback
// interrupted halfway resumes at the step it reached instead of deleting a
// resource twice, which for a real snapshot is the difference between a clean
// rollback and an error nobody can distinguish from success.
func (s *saga) unwind(c *sdk.Context, cause error) error {
	for i := len(s.steps) - 1; i >= 0; i-- {
		st := s.steps[i]
		c.Warn("compensating", "step", st.name, "because", cause.Error())
		if err := sdk.Do(c, "undo-"+st.name, st.undo,
			sdk.TaskKey("undo:"+st.name),
			sdk.TaskRetries(3),
			sdk.TaskRetryDelay(time.Second),
		); err != nil {
			// A failed rollback is worse than the original failure: it is the
			// case a human has to look at, so it replaces the cause.
			return sdk.Permanent(fmt.Errorf("rollback of %s failed (original failure: %v): %w", st.name, cause, err))
		}
	}
	return nil
}

// siteAudit audits one site: probe it, open a maintenance window, snapshot it,
// let it settle, verify, seal. A failure anywhere after the window is opened
// unwinds everything already built, in reverse.
func siteAudit(c *sdk.Context) (any, error) {
	p, err := sdk.Params[AuditParams](c)
	if err != nil {
		return nil, err
	}
	if p.FailAt != "" && !slices.Contains(auditStages, p.FailAt) {
		return nil, sdk.Permanent(fmt.Errorf("fail_at %q is not a stage; want one of %v", p.FailAt, auditStages))
	}
	subsystems := p.Subsystems
	if len(subsystems) == 0 {
		subsystems = auditSubsystems
	}
	bake := p.BakeSeconds
	if bake <= 0 {
		bake = 8
	}
	started := time.Now()
	who := workerName()

	// 1. The window is frozen in a checkpoint before anything derives a key
	//    from it. Computed inline, a run that starts at 10:59 and replays at
	//    11:00 would look up a different hour, miss every cache key below, and
	//    re-probe a site it had already finished.
	window, err := sdk.Task(c, "plan", func(*sdk.Context) (string, error) {
		if err := auditGate(p, "plan"); err != nil {
			return "", err
		}
		if p.Window != "" {
			return p.Window, nil
		}
		return time.Now().UTC().Format("2006-01-02T15"), nil
	}, sdk.TaskKey("plan"))
	if err != nil {
		return nil, err
	}

	site := p.Site
	if site == "" {
		site = c.Run().WorkQueue
	}
	c.Info("audit starting", "site", site, "worker", who, "window", window,
		"subsystems", len(subsystems), "bake_seconds", bake)

	var undo saga

	// 2. One probe per subsystem, keyed by the subsystem name rather than by
	//    loop position: inserting a subsystem at the front on a later attempt
	//    then leaves every other probe's checkpoint where it was instead of
	//    shifting all of them and re-probing a site that was already done.
	//
	//    storage fails its first two attempts every time. A retry you can point
	//    at beats one you wait for the dice to produce, and it makes the x3 in
	//    that lane part of the fixture rather than a flake.
	tries := map[string]int{}
	probes := make([]Probe, 0, len(subsystems))
	for _, sub := range subsystems {
		probe, pErr := sdk.Task(c, "probe", func(c *sdk.Context) (Probe, error) {
			tries[sub]++
			if err := auditGate(p, "probe"); err != nil {
				return Probe{}, err
			}
			if err := work(c, 250*time.Millisecond); err != nil {
				return Probe{}, err
			}
			if sub == "storage" && tries[sub] <= 2 {
				return Probe{}, fmt.Errorf("%s: array busy, attempt %d", sub, tries[sub])
			}
			return Probe{
				Subsystem: sub,
				Healthy:   sub != "telemetry", // telemetry is degraded on purpose: a finding, not a failure
				LatencyMS: 20 + rand.Intn(180),
				Attempts:  tries[sub],
			}, nil
		},
			// Explicit keys are not decoration here. A task nested inside a
			// retried parent that relied on ordinals would number itself
			// differently on the second attempt and miss its own checkpoint.
			sdk.TaskKey("probe:"+sub),
			sdk.TaskRetries(3),
			sdk.TaskRetryDelay(time.Second),
		)
		if pErr != nil {
			return nil, pErr
		}
		probes = append(probes, probe)
	}
	unhealthy := 0
	for _, pr := range probes {
		if !pr.Healthy {
			unhealthy++
		}
	}
	c.Info("probes complete", "count", len(probes), "unhealthy", unhealthy)

	// 3. First resource. From here on a failure has something to clean up.
	win, err := sdk.Task(c, "open-window", func(c *sdk.Context) (Window, error) {
		if err := auditGate(p, "open-window"); err != nil {
			return Window{}, err
		}
		if err := work(c, 600*time.Millisecond); err != nil {
			return Window{}, err
		}
		return Window{ID: fmt.Sprintf("win-%s-%s", site, window), Opened: time.Now().UTC().Format(time.RFC3339)}, nil
	}, sdk.TaskKey("open-window"), sdk.TaskRetries(2))
	if err != nil {
		return nil, err
	}
	undo.push("open-window", func(c *sdk.Context) error {
		c.Info("closing maintenance window", "window_id", win.ID)
		return work(c, 300*time.Millisecond)
	})

	// 4. The expensive, side-effecting step: its own checkpoint and its own
	//    timeout, so a crash after it never takes a second snapshot.
	snap, err := sdk.Task(c, "snapshot", func(c *sdk.Context) (Snapshot, error) {
		if err := auditGate(p, "snapshot"); err != nil {
			return Snapshot{}, err
		}
		if err := work(c, 1200*time.Millisecond); err != nil {
			return Snapshot{}, err
		}
		return Snapshot{
			ID:      fmt.Sprintf("snap-%s-%d", site, time.Now().UnixNano()%1e6),
			SizeGB:  40 + rand.Intn(200),
			Subsyss: len(probes),
		}, nil
	},
		sdk.TaskKey("snapshot"),
		sdk.TaskRetries(2),
		sdk.TaskRetryDelay(3*time.Second),
		sdk.TaskTimeout(2*time.Minute),
	)
	if err != nil {
		return nil, errors.Join(err, undo.unwind(c, err))
	}
	undo.push("snapshot", func(c *sdk.Context) error {
		c.Info("deleting snapshot", "snapshot_id", snap.ID)
		return work(c, 500*time.Millisecond)
	})
	c.Info("snapshot taken", "snapshot_id", snap.ID, "size_gb", snap.SizeGB)

	// 5. Let it settle. Past the suspend threshold this hands the worker slot
	//    back and the run is rescheduled for the wake time; under it the run
	//    simply blocks. Either way the stages above replay from their
	//    checkpoints rather than running again.
	if err := sdk.Sleep(c, "bake", time.Duration(bake)*time.Second); err != nil {
		if _, suspended := sdk.IsSuspend(err); suspended {
			return nil, err // not a failure: nothing to unwind
		}
		return nil, errors.Join(err, undo.unwind(c, err))
	}

	// 6. Bounded polling, with each attempt its own checkpoint so a crash
	//    mid-verify resumes on the attempt it reached instead of restarting the
	//    poll. This is the nested-task shape: a keyed task inside a loop inside
	//    a stage.
	verified, err := sdk.Task(c, "verify", func(c *sdk.Context) (int, error) {
		if err := auditGate(p, "verify"); err != nil {
			return 0, err
		}
		for attempt := 1; attempt <= 5; attempt++ {
			ok, aErr := sdk.Task(c, "verify-attempt", func(c *sdk.Context) (bool, error) {
				if err := work(c, 400*time.Millisecond); err != nil {
					return false, err
				}
				// Settles by the third look, deterministically.
				return attempt >= 3, nil
			}, sdk.TaskKey(fmt.Sprintf("verify:%s:%02d", snap.ID, attempt)))
			if aErr != nil {
				return 0, aErr
			}
			if ok {
				return attempt, nil
			}
		}
		return 0, fmt.Errorf("snapshot %s did not settle in 5 attempts", snap.ID)
	}, sdk.TaskKey("verify"), sdk.TaskRetries(1))
	if err != nil {
		return nil, errors.Join(err, undo.unwind(c, err))
	}
	c.Info("snapshot verified", "attempts", verified)

	// 7. Sealing is the commit point. Once it succeeds the window is closed
	//    deliberately rather than rolled back, so the saga is dropped.
	if err := sdk.Do(c, "seal", func(c *sdk.Context) error {
		if err := auditGate(p, "seal"); err != nil {
			return err
		}
		return work(c, 400*time.Millisecond)
	}, sdk.TaskKey("seal"), sdk.TaskRetries(2), sdk.TaskRetryDelay(2*time.Second)); err != nil {
		return nil, errors.Join(err, undo.unwind(c, err))
	}
	if err := sdk.Do(c, "close-window", func(c *sdk.Context) error {
		c.Info("closing maintenance window", "window_id", win.ID)
		return work(c, 300*time.Millisecond)
	}, sdk.TaskKey("close-window"), sdk.TaskRetries(3)); err != nil {
		return nil, err
	}

	elapsed := time.Since(started).Seconds()
	_ = c.Table("probes-"+site, probes)
	_ = c.Markdown("audit", fmt.Sprintf(
		"### Audit of %s\n\n- **Worker:** %s (pool `%s`)\n- **Window:** %s\n- **Probes:** %d, %d degraded\n"+
			"- **Snapshot:** `%s` (%d GB), verified on attempt %d\n- **Elapsed:** %.1fs\n",
		site, who, c.Run().WorkQueue, window, len(probes), unhealthy, snap.ID, snap.SizeGB, verified, elapsed))

	return AuditResult{
		Site: site, Worker: who, Queue: c.Run().WorkQueue, Window: window,
		Probes: probes, Unhealthy: unhealthy, SnapshotID: snap.ID,
		Sealed: true, Seconds: elapsed,
	}, nil
}

// auditGate is the one-line check each stage carries so fail_at can stop the
// pipeline anywhere. Permanent, because a stage failed on request should not
// then burn the retry budget pretending the failure might be transient.
func auditGate(p AuditParams, stage string) error {
	if p.FailAt == stage {
		return sdk.Permanent(fmt.Errorf("stage %q failed on purpose (fail_at)", stage))
	}
	return nil
}

// ----------------------------------------------------------- fleet-audit ---
//
// fleet-audit is the orchestrator: it audits every site, one canary first and
// then the rest in parallel waves. It is the counterpart to provision-fleet,
// which waits for its children one at a time -- here the whole wave is in
// flight at once, and the parent is asleep for all of it.

// FleetAuditParams drives the fan-out.
type FleetAuditParams struct {
	Sites       []string `json:"sites,omitempty"`        // default: site-a..d, vm1
	Canary      string   `json:"canary,omitempty"`       // audited alone first; empty means Sites[0]
	WaveSize    int      `json:"wave_size,omitempty"`    // sites per parallel wave, default 2
	Prefix      string   `json:"prefix,omitempty"`       // deployment name = prefix + site, default "site-audit-"
	BakeSeconds int      `json:"bake_seconds,omitempty"` // passed to each child
	FailSite    string   `json:"fail_site,omitempty"`    // this site gets fail_at, to exercise a partial fleet failure
	FailAt      string   `json:"fail_at,omitempty"`
}

// fleetSites is the fleet as docker-compose.sites.yml builds it.
var fleetSites = []string{"site-a", "site-b", "site-c", "site-d", "vm1"}

// SiteOutcome is one site's line in the fleet report. It is deliberately flat:
// a table artifact of these is the thing an operator actually reads.
type SiteOutcome struct {
	Site       string  `json:"site"`
	Wave       int     `json:"wave"`
	RunID      string  `json:"run_id"`
	Status     string  `json:"status"`
	Worker     string  `json:"worker,omitempty"`
	Probes     int     `json:"probes"`
	Unhealthy  int     `json:"unhealthy"`
	SnapshotID string  `json:"snapshot_id,omitempty"`
	Seconds    float64 `json:"seconds"`
	Message    string  `json:"message,omitempty"`
}

// FleetAuditResult is the run result.
type FleetAuditResult struct {
	Sites     int           `json:"sites"`
	Waves     int           `json:"waves"`
	Failed    int           `json:"failed"`
	Unhealthy int           `json:"unhealthy"`
	Canary    string        `json:"canary"`
	Outcomes  []SiteOutcome `json:"outcomes"`
}

func fleetAudit(c *sdk.Context) (any, error) {
	p, err := sdk.Params[FleetAuditParams](c)
	if err != nil {
		return nil, err
	}
	sites := p.Sites
	if len(sites) == 0 {
		sites = fleetSites
	}
	if len(sites) > 32 {
		return nil, sdk.Permanent(fmt.Errorf("sites must be 32 or fewer, got %d", len(sites)))
	}
	prefix := p.Prefix
	if prefix == "" {
		prefix = "site-audit-"
	}
	waveSize := p.WaveSize
	if waveSize <= 0 {
		waveSize = 2
	}
	canary := p.Canary
	if canary == "" {
		canary = sites[0]
	}
	if !slices.Contains(sites, canary) {
		return nil, sdk.Permanent(fmt.Errorf("canary %q is not in sites %v", canary, sites))
	}

	// The plan is a checkpoint so the wave boundaries cannot move under a
	// replay: every dispatch key below is derived from it.
	rest := make([]string, 0, len(sites))
	for _, s := range sites {
		if s != canary {
			rest = append(rest, s)
		}
	}
	waves, err := sdk.Task(c, "plan", func(*sdk.Context) ([][]string, error) {
		var out [][]string
		for i := 0; i < len(rest); i += waveSize {
			out = append(out, rest[i:min(i+waveSize, len(rest))])
		}
		return out, nil
	}, sdk.TaskKey("plan"))
	if err != nil {
		return nil, err
	}
	c.Info("fleet audit planned", "sites", len(sites), "canary", canary, "waves", len(waves), "wave_size", waveSize)

	outcomes := make([]SiteOutcome, 0, len(sites))

	// The canary goes alone and gates everything else: if one site cannot be
	// audited, dispatching the other four only multiplies the mess. This is the
	// sequential shape -- RunDeploymentAndWait suspends until the child lands.
	child, err := c.RunDeploymentAndWait(prefix+canary, auditParamsFor(p, canary),
		sdk.TriggerTags("fleet-audit", "canary"))
	if err != nil {
		return nil, err
	}
	outcomes = append(outcomes, outcomeOf(canary, 0, child.RunID, child.Status, "", child.Result))
	c.Info("canary passed", "site", canary, "run_id", child.RunID)

	// Then the waves. Each is dispatched all at once and waited on as a group:
	// the parent releases its slot and the engine wakes it when the last child
	// of the wave settles.
	for w, wave := range waves {
		ids := make([]string, 0, len(wave))
		for _, site := range wave {
			id, dErr := sdk.Task(c, "dispatch", func(c *sdk.Context) (string, error) {
				return c.RunDeployment(prefix+site, auditParamsFor(p, site),
					sdk.TriggerTags("fleet-audit", fmt.Sprintf("wave-%d", w+1)),
					// The checkpoint already makes this exactly-once on a
					// replay; the idempotency key closes the one gap it cannot
					// -- a crash between the trigger landing and the checkpoint
					// being written.
					sdk.TriggerIdempotencyKey(fmt.Sprintf("fleet:%s:%s", c.Run().RunID, site)),
				)
			}, sdk.TaskKey("dispatch:"+site))
			if dErr != nil {
				return nil, dErr
			}
			ids = append(ids, id)
		}

		states, err := awaitRuns(c, ids)
		if err != nil {
			return nil, err
		}
		for i, site := range wave {
			outcomes = append(outcomes, outcomeOf(site, w+1, ids[i], states[i].Status, states[i].Message, states[i].Result))
		}
		c.Info("wave complete", "wave", w+1, "sites", len(wave))
	}

	// Aggregate, then decide. Failing early would leave the operator with a
	// report on half the fleet, which is the half they already knew about.
	//
	// The artifacts are written before the verdict on purpose: a run that ends
	// FAILED has its result discarded, so on the one run whose report matters
	// most the table below is the only copy that survives.
	failed, unhealthy := 0, 0
	for _, o := range outcomes {
		if o.Status != "COMPLETED" {
			failed++
		}
		unhealthy += o.Unhealthy
	}

	_ = c.Table("fleet", outcomes)
	_ = c.Markdown("fleet-audit", fmt.Sprintf(
		"### Fleet audit\n\n- **Sites:** %d (canary `%s`, then %d wave(s) of %d)\n"+
			"- **Failed:** %d\n- **Degraded subsystems:** %d\n\n%s\n",
		len(sites), canary, len(waves), waveSize, failed, unhealthy, outcomeTable(outcomes)))

	result := FleetAuditResult{
		Sites: len(sites), Waves: len(waves) + 1, Failed: failed,
		Unhealthy: unhealthy, Canary: canary, Outcomes: outcomes,
	}
	if failed > 0 {
		// Permanent: re-running the parent would re-dispatch nothing (every
		// child is checkpointed) and so could only produce the same verdict.
		return result, sdk.Permanent(fmt.Errorf("%d of %d sites failed their audit", failed, len(outcomes)))
	}
	return result, nil
}

// awaitRuns waits -- durably -- for every run in ids to settle. It returns a
// suspension while any are still in flight, which hands the worker slot back;
// the engine reschedules the parent as soon as its last child finishes, and the
// 30-second re-poll is only the backstop for a wake-up that went missing.
func awaitRuns(c *sdk.Context, ids []string) ([]sdk.RunState, error) {
	states := make([]sdk.RunState, len(ids))
	pending := 0
	for i, id := range ids {
		st, err := c.Runtime().GetRunState(c, id)
		if err != nil {
			return nil, err
		}
		states[i] = st
		if !st.Terminal() {
			pending++
		}
	}
	if pending > 0 {
		return nil, sdk.Suspend(time.Now().Add(30*time.Second),
			fmt.Sprintf("waiting for %d of %d sites", pending, len(ids)))
	}
	return states, nil
}

// auditParamsFor builds one child's parameters, applying fail_at to the single
// site the caller nominated so a partial fleet failure can be produced on
// demand rather than waited for.
func auditParamsFor(p FleetAuditParams, site string) AuditParams {
	a := AuditParams{Site: site, BakeSeconds: p.BakeSeconds}
	if p.FailSite == site {
		a.FailAt = p.FailAt
	}
	return a
}

// outcomeOf flattens a child run into its report row. A child that failed has
// no result to decode, so only the status and message survive -- which is
// exactly what the row should show.
func outcomeOf(site string, wave int, runID, status, message string, result json.RawMessage) SiteOutcome {
	o := SiteOutcome{Site: site, Wave: wave, RunID: runID, Status: status, Message: message}
	var a AuditResult
	if len(result) > 0 && string(result) != "null" && json.Unmarshal(result, &a) == nil {
		o.Worker, o.Probes, o.Unhealthy = a.Worker, len(a.Probes), a.Unhealthy
		o.SnapshotID, o.Seconds = a.SnapshotID, a.Seconds
	}
	return o
}

// outcomeTable renders the outcomes as markdown, because the table artifact and
// the summary are read in different places and the summary should stand alone.
func outcomeTable(outcomes []SiteOutcome) string {
	var b strings.Builder
	b.WriteString("| Site | Wave | Status | Worker | Probes | Degraded | Snapshot |\n")
	b.WriteString("|---|---|---|---|---|---|---|\n")
	for _, o := range outcomes {
		wave := "canary"
		if o.Wave > 0 {
			wave = fmt.Sprintf("%d", o.Wave)
		}
		snap := o.SnapshotID
		if snap == "" {
			snap = "—"
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %d | %d | `%s` |\n",
			o.Site, wave, o.Status, o.Worker, o.Probes, o.Unhealthy, snap))
	}
	return b.String()
}

// ------------------------------------------------------------------ main ---

func main() {
	sdk.Flow("provision-vm", provisionVM,
		sdk.Description("Provision a VM in VMware Cloud Director and register it for metering"),
		sdk.Tags("primex", "vcd", "provisioning"),
		sdk.ParamsSchema(ProvisionParams{OrgName: "acme", Template: "ubuntu-22.04", CPU: 2, MemoryMB: 4096}),
		sdk.Retries(1),
		sdk.RetryDelay(30*time.Second),
		sdk.Timeout(30*time.Minute),
	)

	sdk.Flow("collect-metering", collectMetering,
		sdk.Description("Collect per-org usage from Cloud Director and hand it to billing"),
		sdk.Tags("primex", "metering"),
		sdk.ParamsSchema(MeteringParams{}),
		sdk.Retries(2),
		sdk.RetryDelay(2*time.Minute),
	)

	sdk.Flow("provision-fleet", provisionFleet,
		sdk.Description("Provision N VMs as child runs and wait for them all (sub-flow demo)"),
		sdk.Tags("primex", "vcd", "fleet"),
		sdk.ParamsSchema(FleetParams{OrgName: "acme", Template: "ubuntu-22.04", Count: 3}),
		sdk.Timeout(time.Hour),
	)

	sdk.Flow("add-numbers", addNumbers,
		sdk.Description("Add two numbers -- the smallest possible flow"),
		sdk.Tags("demo"),
		sdk.ParamsSchema(AddParams{A: 2, B: 3}),
		sdk.Retries(1),
		sdk.Timeout(time.Minute),
	)

	sdk.Flow("hello-world", helloWorld,
		sdk.Description("Greet a name N times -- the flow to deploy first on a new pool"),
		sdk.Tags("demo", "smoke"),
		sdk.ParamsSchema(HelloParams{Name: "world", Times: 3}),
		sdk.Retries(1),
		sdk.RetryDelay(5*time.Second),
		sdk.Timeout(time.Minute),
	)

	sdk.Flow("onboard-tenant", onboardTenant,
		sdk.Description("Six checkpointed stages -- the worked example of per-task tracking"),
		sdk.Tags("demo", "pipeline"),
		sdk.ParamsSchema(OnboardParams{OrgName: "acme", Tier: "standard"}),
		sdk.Retries(1),
		sdk.RetryDelay(10*time.Second),
		sdk.Timeout(10*time.Minute),
	)

	sdk.Flow("site-loop", siteLoop,
		sdk.Description("Hold a worker slot for ~20s in checkpointed ticks -- the per-site smoke test"),
		sdk.Tags("demo", "site", "smoke"),
		sdk.ParamsSchema(LoopParams{Seconds: 20, StepSeconds: 2}),
		sdk.Retries(1),
		sdk.RetryDelay(5*time.Second),
		sdk.Timeout(10*time.Minute),
	)

	sdk.Flow("site-audit", siteAudit,
		sdk.Description("Probe, snapshot and seal one site -- fan-out keys, a durable wait and a saga rollback"),
		sdk.Tags("demo", "site", "audit"),
		sdk.ParamsSchema(AuditParams{Site: "site-a", BakeSeconds: 8}),
		// No retries on purpose. A run that rolled back has COMPLETED
		// checkpoints for resources that no longer exist, so replaying it would
		// skip the rebuild and seal a snapshot it never took. Rolling forward
		// from a rollback is a new run, not a retry.
		sdk.Retries(0),
		sdk.Timeout(15*time.Minute),
	)

	sdk.Flow("fleet-audit", fleetAudit,
		sdk.Description("Audit every site: one canary, then parallel waves of child runs"),
		sdk.Tags("demo", "fleet", "audit"),
		sdk.ParamsSchema(FleetAuditParams{Sites: fleetSites, WaveSize: 2, BakeSeconds: 8}),
		sdk.Retries(0),
		sdk.Timeout(time.Hour),
	)

	// Two ways to reach the orchestrator: a database connection, or — for a
	// worker at a site that has no route to the database — the worker API.
	hasDB := os.Getenv("PRIMEFLOW_DATABASE_URL") != "" || os.Getenv("DATABASE_URL") != ""
	hasAPI := os.Getenv("PRIMEFLOW_API_URL") != "" && os.Getenv("PRIMEFLOW_WORKER_TOKEN") != ""
	if !hasDB && !hasAPI {
		log.Fatal("set PRIMEFLOW_DATABASE_URL (e.g. postgres://primeflow:primeflow@localhost:5432/primeflow?sslmode=disable), " +
			"or PRIMEFLOW_API_URL and PRIMEFLOW_WORKER_TOKEN to run against the API")
	}

	// -push (or PRIMEFLOW_PUSH=1) runs this binary as a push-pool receiver: it
	// listens for signed dispatches on PRIMEFLOW_PUSH_ADDR instead of polling.
	push := flag.Bool("push", os.Getenv("PRIMEFLOW_PUSH") == "1", "run as a push-pool receiver")
	flag.Parse()

	// pkg/primeflow/worker rather than pkg/primeflow: a worker binary has no
	// use for the API server, the console or the scheduler, and importing the
	// parent package would link all three.
	run := worker.Run
	if *push {
		run = worker.RunPush
	}
	if err := run(context.Background(), worker.Options{}); err != nil {
		log.Fatal(err)
	}
}
