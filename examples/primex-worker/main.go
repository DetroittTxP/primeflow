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
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"slices"
	"time"

	"github.com/primex/primeflow/pkg/primeflow"
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

	run := primeflow.RunWorker
	if *push {
		run = primeflow.RunPushWorker
	}
	if err := run(context.Background(), primeflow.Options{}); err != nil {
		log.Fatal(err)
	}
}
