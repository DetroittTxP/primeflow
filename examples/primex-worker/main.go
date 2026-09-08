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
	day := p.Day
	if day == "" {
		day = time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
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

	if os.Getenv("PRIMEFLOW_DATABASE_URL") == "" && os.Getenv("DATABASE_URL") == "" {
		log.Fatal("set PRIMEFLOW_DATABASE_URL, e.g. postgres://primeflow:primeflow@localhost:5432/primeflow?sslmode=disable")
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
