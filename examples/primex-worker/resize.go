package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/DetroittTxP/primeflow/pkg/sdk"
)

// ------------------------------------------------------------- resize-vm ---
//
// resize-vm is the flow to open the console's "Run…" dialog on: its parameter
// struct carries one field of every type the form renderer knows, so the dialog
// shows a text box, a number box, a boolean select and a JSON field together --
// pre-filled from the deployment's own stored parameters.
//
// It is also the shape a real reconfigure takes: validate, power down if the
// change needs it, apply, attach what was asked for, power back up and wait for
// the guest to settle. So the checkpoint boundaries sit where a real one's
// would, and a worker that dies mid-resize resumes without re-attaching a disk
// it already attached.

// ResizeParams is what an operator supplies. Every field maps to one schema
// type -- string, integer, number, boolean, array, object -- which is what
// makes this the flow worth pointing the run dialog at. The three fields
// without omitempty are the ones the form marks required.
type ResizeParams struct {
	OrgName  string            `json:"org_name"`
	VMName   string            `json:"vm_name"`
	CPU      int               `json:"cpu"`
	MemoryGB float64           `json:"memory_gb,omitempty"`
	Restart  bool              `json:"restart,omitempty"`
	AddDisks []string          `json:"add_disks,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// ResizeResult is the run result. Recording the worker and pool is what makes
// it answer "where did this actually run", which is the question a deployment
// pinned to a pool raises.
type ResizeResult struct {
	VMID       string    `json:"vm_id"`
	CPU        int       `json:"cpu"`
	MemoryGB   float64   `json:"memory_gb"`
	DisksAdded []string  `json:"disks_added"`
	Restarted  bool      `json:"restarted"`
	Worker     string    `json:"worker"`
	Queue      string    `json:"queue"`
	At         time.Time `json:"at"`
}

func resizeVM(c *sdk.Context, p ResizeParams) (any, error) {
	org := strings.TrimSpace(p.OrgName)
	name := strings.TrimSpace(p.VMName)
	if org == "" || name == "" {
		return nil, sdk.Permanent(errors.New("org_name and vm_name are required"))
	}
	// Sizing the caller got wrong is not a transient fault: Permanent skips the
	// retry budget rather than spending it re-submitting the same rejection.
	if p.CPU < 1 || p.CPU > 64 {
		return nil, sdk.Permanent(fmt.Errorf("cpu must be between 1 and 64, got %d", p.CPU))
	}
	memory := p.MemoryGB
	if memory == 0 {
		memory = 4
	}
	if memory < 0.5 || memory > 512 {
		return nil, sdk.Permanent(fmt.Errorf("memory_gb must be between 0.5 and 512, got %g", memory))
	}
	c.Info("resize requested", "org", org, "vm", name,
		"cpu", p.CPU, "memory_gb", memory, "restart", p.Restart, "disks", len(p.AddDisks))

	// Cached for an hour under the VM's own key: every resize against the same
	// VM in that window would otherwise repeat the lookup, and a replay of this
	// run certainly should not.
	vm, err := sdk.Task(c, "lookup-vm", func(c *sdk.Context) (VM, error) {
		return vcdFindVM(c, org, name)
	}, sdk.TaskRetries(3), sdk.TaskRetryDelay(5*time.Second),
		sdk.TaskCache("vcd:vm:"+org+":"+name, time.Hour))
	if err != nil {
		return nil, err
	}

	// A sizing change needs the VM down first. The whole task is inside the
	// branch, so a given set of parameters always produces the same checkpoint
	// set -- which is what makes the positional keys below stable on replay.
	if p.Restart {
		if err := sdk.Do(c, "power-off", func(c *sdk.Context) error {
			return vcdSetPower(c, vm.ID, false)
		}, sdk.TaskRetries(2), sdk.TaskTimeout(2*time.Minute)); err != nil {
			return nil, err
		}
	}

	if err := sdk.Do(c, "apply-sizing", func(c *sdk.Context) error {
		return vcdResize(c, vm.ID, p.CPU, memory)
	}, sdk.TaskRetries(2), sdk.TaskRetryDelay(10*time.Second),
		sdk.TaskTimeout(5*time.Minute)); err != nil {
		return nil, err
	}

	// One checkpoint per disk, keyed by the disk's own name rather than left to
	// the default positional key: adding a disk to the list on a later attempt
	// must not shift the keys of the ones already attached and attach them
	// twice. This is the loop the TaskKey rule exists for.
	added := make([]string, 0, len(p.AddDisks))
	for _, disk := range p.AddDisks {
		disk := strings.TrimSpace(disk)
		if disk == "" {
			continue
		}
		id, err := sdk.Task(c, "attach-disk", func(c *sdk.Context) (string, error) {
			return vcdAttachDisk(c, vm.ID, disk)
		}, sdk.TaskKey("attach-disk:"+vm.ID+":"+disk), sdk.TaskRetries(2))
		if err != nil {
			return nil, err
		}
		added = append(added, id)
	}

	// The object parameter. Sorted because map iteration is not ordered and the
	// log line should read the same on a replay as it did on the first attempt.
	if len(p.Labels) > 0 {
		keys := make([]string, 0, len(p.Labels))
		for k := range p.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if err := sdk.Do(c, "apply-labels", func(c *sdk.Context) error {
			return vcdSetLabels(c, vm.ID, keys, p.Labels)
		}, sdk.TaskRetries(1)); err != nil {
			return nil, err
		}
	}

	if p.Restart {
		if err := sdk.Do(c, "power-on", func(c *sdk.Context) error {
			return vcdSetPower(c, vm.ID, true)
		}, sdk.TaskRetries(3), sdk.TaskRetryDelay(10*time.Second)); err != nil {
			return nil, err
		}
		// Over the 30s suspend threshold, so the run hands its worker slot back
		// while the guest boots instead of holding a slot to watch a clock.
		if err := sdk.Sleep(c, "settle", 45*time.Second); err != nil {
			return nil, err
		}
	}

	_ = c.Table("changes", []map[string]any{
		{"change": "cpu", "value": p.CPU},
		{"change": "memory_gb", "value": memory},
		{"change": "disks_added", "value": len(added)},
		{"change": "restarted", "value": p.Restart},
	})
	_ = c.Markdown("summary", fmt.Sprintf(
		"### Resized `%s`\n\n- **Org:** %s\n- **VM:** `%s`\n- **CPU:** %d\n"+
			"- **Memory:** %g GB\n- **Disks added:** %d\n- **Restarted:** %t\n"+
			"- **Worker:** %s (pool `%s`)\n",
		vm.Name, org, vm.ID, p.CPU, memory, len(added), p.Restart,
		workerName(), c.Run().WorkQueue))
	_ = c.Link("console", vm.Href, "Open in Cloud Director")

	return ResizeResult{
		VMID: vm.ID, CPU: p.CPU, MemoryGB: memory,
		DisksAdded: added, Restarted: p.Restart,
		Worker: workerName(), Queue: c.Run().WorkQueue, At: time.Now().UTC(),
	}, nil
}

// ---- stubs -----------------------------------------------------------------
//
// As elsewhere in this example, no vCD is contacted. The random failures are
// intentional: they exercise the retry and checkpoint machinery, so a resize
// that fails at attach-disk and resumes can be watched before a real client is
// wired in.

func vcdFindVM(c *sdk.Context, org, name string) (VM, error) {
	if rand.Float64() < 0.15 {
		return VM{}, errors.New("vcd: 503 from /api/vApp (transient)")
	}
	id := "urn:vcloud:vm:" + strings.ToLower(name)
	c.Debug("resolved vm", "org", org, "name", name, "id", id)
	return VM{
		ID:        id,
		Name:      name,
		Href:      "https://vcd.example.com/tenant/" + org + "/vm/" + id,
		CreatedAt: time.Now().UTC(),
	}, nil
}

func vcdSetPower(c *sdk.Context, vmID string, on bool) error {
	if err := work(c, 800*time.Millisecond); err != nil {
		return err
	}
	if rand.Float64() < 0.1 {
		return errors.New("vcd: power task timed out")
	}
	c.Info("power state set", "vm_id", vmID, "on", on)
	return nil
}

func vcdResize(c *sdk.Context, vmID string, cpu int, memoryGB float64) error {
	if err := work(c, time.Second); err != nil {
		return err
	}
	if rand.Float64() < 0.15 {
		return errors.New("vcd: task failed — no room on the host for the new sizing")
	}
	c.Info("sizing applied", "vm_id", vmID, "cpu", cpu, "memory_gb", memoryGB)
	return nil
}

func vcdAttachDisk(c *sdk.Context, vmID, disk string) (string, error) {
	if err := work(c, 600*time.Millisecond); err != nil {
		return "", err
	}
	if rand.Float64() < 0.12 {
		return "", errors.New("vcd: disk attach rejected — bus full")
	}
	c.Info("disk attached", "vm_id", vmID, "disk", disk)
	return vmID + "/disk/" + disk, nil
}

func vcdSetLabels(c *sdk.Context, vmID string, keys []string, labels map[string]string) error {
	if err := work(c, 300*time.Millisecond); err != nil {
		return err
	}
	c.Info("labels applied", "vm_id", vmID, "keys", strings.Join(keys, ","), "count", len(labels))
	return nil
}
