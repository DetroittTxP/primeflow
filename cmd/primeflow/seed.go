// Development fixtures. "primeflow seed" writes the work queues, deployments
// and operator accounts that the example worker's flows expect, and can queue a
// few runs so a fresh console has something in it.
//
// Like migrate and user, it talks straight to the database rather than through
// the API: seeding a database whose server is not up yet is the normal case,
// and it saves the target having to hold an API token.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/authn"
	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
	"github.com/primex/primeflow/pkg/primeflow"
)

// The lanes the compose worker polls (PRIMEFLOW_QUEUES=default,vcd,metering).
// vcd is deliberately narrow: it is the lane that stands in for a real vCenter,
// and a throttled lane is what makes the queue page worth looking at.
var seedQueues = []core.WorkQueue{
	{
		Name: "default", Description: "Demo and smoke work",
		ConcurrencyLimit: intPtr(8), MinWorkers: 1, MaxWorkers: intPtr(4),
		TargetReadyPerWorker: 5, Owner: "platform",
	},
	{
		Name: "vcd", Description: "vCloud Director provisioning — throttled to protect the endpoint",
		ConcurrencyLimit: intPtr(2), MinWorkers: 1, MaxWorkers: intPtr(2),
		TargetReadyPerWorker: 3, Owner: "platform",
	},
	{
		Name: "metering", Description: "Nightly metering collection",
		ConcurrencyLimit: intPtr(4), MinWorkers: 0, MaxWorkers: intPtr(2),
		TargetReadyPerWorker: 5, Owner: "billing",
	},
}

// Deployment names are part of the fixture, not decoration: provision-fleet
// triggers "provision-vm-standard" by name, and fleet-audit triggers
// "site-audit-<site>". Renaming either breaks those flows.
func seedDeployments() []core.Deployment {
	deps := []core.Deployment{
		{
			Name: "hello-world", FlowName: "hello-world",
			Description: "The smallest demo: greet a name a few times",
			Parameters:  json.RawMessage(`{"name":"primeflow","times":3}`),
			WorkQueue:   "default", Tags: []string{"demo", "smoke"},
			Retries: 1, RetryDelay: 5 * time.Second, Timeout: time.Minute,
		},
		{
			Name: "add-numbers", FlowName: "add-numbers",
			Description: "One task, one result — the flow to check a worker with",
			Parameters:  json.RawMessage(`{"a":2,"b":3}`),
			WorkQueue:   "default", Tags: []string{"demo", "smoke"},
			Retries: 1, Timeout: time.Minute,
		},
		{
			Name: "onboard-tenant-standard", FlowName: "onboard-tenant",
			Description: "Six checkpointed stages — the happy path",
			Parameters:  json.RawMessage(`{"org_name":"acme","tier":"standard"}`),
			WorkQueue:   "default", Tags: []string{"demo", "pipeline"},
			Retries: 1, RetryDelay: 10 * time.Second, Timeout: 10 * time.Minute,
		},
		{
			// A deployment that always fails, on purpose: a console with no
			// failed run in it does not show what the failure views do.
			Name: "onboard-tenant-failing", FlowName: "onboard-tenant",
			Description: "Fails at attach-storage on purpose — for the failure and retry views",
			Parameters:  json.RawMessage(`{"org_name":"initech","tier":"standard","fail_at":"attach-storage"}`),
			WorkQueue:   "default", Tags: []string{"demo", "pipeline", "failure"},
			Retries: 1, RetryDelay: 10 * time.Second, Timeout: 10 * time.Minute,
		},
		{
			Name: "provision-vm-standard", FlowName: "provision-vm",
			Description: "Build one VM — the child provision-fleet fans out to",
			Parameters: json.RawMessage(`{"org_name":"acme","vdc_name":"acme-vdc",` +
				`"template":"ubuntu-22.04","name":"demo-vm","cpu":2,"memory_mb":4096,"owner":"platform@primeflow.local"}`),
			WorkQueue: "vcd", Tags: []string{"demo", "vcd"},
			Retries: 2, RetryDelay: 15 * time.Second, Timeout: 30 * time.Minute,
		},
		{
			// The deployment to open the console's "Run…" dialog on: its stored
			// parameters cover every type the form renders, so the dialog opens
			// pre-filled on all of them rather than on a JSON textarea.
			Name: "resize-vm-web-01", FlowName: "resize-vm",
			Description: "Reconfigure one VM — the worked example for running with parameters",
			Parameters: json.RawMessage(`{"org_name":"acme","vm_name":"web-01","cpu":4,` +
				`"memory_gb":8,"restart":true,"add_disks":["data-01"],"labels":{"env":"prod"}}`),
			WorkQueue: "vcd", Tags: []string{"demo", "vcd", "resize"},
			Retries: 1, RetryDelay: 30 * time.Second, Timeout: 30 * time.Minute,
		},
		{
			Name: "provision-fleet-acme", FlowName: "provision-fleet",
			Description: "Three child VM builds with a durable wait between them",
			Parameters:  json.RawMessage(`{"org_name":"acme","template":"ubuntu-22.04","count":3}`),
			WorkQueue:   "vcd", Tags: []string{"demo", "vcd", "subflow"},
			Timeout: time.Hour,
		},
		{
			Name: "collect-metering-nightly", FlowName: "collect-metering",
			Description: "Nightly usage collection for the demo orgs",
			Parameters:  json.RawMessage(`{"orgs":["acme","initech","globex"]}`),
			WorkQueue:   "metering", Tags: []string{"demo", "metering", "scheduled"},
			ScheduleKind: core.ScheduleCron, Schedule: "15 2 * * *", Timezone: "UTC",
			// Yesterday's numbers do not get more correct by being collected
			// eight times after a weekend of downtime.
			CatchUp: false,
			Retries: 3, RetryDelay: time.Minute, Timeout: 30 * time.Minute,
		},
		{
			// Paused on insert so a seeded database does not start doing work on
			// its own. Unpause it in the console to watch the scheduler tick.
			Name: "site-loop-every-15m", FlowName: "site-loop",
			Description: "Holds a worker slot for ~20s every 15 minutes — paused until you want it",
			Parameters:  json.RawMessage(`{"seconds":20,"step_seconds":2,"label":"seed"}`),
			WorkQueue:   "default", Tags: []string{"demo", "site", "scheduled"},
			ScheduleKind: core.ScheduleInterval, Schedule: "15m", Timezone: "UTC",
			Paused:  true,
			Retries: 1, RetryDelay: 5 * time.Second, Timeout: 10 * time.Minute,
		},
		{
			Name: "fleet-audit", FlowName: "fleet-audit",
			Description: "Audit every site: one canary, then parallel waves of child runs",
			Parameters:  json.RawMessage(`{"wave_size":2,"bake_seconds":8}`),
			WorkQueue:   "default", Tags: []string{"demo", "fleet", "audit"},
			// A run that rolled back must not be replayed; see the flow.
			Retries: 0, Timeout: time.Hour,
		},
	}
	// fleet-audit dispatches one child deployment per site, named by prefix.
	// Keep this list in step with fleetSites in the example worker.
	for _, site := range []string{"site-a", "site-b", "site-c", "site-d", "vm1"} {
		deps = append(deps, core.Deployment{
			Name: "site-audit-" + site, FlowName: "site-audit",
			Description: "Probe, snapshot and seal " + site,
			Parameters:  json.RawMessage(fmt.Sprintf(`{"site":%q,"bake_seconds":8}`, site)),
			WorkQueue:   "default", Tags: []string{"demo", "site", "audit"},
			Retries: 0, Timeout: 15 * time.Minute,
		})
	}
	return deps
}

// The demo accounts, one per role, so every permission path can be clicked
// through. They are created only when missing, and never have their password
// reset by a re-seed.
var seedUsers = []struct{ email, role string }{
	{"admin@primeflow.local", string(authn.RoleAdmin)},
	{"operator@primeflow.local", string(authn.RoleOperator)},
	{"viewer@primeflow.local", string(authn.RoleViewer)},
}

// The deployments -runs cycles through: short, self-contained, and between them
// they leave a completed run, a failed one and a multi-task one in the console.
var seedRunDeployments = []string{"hello-world", "add-numbers", "onboard-tenant-standard", "onboard-tenant-failing"}

func cmdSeed(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	password := fs.String("password", "primeflow-demo", "password for the seeded demo accounts (min 8 chars)")
	runs := fs.Int("runs", 0, "also queue this many demo runs")
	noUsers := fs.Bool("no-users", false, "do not create the demo operator accounts")
	noMigrate := fs.Bool("no-migrate", false, "do not apply the schema first")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*noUsers && len(*password) < 8 {
		return fmt.Errorf("-password must be at least 8 characters")
	}

	app, err := primeflow.Open(ctx, primeflow.Options{})
	if err != nil {
		return err
	}
	defer app.Close()
	st := app.Store

	if !*noMigrate {
		if err := app.Migrate(ctx); err != nil {
			return err
		}
	}

	for i := range seedQueues {
		q := seedQueues[i]
		// Mirror the API's read-modify-write. UpsertWorkQueue writes paused
		// straight from what it is handed -- unlike UpsertDeployment, which
		// leaves it alone -- and pausing a lane is an operator's decision, not
		// something a re-seed gets to undo.
		switch cur, err := st.GetWorkQueue(ctx, q.Name); {
		case err == nil:
			q.Paused = cur.Paused
		case !errors.Is(err, store.ErrNotFound):
			return fmt.Errorf("queue %s: %w", q.Name, err)
		}
		if err := st.UpsertWorkQueue(ctx, &q); err != nil {
			return fmt.Errorf("queue %s: %w", q.Name, err)
		}
	}
	fmt.Printf("queues:      %d\n", len(seedQueues))

	deps := seedDeployments()
	for i := range deps {
		d := deps[i]
		// The upsert keys on name, so the id only matters on first insert; an
		// existing deployment keeps its id and its paused flag.
		d.ID = uuid.NewString()
		if d.Priority == 0 {
			d.Priority = core.PriorityNormal
		}
		if err := st.UpsertDeployment(ctx, &d); err != nil {
			return fmt.Errorf("deployment %s: %w", d.Name, err)
		}
	}
	fmt.Printf("deployments: %d\n", len(deps))

	if !*noUsers {
		created := 0
		for _, u := range seedUsers {
			switch _, err := st.GetUserByEmail(ctx, u.email); {
			case err == nil:
				continue // leave an existing account, and its password, alone
			case !errors.Is(err, store.ErrNotFound):
				return fmt.Errorf("user %s: %w", u.email, err)
			}
			if _, err := st.CreateUser(ctx, store.UserInput{
				ID: uuid.NewString(), Email: u.email,
				PasswordHash: authn.HashPassword(*password), Role: u.role, Active: true,
			}); err != nil {
				return fmt.Errorf("user %s: %w", u.email, err)
			}
			created++
		}
		fmt.Printf("accounts:    %d created, %d already existed\n", created, len(seedUsers)-created)
		if created > 0 {
			// Only the new accounts got this password. One that already existed
			// keeps its own -- admin@primeflow.local is normally the bootstrap
			// account PRIMEFLOW_ADMIN_PASSWORD created on the server's first boot.
			fmt.Printf("             password %q (newly created accounts only)\n", *password)
		}
	}

	for i := 0; i < *runs; i++ {
		name := seedRunDeployments[i%len(seedRunDeployments)]
		d, err := st.GetDeploymentByName(ctx, name)
		if err != nil {
			return fmt.Errorf("run %s: %w", name, err)
		}
		r, err := st.CreateFlowRun(ctx, store.CreateRunInput{
			FlowName: d.FlowName, DeploymentID: &d.ID, Parameters: d.Parameters,
			WorkQueue: d.WorkQueue, Priority: d.Priority, Retries: d.Retries,
			RetryDelay: d.RetryDelay, Timeout: d.Timeout,
			Tags: append([]string{"seed"}, d.Tags...),
		})
		if err != nil {
			return fmt.Errorf("run %s: %w", name, err)
		}
		fmt.Printf("queued %s (%s)\n", r.Name, r.ID)
	}

	fmt.Println("seeded")
	return nil
}

func intPtr(n int) *int { return &n }
