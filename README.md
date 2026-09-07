# PrimeFlow

Durable workflow orchestration in Go, built for PrimeX.

PrimeFlow takes the ideas that make Prefect's open-source core good — flows as
ordinary code, automatic state tracking, retries, work pools, event-driven
automations — and rebuilds them as a single static Go binary backed by
PostgreSQL. Flows are Go functions. There is no DAG to declare, no Python
runtime to ship, and no broker to babysit.

```go
sdk.Flow("provision-vm", func(c *sdk.Context) (any, error) {
    p, err := sdk.Params[ProvisionParams](c)
    if err != nil {
        return nil, err
    }

    vm, err := sdk.Task(c, "create-vm", func(c *sdk.Context) (VM, error) {
        return vcd.CreateVM(c, p.OrgID, p.Template)
    }, sdk.TaskRetries(3), sdk.TaskRetryDelay(10*time.Second))
    if err != nil {
        return nil, err
    }

    // Releases the worker slot; the run resumes here in two minutes.
    if err := sdk.Sleep(c, "settle", 2*time.Minute); err != nil {
        return nil, err
    }

    return vm, sdk.Do(c, "register-metering", func(c *sdk.Context) error {
        return metering.Register(c, vm.ID)
    })
}, sdk.Retries(2), sdk.Timeout(30*time.Minute))
```

---

## What durability actually means here

If a worker is killed halfway through that flow, the run's lease expires, the
janitor marks it crashed, and another worker picks it up. The function runs
again **from the top** — but `create-vm` returns its stored result instead of
building a second VM, and execution continues from where it left off.

That is checkpoint-and-replay, the same model Prefect uses. It costs you one
authoring rule:

> **Side effects go inside a `Task`. Code between tasks may run more than once.**

In exchange you write ordinary Go — loops, conditionals, early returns,
`time.Now()`, goroutines — with none of the determinism restrictions a
replay-based engine like Temporal imposes.

---

## Feature map

| Prefect OSS | PrimeFlow |
|---|---|
| `@flow` / `@task` decorators | `sdk.Flow` / `sdk.Task[T]`, generic and type-safe |
| Durable execution, result persistence | Task checkpoints in Postgres, replayed on resume |
| State tracking | 9 states with an enforced transition table |
| Automatic retries | Per-task and per-flow, with exponential backoff |
| Task result caching | Cross-run cache keys with TTL |
| Deployments & schedules | Cron (5/6-field, timezone-aware) and interval, with catch-up control |
| Work pools / work queues | Named queues with concurrency limits and pause switches |
| Workers | Leased, heartbeating, gracefully draining |
| Artifacts | Markdown, table, link and JSON, attached to runs |
| Observability | Structured logs, run timeline, event feed, live SSE stream |
| Events & automations | Event log plus threshold rules with six action types |
| Self-hosted UI | Bundled single-file console, no build step |
| — | **Operator queue control: priority bands, pin-to-front, live reordering** |

The last row is the part Prefect does not have, and it is the reason this exists
rather than a Prefect deployment.

---

## Queue control

Every run carries a **priority** (0–100, default 50) and an optional
**pin**. Dispatch order is exactly:

1. pinned runs first, in pin order
2. then descending priority
3. then oldest scheduled first

Operators change all of it at runtime, from the console or the API, and the
change takes effect on the next lease — never disturbing work already running:

```bash
primeflow queue                                  # depth per lane
primeflow queue show vcd                         # the exact dispatch order
primeflow queue front  <run-id>                  # run this one next
primeflow queue priority <run-id> 100            # promote to urgent
primeflow queue pause  vcd                       # stop the lane, keep the work
```

`GET /api/v1/queues/{name}/pending` returns the same ordering the leasing query
uses, so what an operator sees on screen is what will actually happen.

Queues also carry a **concurrency limit** — the way you stop forty parallel
provisioning flows from overwhelming a vCD endpoint, without changing a line of
flow code.

---

## Architecture

```
   PrimeX backend ─┐
   Web console ────┼──► PrimeFlow server ──► PostgreSQL   (state + queue, source of truth)
   CLI / webhooks ─┘      │  scheduler                │
                          │  automations              │
                          │  REST + SSE               ▼
                          └──────────────────────►  Redis   (wake-ups + live fan-out)
                                                       ▲
                              Workers  ─────────────────┘
                              (your binary + pkg/sdk)
```

**Postgres is the only source of truth**, including queue order. Dispatch is a
single `SELECT … FOR UPDATE SKIP LOCKED` statement, so any number of workers
pull from the same lane without a broker and without double execution.

**Redis is an accelerator, never a dependency.** It carries "new work on queue
X", cancellation signals, and the UI's live stream. Lose it and everything still
works, just with polling latency instead of instant wake-ups. If no Redis URL is
configured, an in-process bus is used and the system runs on one binary plus
Postgres.

**The server scales horizontally.** The scheduler and automation evaluator are
leader-elected through a lease row, so N replicas produce one set of scheduled
runs, not N.

Full design notes: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Quick start

```bash
docker compose up --build     # Postgres, Redis, server, 2 workers
open http://localhost:8080
```

Or run it directly:

```bash
export PRIMEFLOW_DATABASE_URL="postgres://primeflow:primeflow@localhost:5432/primeflow?sslmode=disable"
export PRIMEFLOW_REDIS_URL="redis://localhost:6379"

go run ./cmd/primeflow server           # API + UI + scheduler + automations
go run ./examples/primex-worker         # a worker with two example flows
```

Then create a deployment and run it:

```bash
curl -X POST localhost:8080/api/v1/deployments -d '{
  "name": "provision-vm-standard",
  "flow_name": "provision-vm",
  "work_queue": "vcd",
  "priority": 50,
  "retries": 1,
  "retry_delay": "30s",
  "timeout": "30m"
}'

primeflow run provision-vm-standard -param org_name=acme -param name=web-01
```

---

## Writing a worker

A worker is your own binary. Register flows, then hand over:

```go
package main

import (
    "context"
    "log"

    "github.com/primex/primeflow/pkg/primeflow"
    "github.com/primex/primeflow/pkg/sdk"
)

func main() {
    sdk.Flow("provision-vm", provisionVM, sdk.Retries(2))
    sdk.Flow("collect-metering", collectMetering)

    log.Fatal(primeflow.RunWorker(context.Background(), primeflow.Options{}))
}
```

Configuration comes from the environment, so the same image runs everywhere:

| Variable | Meaning | Default |
|---|---|---|
| `PRIMEFLOW_DATABASE_URL` | Postgres DSN | required |
| `PRIMEFLOW_REDIS_URL` | Redis URL for live updates | optional |
| `PRIMEFLOW_QUEUES` | comma-separated lanes to poll | `default` |
| `PRIMEFLOW_CONCURRENCY` | runs executed in parallel | `4` |
| `PRIMEFLOW_LEASE` | lease duration | `60s` |
| `PRIMEFLOW_POLL` | fallback poll interval | `2s` |
| `PRIMEFLOW_HTTP_ADDR` | server listen address | `:8080` |
| `PRIMEFLOW_API_TOKEN` | bearer token required on `/api` | none |
| `PRIMEFLOW_CORS_ORIGIN` | allow the PrimeX console to call the API | none |

### SDK reference

**Flows**

```go
sdk.Flow(name, fn, sdk.Retries(2), sdk.RetryDelay(30*time.Second),
    sdk.Timeout(time.Hour), sdk.Description("…"), sdk.Tags("primex"))

sdk.Flow("resize", sdk.Typed(func(c *sdk.Context, p ResizeParams) (any, error) { … }))
```

**Tasks**

```go
v, err := sdk.Task(c, "name", fn,
    sdk.TaskKey("stable-key"),          // pin the checkpoint key (see below)
    sdk.TaskRetries(3),
    sdk.TaskRetryDelay(5*time.Second),  // exponential, capped
    sdk.TaskTimeout(2*time.Minute),
    sdk.TaskCache("vcd:org:acme", time.Hour), // share the result across runs
    sdk.TaskEphemeral(),                // never persist; always execute
)

err := sdk.Do(c, "name", fn)            // for steps with no return value
```

Checkpoint keys default to `<task name>-<n>` in reach order. **Inside a loop
over data that can change between attempts, supply an explicit key derived from
the data** — otherwise adding an item shifts every subsequent key and re-runs
work that already succeeded:

```go
for _, org := range orgs {
    row, err := sdk.Task(c, "collect", collectFn, sdk.TaskKey("collect:"+org+":"+day))
}
```

**Waiting**

```go
sdk.Sleep(c, "settle", 2*time.Minute)   // >30s releases the worker slot
sdk.WaitUntil(c, "window", startOfDay)
sdk.Suspend(until, "waiting for approval")
```

**Errors**

```go
return sdk.Permanent(err)               // skip the retry budget entirely
```

**Logs, artifacts, fan-out**

```go
c.Info("provisioning", "org", p.OrgName)
c.Markdown("summary", "### VM created…")
c.Table("usage", rows)
c.Link("console", vm.Href, "Open in Cloud Director")
c.RunDeployment("notify-oncall", payload, sdk.TriggerPriority(100))
```

---

## Scheduling

```json
{
  "name": "nightly-metering",
  "flow_name": "collect-metering",
  "work_queue": "metering",
  "schedule_kind": "cron",
  "schedule": "0 2 * * *",
  "timezone": "Asia/Bangkok",
  "catchup": false
}
```

- `cron` accepts 5-field and 6-field (leading seconds) expressions and `@hourly`-style descriptors.
- `interval` accepts a Go duration such as `15m`, phase-anchored to the deployment's creation time so restarts do not cause drift.
- `catchup: false` (the default) materialises one run after downtime rather than every missed window.
- Schedules are validated when the deployment is saved, not silently every cycle afterwards.

Runs are materialised an hour ahead, which is what lets you see, reprioritise
or cancel tomorrow's work today.

---

## Events and automations

Every state change writes an event (`flow-run.FAILED`, `flow-run.COMPLETED`, …).
Automations match events — with optional thresholds and time windows — and act:

```json
{
  "name": "escalate-repeated-provisioning-failures",
  "match_event": "flow-run.FAILED",
  "match_flow": "provision-vm",
  "threshold": 3,
  "window": "10m",
  "action": "run-deployment",
  "action_config": {"deployment": "notify-oncall", "pass_event": true, "priority": 100}
}
```

Actions: `run-deployment`, `cancel-run`, `set-priority`, `pause-queue`,
`resume-queue`, `webhook`.

External systems trigger flows the other way through
`POST /api/v1/webhooks/{deployment}` — the request body becomes the run's
parameters, which is all it takes to wire vCD or a billing system straight into
a flow.

---

## API

All routes are under `/api/v1`. `GET /api/v1/health` is always unauthenticated
so probes need no token.

| | |
|---|---|
| `GET /summary` | dashboard counters in one round trip |
| `GET /flows`, `GET /workers` | catalogue and worker liveness |
| `GET/POST /deployments`, `POST /deployments/{id}/run` | manage and trigger |
| `POST /deployments/{id}/pause` · `/resume` | stop or start scheduling |
| `GET/POST /runs`, `GET /runs/{id}` | list, create, inspect |
| `GET /runs/{id}/tasks` · `/logs` · `/artifacts` | the run timeline |
| `POST /runs/{id}/cancel` · `/retry` · `/reschedule` | lifecycle |
| `POST /runs/{id}/priority` · `/front` · `/back` · `/unpin` · `/queue` | **queue control** |
| `GET/POST /queues`, `GET /queues/{name}/pending` | lanes and dispatch order |
| `POST /queues/{name}/pause` · `/resume` | throttle a lane |
| `GET /events`, `GET/POST /automations` | event feed and rules |
| `POST /webhooks/{deployment}` | external trigger |
| `GET /stream` | Server-Sent Events, live |

---

## Deployment

**Kubernetes** — [`deploy/k8s/primeflow.yaml`](deploy/k8s/primeflow.yaml) has a
server Deployment (safe to scale: leader election handles the singleton loops)
and one worker Deployment per queue, so a slow lane scales independently.

Give workers a `terminationGracePeriodSeconds` long enough to reach the next
checkpoint. Past it nothing is lost either — the lease expires and another
worker resumes the run.

**Sizing.** The dispatch query is a single indexed statement per queue per poll.
One Postgres instance comfortably handles tens of thousands of runs a day; the
`pf_logs` table is the one that grows, so add a retention job when you turn this
on for real.

---

## Testing

```bash
make test-unit          # no database needed
make test-integration   # everything, against a real Postgres
```

The integration packages each reset the same database, so they must not run
concurrently — `make test-integration` passes `-p 1` for that reason.

The suite covers the guarantees that matter: durable resume skipping completed
tasks, retry budgets, permanent errors bypassing retries, durable sleep
releasing the worker, cancellation of a running flow, priority and pin
ordering, concurrency limits, no double-leasing under concurrent workers, lease
expiry and recovery, transition-rule enforcement, and idempotent schedule
materialisation.

---

## Repository layout

```
cmd/primeflow/          server, migrator and admin CLI
pkg/sdk/                the authoring surface — flows, tasks, waits, artifacts
pkg/primeflow/          wiring, so a worker's main() is five lines
internal/core/          domain model and the state transition table
internal/store/         persistence interface + PostgreSQL implementation
internal/engine/        durable execution: checkpoints, retries, cancellation
internal/worker/        leasing, heartbeats, graceful drain
internal/server/        REST API, SSE stream, embedded console
internal/scheduler/     schedule materialisation and the lease janitor
internal/automations/   event-driven rules
internal/bus/           Redis pub/sub, with an in-process fallback
examples/primex-worker/ VM provisioning and metering flows
deploy/k8s/             manifests
```

---

## Known gaps

Honest list of what is not built yet:

- **NATS transport.** The `bus.Bus` interface is there and Redis implements it;
  a NATS implementation is a single file, not yet written.
- **Prometheus metrics.** Events and the API cover observability today; the
  HPA example in the manifests assumes a `primeflow_queue_ready` metric that
  needs an exporter.
- **Log retention.** `pf_logs` grows without bound. Add a partition or a
  cleanup job before production.
- **Sub-flows.** `RunDeployment` fans out but does not wait for children.
- **RBAC.** A single bearer token, not per-user roles.
