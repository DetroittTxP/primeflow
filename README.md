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
   CLI / webhooks ─┘      │  scheduler + janitor      │
                          │  automations              │
                          │  REST + SSE + /metrics    ▼
                          └──────────────────────►  NATS / Redis   (wake-ups + live fan-out)
                                                       ▲
                              Workers  ─────────────────┘
                              (your binary + pkg/sdk)
```

**Postgres is the only source of truth**, including queue order. Dispatch is a
`SELECT … FOR UPDATE SKIP LOCKED` statement, so any number of workers pull from
the same lane without a broker and without double execution. A lane that carries
a **concurrency limit** additionally serialises its dispatchers behind one
transaction-scoped advisory lock, which is what makes that limit exact rather
than per-worker; an uncapped lane takes no lock at all. Workers still cannot
deadlock each other: the advisory lock is always taken first, only one is ever
held at a time, and the row locks that follow use `SKIP LOCKED` and never wait.
The only structure that could form a cycle is sub-flow recursion, which is
depth-bounded (see below).

**The bus is an accelerator, never a dependency.** It carries "new work on queue
X", cancellation signals, and the UI's live stream. Lose it and everything still
works, just with polling latency instead of instant wake-ups. Choose the
transport with `PRIMEFLOW_NATS_URL` or `PRIMEFLOW_REDIS_URL` (NATS wins if both
are set); with neither, an in-process bus runs the whole system on one binary
plus Postgres.

**The server scales horizontally.** The scheduler, janitor and automation
evaluator are leader-elected through a lease row, so N replicas produce one set
of scheduled runs, not N. Worker pools scale on the workload signal PrimeFlow
publishes at `/metrics` — a KEDA `ScaledObject` or HPA acts on
`primeflow_queue_desired_workers`; PrimeFlow never launches workers itself.

Full design notes: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Quick start

```bash
docker compose up --build          # Postgres, NATS, Redis, server, 2 workers, push receiver
open http://localhost:8080         # log in as admin@primeflow.local / primeflow-admin
```

That is the whole local stack. The server applies its own schema on start, seeds
the admin account from `PRIMEFLOW_ADMIN_EMAIL` / `PRIMEFLOW_ADMIN_PASSWORD` on an
empty database, and the two `primex-worker` replicas register themselves against
the `default`, `vcd` and `metering` lanes.

**If a port is already taken.** Every published port is a variable with the
conventional value as its default, so nothing needs editing to move them — write
a `.env` beside the compose file (it is gitignored, and Compose reads it
automatically):

```bash
# .env — host ports only; container-to-container ports are unchanged
PRIMEFLOW_PG_PORT=5435
PRIMEFLOW_REDIS_PORT=6380
PRIMEFLOW_HTTP_PORT=8085
PRIMEFLOW_NATS_PORT=4223
PRIMEFLOW_NATS_MON_PORT=8223
```

The console is then on `http://localhost:8085`, and `sites/issue-keys.sh`
already defaults to that URL. This matters more than it sounds: 5432, 6379,
8080 and 4222 are the first ports every other local stack claims.

### Talking to it from the CLI

`/api/v1/*` always requires a credential — there is no open-by-default mode. A
browser gets a session cookie from the login form; a script uses the static
machine token, which is admin-equivalent and exempt from CSRF. Set it on the
server first:

```yaml
# docker-compose.yml, under server.environment (uncomment)
PRIMEFLOW_API_TOKEN: local-dev-token
```

```bash
docker compose up -d server        # pick up the new token

export PRIMEFLOW_API_URL=http://localhost:8080     # match PRIMEFLOW_HTTP_PORT
export PRIMEFLOW_API_TOKEN=local-dev-token

curl -sS -X POST $PRIMEFLOW_API_URL/api/v1/deployments \
  -H "Authorization: Bearer $PRIMEFLOW_API_TOKEN" \
  -H 'Content-Type: application/json' -d '{
    "name": "provision-vm-standard",
    "flow_name": "provision-vm",
    "work_queue": "vcd",
    "priority": 50,
    "retries": 1,
    "retry_delay": "30s",
    "timeout": "30m"
  }'

primeflow run provision-vm-standard -param org_name=acme -param name=web-01
primeflow runs
```

Without the token both the `curl` and the CLI get `401 authentication required`.
`GET /api/v1/health` and `GET /metrics` are the only open routes; the console
also accepts `?token=<PRIMEFLOW_API_TOKEN>` on a URL, which is the quick way to
open a view without typing the seed password.

---

## Local development

**Build and test without a Go toolchain on the host.** The Dockerfile's build
stage is an ordinary `golang:1.25-alpine`, so the same image runs `go` directly
against the working tree — nothing to install, and the version matches CI:

```bash
docker run --rm -v "$PWD":/src -w /src -e CGO_ENABLED=0 golang:1.25-alpine \
  go build ./...
```

With Go installed locally the Makefile is the shorter path:

```bash
make build            # bin/primeflow (server + CLI) and bin/primex-worker
make lint             # gofmt -w + go vet
make test-unit        # every test that needs no database
```

**Run the pieces outside the container.** A worker needs a database URL and,
optionally, a bus; it does not have to be a container, so the usual loop is
compose for infrastructure and a native process for whatever you are editing:

```bash
export PRIMEFLOW_DATABASE_URL="postgres://primeflow:primeflow@localhost:5435/primeflow?sslmode=disable"
export PRIMEFLOW_NATS_URL="nats://localhost:4223"

go run ./cmd/primeflow server           # API + UI + scheduler + automations
go run ./examples/primex-worker         # a worker with the example flows
```

Quote the DSN: the `?` in `?sslmode=disable` is a glob in zsh. Ports here are
the `.env` ones above — with no `.env`, use 5432 and 4222.

**Integration tests need their own database.** They reset the schema, so point
them at a database you do not mind losing rather than the one the stack is
using:

```bash
docker compose exec -T postgres psql -U primeflow -c 'CREATE DATABASE primeflow_test'

make test-integration \
  TEST_DB="postgres://primeflow:primeflow@localhost:5435/primeflow_test?sslmode=disable"
```

`make test-integration` passes `-p 1`, because the integration packages each
reset that one database and must not run concurrently. `TestLogPartitionMaintenance`
is calendar-sensitive and can fail near a month boundary independently of your
change.

**The console has no build step.** `internal/server/ui/*.html` is compiled into
the binary with `//go:embed`, so a UI change is `docker compose up -d --build
server` and a hard refresh — see [For new developers](#for-new-developers) for
how the SPA is laid out and how to add an endpoint behind it.

**Simulating remote sites.** `docker-compose.sites.yml` runs pool-scoped workers
that can reach nothing but the server's API, optionally behind a TLS edge —
the closest thing to a site VM without a VM. See
[Trying it locally](#trying-it-locally).

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
| `PRIMEFLOW_NATS_URL` | NATS URL for live updates (wins over Redis) | optional |
| `PRIMEFLOW_REDIS_URL` | Redis URL for live updates | optional |
| `PRIMEFLOW_QUEUES` | comma-separated lanes to poll | `default` |
| `PRIMEFLOW_API_URL` | run this worker against the API instead of the database | none |
| `PRIMEFLOW_WORKER_TOKEN` | pool-scoped `api-worker` key, required with `_API_URL` | none |
| `PRIMEFLOW_CONCURRENCY` | runs executed in parallel | `4` |
| `PRIMEFLOW_LEASE` | lease duration | `60s` |
| `PRIMEFLOW_POLL` | fallback poll interval | `2s`, or `15s` against the API |
| `PRIMEFLOW_MAX_SUBFLOW_DEPTH` | how deep `RunDeployment` may nest | `8` |
| `PRIMEFLOW_LOG_RETENTION` | prune `pf_logs` older than this (Go duration) | `720h` |
| `PRIMEFLOW_METRICS_ADDR` | worker's own `/metrics` listener (flow/task timings) | `:9090` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | send traces here; unset = tracing off, zero cost | none |
| `PRIMEFLOW_HTTP_ADDR` | server listen address | `:8080` |
| `PRIMEFLOW_API_TOKEN` | bearer token for machine clients (workers, CLI) on `/api/v1` | none |
| `PRIMEFLOW_CORS_ORIGIN` | allow the PrimeX console to call the API | none |
| `PRIMEFLOW_ADMIN_EMAIL` / `PRIMEFLOW_ADMIN_PASSWORD` | seed the first operator account on an empty database | none |
| `PRIMEFLOW_SESSION_TTL` | operator login lifetime (slides on use) | `168h` |
| `PRIMEFLOW_COOKIE_SECURE` | mark session cookies `Secure` | auto (on when a proxy is trusted) |
| `PRIMEFLOW_TRUSTED_PROXY_CIDRS` | networks whose `X-Forwarded-For` / client-cert headers are believed | none |
| `PRIMEFLOW_PUSH_ADDR` / `PRIMEFLOW_PUSH_SECRET` | push-pool receiver: listen address and dispatch HMAC secret | `:8090` / none |
| `PRIMEFLOW_OIDC_ISSUER` / `_CLIENT_ID` / `_CLIENT_SECRET` | enable OIDC SSO (issuer + client id required) | none |
| `PRIMEFLOW_OIDC_REDIRECT_URL` | OIDC callback (else derived from the request) | derived |
| `PRIMEFLOW_OIDC_DEFAULT_ROLE` / `_ROLE_CLAIM` / `_ROLE_MAP` | JIT role: fallback, claim name, `group=role,…` map | `viewer` / — / — |
| `PRIMEFLOW_RESET_TTL` | admin-issued password-reset link lifetime | `1h` |

When `PRIMEFLOW_REDIS_URL` is set — even with NATS as the bus — the login
throttle and per-API-key rate limiter use a **shared Redis token bucket**, so
limits hold across server replicas; otherwise they are per-process.

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
c.RunDeployment("notify-oncall", payload, sdk.TriggerPriority(100)) // fire and forget
child, err := c.RunDeploymentAndWait("provision-vm-standard", params) // durable wait — see Sub-flows
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

## Authentication

Two independent surfaces, detailed in [`docs/api_roles_and_permissions.md`](docs/api_roles_and_permissions.md):

- **Operator API & console** (`/api/v1`). Humans log in (`POST /api/v1/auth/login`)
  and get a session cookie carrying one of three roles — `viewer` (read-only),
  `operator` (day-to-day run/queue/deployment control), `admin` (adds user, API-key
  and settings management). Browser writes carry a double-submit CSRF token. Workers
  and the CLI keep using `PRIMEFLOW_API_TOKEN` as a bearer token, treated as `admin`.
  Seed the first admin with `PRIMEFLOW_ADMIN_*` or `primeflow user add`.
  - **SSO.** Set `PRIMEFLOW_OIDC_ISSUER` + `_CLIENT_ID` (+ `_CLIENT_SECRET`) and the
    login page gains a **Sign in with SSO** button. First login JIT-provisions an
    `oidc` account; its role comes from `_ROLE_MAP` on `_ROLE_CLAIM`, else
    `_DEFAULT_ROLE`. Local accounts and SSO accounts coexist.
  - **Password reset.** No SMTP. An admin issues a one-time link — console
    **Settings → Users → Reset link**, or `primeflow user reset-link -email …` —
    and the user sets a new password at `/reset.html`.
- **External API** (`/api/external/v1`). A role-gated, key-authenticated projection
  of runs, deployments, queues and events for external integrations. Managed from
  **Settings → External API** in the console: a global master switch, issued keys
  each with a role (scope bundle) plus per-key IP allowlist, rate limit, mutual-TLS
  requirement and PII redaction, and a per-key audit trail. A key whose role lacks a
  route's scope gets `403`; a bad or disabled key gets `401`.

## API

All operator routes are under `/api/v1`. `GET /api/v1/health` is always
unauthenticated so probes need no credential.

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
| `POST /auth/login` · `/auth/logout` · `GET /auth/me` · `GET /auth/config` | operator login |
| `GET /auth/oidc/login` · `/auth/oidc/callback` | OIDC SSO (when configured) |
| `POST /auth/reset` · `POST /users/{id}/reset-link` | password-reset links |
| `GET/POST /users`, `PATCH/DELETE /users/{id}` | operator accounts (admin) |
| `GET/PUT /settings/external-api` | External API master switch (admin) |
| `GET/POST /api-keys`, `PATCH/DELETE /api-keys/{id}`, `POST /api-keys/{id}/rotate`, `GET /api-keys/{id}/history` | External API keys (admin) |
| `GET /api-roles` | role / scope / route catalogue (admin) |
| `GET /flows/{name}` | one flow: versions, param schema, recent runs |
| `GET /queues/{name}` | one work pool: workers + computed `desired_workers` |
| `GET /runs/{id}/children` | sub-flow runs a run started |
| `GET/PUT /settings/log-retention` | `pf_logs` cleanup policy (admin) |
| `GET/PUT /settings/git` | GitOps target repo for worker delivery — repo URL, branch, base path, PAT (write-only), auto-sync flag (admin) |
| `GET /stats?window=8h` | time-bucketed activity for the Dashboard |
| `GET /metrics` | Prometheus (unauthenticated) |

The External API lives under `/api/external/v1` and is documented in
[`docs/api_roles_and_permissions.md`](docs/api_roles_and_permissions.md). It is a
key-authenticated, scope-gated projection:

| | scope |
|---|---|
| `GET /runs`, `POST /runs`, `GET /runs/{id}` (+ `/tasks` `/logs` `/artifacts`) | `read:runs` / `write:runs` |
| `GET /deployments`, `GET /deployments/{id}`, `POST /deployments/{id}/run` | `read:deployments` / `write:runs` |
| `GET /queues`, `GET /queues/{name}/pending` | `read:queues` |
| `POST /queues` — **create / update a work pool** (IaC & GitOps callers) | `write:queues` |
| `GET /workers` — live worker heartbeat table (read-only; workers self-register) | `read:workers` |
| `GET /events` | `read:events` |

---

## Deployment

The server is one static binary that needs PostgreSQL and nothing else — 14 or
newer for the dashboard's time-bucketed charts, which fall back to bare totals
below that. A bus (NATS or Redis) is an accelerator, never a dependency. Three shapes are
documented: a single VM below, Kubernetes, and
[workers at a remote site](#workers-at-a-remote-site) for a VM that should hold
no database credential.

### On a single VM

Two ways to run it, both ending at the same place. Use Compose if Docker is
already on the box; use the binary and systemd if you would rather not run a
container runtime beside your workloads.

**Ship the image or the binary.** Neither needs the source on the VM:

```bash
make docker                                  # primex/primeflow:$(git describe) and :latest
docker push primex/primeflow:latest          # to your registry

# or a plain binary, cross-compiled from anywhere:
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" \
  -o primeflow ./cmd/primeflow
```

**Postgres.** Managed is one less thing to run. Local is fine too:

```bash
sudo -u postgres createuser --pwprompt primeflow
sudo -u postgres createdb --owner=primeflow primeflow
```

The server applies its schema on start (`-no-migrate` turns that off), so there
is no separate migration step on a first boot. `primeflow migrate` exists for
the case where you want the schema applied before anything serves traffic.

**Configuration is one env file.**

```ini
# /etc/primeflow/server.env — chmod 600, it holds the DSN and the token
PRIMEFLOW_DATABASE_URL=postgres://primeflow:CHANGE_ME@127.0.0.1:5432/primeflow?sslmode=require
PRIMEFLOW_HTTP_ADDR=127.0.0.1:8080          # loopback: the proxy takes 443
PRIMEFLOW_API_TOKEN=CHANGE_ME               # machine clients (CLI, scripts)

# Seeds the first operator account, on an empty database only. Ignored once any
# user exists, so it is safe to leave in place.
PRIMEFLOW_ADMIN_EMAIL=admin@example.com
PRIMEFLOW_ADMIN_PASSWORD=CHANGE_ME

# Believe X-Forwarded-For and forwarded client-cert state from the proxy, and
# mark session cookies Secure (implied by this, since TLS terminates in front).
PRIMEFLOW_TRUSTED_PROXY_CIDRS=127.0.0.1/32
PRIMEFLOW_SESSION_TTL=168h
PRIMEFLOW_LOG_RETENTION=720h                # the janitor drops aged-out pf_logs partitions
PRIMEFLOW_LOG_LEVEL=info
```

**Compose on the VM.** [`docker-compose.yml`](docker-compose.yml) is a
development file — it builds from source and publishes Postgres to the host.
For a VM, point the services at a pushed image and stop publishing anything but
the proxy's upstream:

```yaml
server:
  image: primex/primeflow:latest      # instead of `build: .`
  env_file: [/etc/primeflow/server.env]
  ports: ["127.0.0.1:8080:8080"]      # loopback only
  restart: unless-stopped
```

**Binary and systemd.** The same shape as a site worker — a file, an env file,
a unit:

```ini
# /etc/systemd/system/primeflow-server.service
[Unit]
Description=PrimeFlow server
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
User=primeflow
EnvironmentFile=/etc/primeflow/server.env
ExecStart=/usr/local/bin/primeflow server
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now primeflow-server
curl -sS localhost:8080/api/v1/health        # open route, no credential
```

**Workers on the same VM.** A worker is your own binary with your flows compiled
in, so it gets its own unit — see
[Deploying to a site VM](#deploying-to-a-site-vm) for the unit itself. Beside
the database it takes a DSN directly:

```ini
# /etc/primeflow/worker.env
PRIMEFLOW_DATABASE_URL=postgres://primeflow:CHANGE_ME@127.0.0.1:5432/primeflow?sslmode=require
PRIMEFLOW_QUEUES=default,vcd
PRIMEFLOW_CONCURRENCY=4
PRIMEFLOW_WORKER_NAME=vm1
PRIMEFLOW_METRICS_ADDR=127.0.0.1:9090       # keep it off the public interface
```

One thing to know about a single VM with no bus: the in-process fallback does
not cross process boundaries, so a separate worker process hears about new work
on its next poll (`PRIMEFLOW_POLL`, 2s) rather than immediately. That is correct,
just not instant. Running Redis or NATS on the box and setting
`PRIMEFLOW_REDIS_URL` / `PRIMEFLOW_NATS_URL` on both processes takes dispatch
sub-second. So does collapsing the two into one process — but that has to be
*your* binary, since flows are compiled in: register them and call
`app.ServeAll` instead of `app.ServeAPI` (`primeflow server -with-worker` runs
the same mode on the stock binary, which has no flows registered, so it is a
development convenience rather than a deployment).

**TLS in front.** The server speaks plain HTTP, so something terminates 443.
[`sites/nginx.conf`](sites/nginx.conf) is the reference, and
[the proxy notes](#the-proxy-in-front-of-the-server) explain the two things that
fail silently. Both server-sent-event endpoints are covered there — the worker
wake-up channel and the console's live feed — which matters here because a VM
serving the console through the same proxy is the case where a buffered
`/api/v1/stream` leaves the dashboard frozen while every other page loads fine.

**Firewall.** Inbound 443 for the proxy is the whole public surface. Postgres,
the server's `PRIMEFLOW_HTTP_ADDR` and every worker's `PRIMEFLOW_METRICS_ADDR`
belong on loopback or a private interface. Remote sites need no inbound port at
all; they dial out.

**Upgrades.** Additive, idempotent migrations run on start, so an upgrade is a
new binary or image tag and a restart. Workers can be restarted at any point:
an interrupted run's lease expires, the janitor marks it crashed, and it resumes
from its last checkpoint on whichever worker leases it next. Give
`TimeoutStopSec` enough room to reach the next checkpoint and the common case
does not even reach that path.

**Backups.** Postgres holds everything — runs, checkpoints, users, API keys,
settings. Back it up normally. Nothing on a worker is worth preserving, which is
what makes replacing one a non-event.

### Kubernetes

[`deploy/k8s/primeflow.yaml`](deploy/k8s/primeflow.yaml) has a server Deployment
(safe to scale: leader election handles the singleton loops) and one worker
Deployment per queue, so a slow lane scales independently.

Give workers a `terminationGracePeriodSeconds` long enough to reach the next
checkpoint. Past it nothing is lost either — the lease expires and another
worker resumes the run.

### Sizing

The dispatch query is a single indexed statement per queue per poll. One
Postgres instance comfortably handles tens of thousands of runs a day; the
`pf_logs` table is the one that grows, so set `PRIMEFLOW_LOG_RETENTION` when you
turn this on for real.

---

## Testing

```bash
make test-unit          # no database needed
make test-integration   # everything, against a real Postgres
```

Point `TEST_DB` at a database you do not mind losing — the integration packages
reset it, and each other, which is why `make test-integration` passes `-p 1`.
[Local development](#local-development) has the setup.

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
internal/store/remote/  the same worker-facing interface over /api/v1/worker (remote sites)
internal/engine/        durable execution: checkpoints, retries, cancellation
internal/worker/        leasing, heartbeats, graceful drain
internal/server/        REST API, SSE stream, embedded console
internal/scheduler/     schedule materialisation and the lease janitor
internal/automations/   event-driven rules
internal/bus/           NATS and Redis pub/sub, with an in-process fallback
internal/metrics/       Prometheus registry + scrape-time queue collector
internal/otelinit/      OTLP tracing setup (no-op unless an endpoint is set)
examples/primex-worker/ VM provisioning, metering, and a sub-flow fleet demo
deploy/k8s/             manifests + KEDA/HPA autoscaling examples
docker-compose.sites.yml, sites/
                        remote-site simulation: site workers, key issuing, nginx edge
```

---

## For new developers

**Get it running.** [Quick start](#quick-start) brings the stack up;
[Local development](#local-development) covers host ports, building without a Go
toolchain, running a process outside the container, and the test database. What
follows is how the code is laid out once it is running.

**The console is embedded, no build step.** `internal/server/ui/*.html` is
compiled into the binary via `//go:embed` ([`internal/server/ui.go`](internal/server/ui.go)).
`index.html` is the whole SPA — one `<style>`, one `<script>`, view sections
`#v-<name>` toggled by `show(name)`, data via `api('/path')`. Edit it, rebuild
the server image (`docker compose up -d --build server`), hard-refresh (favicons
and the bundle cache hard).

**Add an endpoint.**
1. Handler in `internal/server/*.go` (`handlers.go`, `flows.go`, `apikeys.go`, …).
   Use `decode(r, &body)`, `writeJSON`, `writeErr`, `fail`.
2. Route in `internal/server/server.go` (`mux.HandleFunc("METHOD /api/v1/…", s.h)`).
   `/api/v1/settings/*`, `/api/v1/users*`, `/api/v1/api-keys*` are admin-gated by
   `adminOperatorPath`.
3. External twin (optional): handler in `internal/server/external.go`, route in
   `externalMux()`, and add it to `apiauth.Routes` with a `Scope` — that slice is
   the single source of truth the router, the auth middleware and the API
   Explorer all read.

**Add a store method + setting.** Persistence is an interface
([`internal/store/store.go`](internal/store/store.go)) with one implementation
(`internal/store/postgres`). Instance settings are JSON rows in `pf_settings`
keyed by a string — copy `GetGitConnection` / `PutGitConnection`
([`internal/store/postgres/gitconn.go`](internal/store/postgres/gitconn.go)): no
migration, `INSERT … ON CONFLICT (key) DO UPDATE`, and keep secrets out of the
read path.

**Add a schema migration.** Drop
`internal/store/postgres/migrations/000N_name.sql` — additive and idempotent
(`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`), embedded and applied
in order on server start unless `-no-migrate`.

**Tests.** See [Local development](#local-development) for the test database, and
[Testing](#testing) for what the suite actually pins down.

---

## Transports

The notification bus has three implementations behind one interface
(`internal/bus`). Precedence: **NATS → Redis → in-process**.

| Set | Transport | Notes |
|---|---|---|
| `PRIMEFLOW_NATS_URL` | core NATS pub/sub | cluster-friendly; `nats://host:4222` |
| `PRIMEFLOW_REDIS_URL` | Redis pub/sub | also fine for the UI stream |
| neither | in-process | single binary + Postgres; workers fall back to `PRIMEFLOW_POLL` |

A dial failure at start-up logs a warning and degrades to polling — the bus is
never load-bearing.

## Observability

**Metrics.** `GET /metrics` (unauthenticated, like `/api/v1/health`) exposes:

| Metric | Type | Meaning |
|---|---|---|
| `primeflow_queue_ready{queue}` | gauge | scheduled runs whose time has come |
| `primeflow_queue_scheduled{queue}` · `_running{queue}` | gauge | backlog and in-flight |
| `primeflow_queue_desired_workers{queue}` | gauge | `clamp(ceil(ready/target), min, max)` — the autoscale target |
| `primeflow_workers_online` · `_total` | gauge | fleet liveness |
| `primeflow_flow_run_transitions_total{to_state}` | counter | state changes |
| `primeflow_flow_run_duration_seconds` · `primeflow_task_run_duration_seconds{outcome}` | histogram | execution timings |
| `primeflow_http_requests_total{route,method,code}` · `_duration_seconds{route}` | counter/histogram | API RED |

**Traces.** Set `OTEL_EXPORTER_OTLP_ENDPOINT` (standard OTEL env) and PrimeFlow
emits `flow_run` → `task_run` spans over OTLP/HTTP, with trace context propagated
from HTTP callers and across `RunDeployment` into child runs. Unset, the tracer
is a no-op and costs nothing.

## Sub-flows

`RunDeployment(name, params)` fans out and returns immediately. To wait:

```go
child, err := c.RunDeploymentAndWait("provision-vm-standard", params)
if err != nil { return nil, err }      // a failed child is a permanent error
var vm VM
_ = child.Into(&vm)
```

It is a durable checkpoint: the child is triggered exactly once no matter how
often the parent replays, the parent releases its worker slot while the child
runs, and it resumes the instant the last child settles (or re-polls every 30s
as a backstop). Every child records its `parent_run_id`, so the console shows the
tree. Recursion is bounded by `PRIMEFLOW_MAX_SUBFLOW_DEPTH` (default 8) and a
child whose deployment already appears in the ancestor chain is refused.

## Work pools & autoscaling

A work queue doubles as a **work pool**: give it `min_workers`, `max_workers`,
`target_ready_per_worker` and an `owner` (`POST /api/v1/queues`, or the console's
**Work Pools → Autoscale…**). The server then publishes
`primeflow_queue_desired_workers{queue}` and a KEDA `ScaledObject` or HPA scales
the matching worker Deployment. PrimeFlow **never launches workers itself** — it
publishes the target, Kubernetes acts. See
[`deploy/k8s/primeflow.yaml`](deploy/k8s/primeflow.yaml) for both.

**External teams run their own workers**: point `primex-worker` at your pool with
`PRIMEFLOW_QUEUES=<pool>`; the `owner` field groups it in the console. Workers
never block each other — dispatch is one `SKIP LOCKED` statement per poll.

### Push pools

Set `pool_type: "push"` and a `push_endpoint` (console **Work Pools → Push
endpoint…**, or `POST /api/v1/queues`) and the pool has no polling workers.
Instead the leader `POST`s each ready run — `{run_id, flow_name, …}`, HMAC-signed
with the pool's `push_secret` as `X-PrimeFlow-Signature: sha256=…` — to the
endpoint. The receiver is your PrimeFlow binary run as
`primeflow.RunPushWorker` (or `primex-worker -push`): it verifies the signature,
claims that one run, executes it with the engine, and reports normally. A
dispatch that is never claimed lapses and is retried, then reclaimed as
`CRASHED` by the janitor like any abandoned run. Point `push_endpoint` at a
Knative `Service` / Cloud Run URL for scale-to-zero — the server pings only when
there is work.

## Console

Single embedded page, no build step. It opens on a **Dashboard** — time-bucketed
flow-run / task-run / event charts over 8h · 24h · 1w (`GET /api/v1/stats`,
Postgres 14+ for `date_bin`; without it the dashboard shows bare totals), plus
recent-flow and work-pool cards. **Runs** has a timeline strip and segmented
filters; open any run for a Temporal-style execution **Timeline** (a lane per
checkpoint and sub-flow on a shared time axis). **Event feed** is a rail
timeline. **Flows** lists every registered flow with its param schema and a typed
quick-run form — declare a schema so the form is typed:

```go
sdk.Flow("provision-vm", provisionVM,
    sdk.ParamsSchema(ProvisionParams{OrgName: "acme", CPU: 2}))
```

### Work Pools

**Create pool…** opens a form (name, `pull`/`push` type, concurrency limit,
owner, and — for pull pools — the `min` / `max` / `target-ready-per-worker`
autoscaling envelope; for push pools — the endpoint URL and HMAC secret). Rows
carry **Pause/Resume**, **Limit…**, **Autoscale…** and **Make push…**.

### Workers

The table lists the live heartbeat rows (workers self-register on start). **Click
a row** for a detail dialog: parameters (id, pools, concurrency, active runs,
heartbeat), the resolved `PRIMEFLOW_*` env, the **Deployment + Secret YAML**
rendered from that worker's live config, and the **package list** the host needs
(base components plus flow-specific ones inferred from the registered flows'
tags — e.g. a `vcd` tag adds "VMware Cloud Director API + client library").

**Add worker…** is a full page, not a modal. It captures name / concurrency /
image, the pools it serves (**+ Create pool…** inline), a **Connection** mode —
*Database* (DSN + bus, for a worker beside Postgres) or *API* (a remote site:
`PRIMEFLOW_API_URL` plus a pool-scoped `api-worker` key, which an admin can mint
inline with **Issue key…**, scoped to exactly the pools ticked) — a
**host-requirements checklist that gates generation** (its network line follows
the mode), and a **Delivery** method:

- **Git commit + PR/MR** (default) — renders `secret.yaml`, `deployment.yaml`,
  `kustomization.yaml`, then `git clone → checkout -b → add → commit → push` and
  a host-aware `gh pr create` / `glab mr create`.
- **Argo CD Application** — plus an `argoproj.io/v1alpha1 Application` CR and
  `argocd app create`.
- **Flux Kustomization** — plus a `kustomize.toolkit.fluxcd.io/v1 Kustomization`.
- **Script** — Docker `run` / systemd unit / `kubectl apply`.

An **Auto-sync** toggle threads through every GitOps mode: on ⇒
`syncPolicy.automated` (Argo) / no `suspend` (Flux); off ⇒ manual, and the
output appends the exact **sync command** (`argocd app sync` /
`flux reconcile kustomization … --with-source`). The git fields pre-fill from
**Settings → Git connection**.

### Settings → Git connection

Stores the GitOps target repo (`GET/PUT /api/v1/settings/git`): repo URL, branch,
base path, commit author, and a **write-only PAT** (stored, never returned; a
read reports only `has_token`). The provider (`github` / `gitlab` / `other`) is
derived from the URL. Today this connection pre-fills the Add-worker git fields;
**server-side clone/commit/push and an auto-sync reconciler are the next
iteration** (see Known gaps).

---

## GitOps worker delivery

PrimeFlow does not deploy workers itself — it renders the manifests and hands you
the apply path. The current flow:

1. Configure **Settings → Git connection** once.
2. **Workers → Add worker…**, pick pools + delivery method + auto-sync, confirm
   the host-requirements checklist.
3. Copy the generated bundle (manifests + Argo/Flux CR + git/sync commands) into
   your GitOps repo. Your CD controller reconciles it; the worker registers on
   start and appears in the Workers table, where its live config round-trips back
   to the same YAML.

**External / IaC callers** get the write half of pool management with
`POST /api/external/v1/queues` (scope `write:queues`) and read the fleet with
`GET /api/external/v1/workers` (scope `read:workers`).

---

## Workers at a remote site

A worker normally opens its own PostgreSQL connection. That is the right shape
beside the database and the wrong one across a site boundary: the VM would hold
a credential that reads every table, including operator password hashes and API
keys, and it would need 5432 open across the WAN.

Set `PRIMEFLOW_API_URL` and `PRIMEFLOW_WORKER_TOKEN` instead and the worker
reaches the orchestrator through `/api/v1/worker/*` — outbound HTTPS only, no
inbound port at the site, no database credential anywhere on the box.

```bash
export PRIMEFLOW_API_URL="https://primeflow.example.com"
export PRIMEFLOW_WORKER_TOKEN="pmx_…"     # an api-worker key, scoped to this site's pools
export PRIMEFLOW_QUEUES="site-bkk"
./primex-worker                            # no PRIMEFLOW_DATABASE_URL
```

Issue the key from **Workers → Add worker…** (Connection: *API*, then
**Issue key…** — the page renders the site's `worker.env`, systemd unit or
manifests with the key already in place), from **Settings → External API**, or:

```bash
curl -X POST $PRIMEFLOW_API_URL/api/v1/api-keys -d '{
  "name": "site-bkk worker", "role": "api-worker", "pools": ["site-bkk"]
}'
```

The `pools` list is the boundary. A key may lease only from the lanes it names,
and a run on any other lane is refused even when the caller knows its id;
refusals land in that key's audit trail, so a site reaching outside its lanes is
visible in the console. Everything else the External API offers applies too —
IP allow-list, a required client certificate, a per-key rate limit — and
disabling one key revokes one site, which a shared database password cannot do.

**What changes at a site.** Wake-ups arrive on `GET /api/v1/worker/stream`, an
SSE channel on the same connection, so dispatch stays sub-second without NATS or
Redis; losing it costs latency and nothing else, because the poll is still
there. The poll itself defaults to 15s rather than 2s, and one heartbeat carries
liveness, every lease renewal and any cancellation, so a worker's request rate
does not grow with how much work it is holding.

**What does not change.** Checkpoints, leases, the transition table and the
dispatch ordering are all still the server's, enforced in exactly one place. A
site that loses its link mid-run has its lease reclaimed by the janitor, is
marked `CRASHED`, and resumes from its checkpoints — the expensive step is not
repeated.

**Rolling it out.** Migrate one pool at a time: issue a key for that pool, start
a remote worker beside the existing database-connected one, watch both take work
from the lane, then stop the old one and remove its DSN. Nothing needs to happen
at once, and the two kinds of worker are interchangeable from the server's side.

### Deploying to a site VM

The worker is one static binary with your flows compiled in, so a site gets a
file, an env file and a unit — no runtime, no package manager, no git access.

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" \
  -o primex-worker ./examples/primex-worker      # or your own main()
```

```ini
# /etc/primeflow/worker.env — the whole configuration of a site
PRIMEFLOW_API_URL=https://primeflow.example.com
PRIMEFLOW_WORKER_TOKEN=pmx_…
PRIMEFLOW_QUEUES=site-bkk
PRIMEFLOW_WORKER_NAME=site-bkk-vm1
PRIMEFLOW_CONCURRENCY=4
PRIMEFLOW_METRICS_ADDR=127.0.0.1:9090       # the only port a worker opens; keep it on loopback
```

```ini
# /etc/systemd/system/primeflow-worker.service
[Unit]
Description=PrimeFlow worker (site-bkk)
After=network-online.target
Wants=network-online.target

[Service]
User=primeflow
EnvironmentFile=/etc/primeflow/worker.env
ExecStart=/usr/local/bin/primex-worker
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=120

[Install]
WantedBy=multi-user.target
```

`TimeoutStopSec` is the same knob as `terminationGracePeriodSeconds` on
Kubernetes: long enough to reach the next checkpoint, harmless past it.
`Restart=always` is what turns a crash into a resume — the new process gets a
new worker id, the old lease expires, and the run continues from its
checkpoints on whichever worker in the pool leases it next.

The first two log lines tell you it worked: `worker store: primeflow api`, then
`wake-up stream connected`. The worker appears in **Workers** with its pools.
Trigger a run on one of them and watch scheduled → completed: well under a
second means the stream is doing the waking; a steady ~15s means something
between the site and the server is buffering it (see the proxy notes below).

Things that bite:

- `PRIMEFLOW_QUEUES` must lie inside the key's `pools`. Lanes outside it are
  silently dropped from every lease, so a mismatched worker looks healthy and
  never takes work.
- Remote mode runs workers only. `primeflow server` and `primeflow migrate`
  refuse to start without a database rather than guess.
- A `PRIMEFLOW_DATABASE_URL` set alongside the API variables is ignored with a
  warning. Remove it, so the box holds no credential it does not use.
- To rotate: **Settings → External API → Rotate** (or `POST
  /api/v1/api-keys/{id}/rotate`), update the env file, restart the unit.

### The proxy in front of the server

The server speaks plain HTTP on `PRIMEFLOW_HTTP_ADDR`. Whatever terminates 443
in front of it has to get two things right, and both fail silently:

- **Do not buffer the two SSE endpoints.** `/api/v1/worker/stream` is the worker
  wake-up channel: a proxy that holds the bytes turns sub-second dispatch into
  the 15s poll. `/api/v1/stream` is the console's live event feed: holding those
  bytes freezes the dashboard. Neither reports an error.
- **Re-resolve the upstream.** nginx resolves a name in `proxy_pass` once, at
  start-up. Behind Docker or any DNS-based discovery the server moves on a
  restart and the proxy keeps the old address.

[`sites/nginx.conf`](sites/nginx.conf) does both and is meant to be lifted:

```nginx
resolver 127.0.0.11 valid=10s;              # your resolver outside Docker
set $upstream http://server:8080;

location ~ ^/api/v1/(worker/)?stream$ {   # both SSE endpoints
    proxy_pass $upstream;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;
    proxy_read_timeout 1h;
}
location / {
    proxy_pass $upstream;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
}
```

Set `PRIMEFLOW_TRUSTED_PROXY_CIDRS` to the proxy's network if any key uses an
IP allow-list or a required client certificate — that is what lets the server
believe `X-Forwarded-For` and `X-SSL-Client-Verify` from it.

### What crosses the link

Everything a worker does is a call on `/api/v1/worker/*`, guarded by the key:

| Call | Carries |
|---|---|
| `GET /stream` | SSE, held open: work and cancellation notices, filtered to the key's pools |
| `POST /lease` | the lanes to poll, narrowed to the key's pools; returns the leased runs |
| `PUT /runs/{id}/tasks/{key}` | a task checkpoint — the unit of replay |
| `POST /runs/{id}/logs`, `…/artifacts` | flow logs and artifacts |
| `POST /heartbeat` | liveness, every lease renewal, and the runs an operator asked to stop |
| `POST /runs/{id}/state` | a transition; the server writes the event and publishes it on the worker's behalf |

Per interval that is two calls whether the worker holds one run or eight
(`TestHeartbeatCostIsIndependentOfHeldRuns` pins it); the rest is proportional
to work done. Three things are never taken from a request body: the worker id
(it comes from the credential), `force` (the janitor's), and the clock a cache
entry is checked against (the server's, so a skewed site cannot extend one).
There is no route that writes the event log, because automations act on it. A
refusal is an answer, not a retry: 4xx is final, 5xx and transport failures
back off with jitter.

### Trying it locally

[`docker-compose.sites.yml`](docker-compose.sites.yml) runs three "site VMs" as
containers beside the main stack. Each boots from an env file holding a
pool-scoped key, sits on a network the server joins and Postgres, NATS and
Redis do not, and can reach nothing else.

```bash
./sites/issue-keys.sh                                      # pools + keys → sites/site-*.env
docker compose -f docker-compose.sites.yml up -d --build   # plain http, straight to server:8080
docker compose -f docker-compose.sites.yml logs -f site-a
```

Add `--env-file sites/tls.env` to the `up` and an nginx edge takes 443 in
front of the server — worth one run before a real rollout, since it is the
proxy, not the worker, that usually breaks. The key script is idempotent (a key
it finds is rotated, not duplicated) and the env files are gitignored.

Measured on this harness: a run on a site pool completes ~160ms after it is
scheduled in both modes; a `site-a` key gets an empty lease for `site-b` and
`403` on a `site-b` run id, logged as `pool-denied:site-b` in the key's
history; a paused pool holds a run until resumed; `kill -9` mid-flow is
rescheduled when the lease expires and replayed with `create-vm` returning its
checkpoint rather than running again (`docker kill`, then `docker compose …
start site-a` — Docker treats kill as a manual stop); and a server restart
under nginx is followed by a completed run without touching the edge.

---

## Known gaps

Honest list of what is not built yet:

- **`pf_logs` partitioning migration on a large table.** New installs and small
  ones convert instantly; converting a `pf_logs` that already holds millions of
  rows does one validation scan on the `ATTACH` — run it in a maintenance window.
  After that the janitor `DROP`s whole aged-out monthly partitions.
- **SMTP.** Password reset is admin-issued one-time links, not a self-service
  "forgot my password" email flow.
- **Per-user API keys.** External-API keys belong to the instance and are
  created by admins.
- **NATS-backed rate limiting.** Shared limits use Redis or fall back to
  in-process; there is no NATS/JetStream limiter.
- **Push pools don't build or ship your code.** The receiver is still your own
  PrimeFlow binary — though since the worker API landed it no longer needs
  database access, only a pool-scoped key. There is still no code-upload step.
- **Single OTLP exporter.** Traces only; no metrics-over-OTLP, no log export.
- **GitOps worker delivery is generate-only so far.** *Settings → Git
  connection* is stored and the Add-worker page renders every artifact, but the
  server does not yet clone/commit/push. Next iteration: a `pf_worker_specs`
  table with full CRUD (operator + `/api/external/v1/worker-specs`), a
  server-side `git` engine, an auto-sync reconciler, and `git` in the (currently
  distroless) image. Until then the Workers detail dialog shows *Sync now* /
  *Auto-sync* as disabled placeholders and the Add-worker page emits the sync
  commands for you to run.
