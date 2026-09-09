# Deploying PrimeFlow with container images

A two-VM install: `mainvm` runs the control plane, `vm-worker1` runs a work
pool and holds no database credential. Both run the same image.

This covers the container path. For the plain-binary-and-systemd path, and for
Kubernetes, see [Deployment](../README.md#deployment) in the
README. The environment variables, the proxy rules and the failure modes
are the same in every shape; only the packaging differs.

---

## 1. One image, two entrypoints

[`Dockerfile`](../Dockerfile) compiles both binaries and copies them into a
distroless base:

| Layer | Size |
|---|---|
| `/usr/local/bin/primeflow` — server, scheduler, console | 24.7 MB |
| `/usr/local/bin/primex-worker` — the example worker | 22.2 MB |
| `gcr.io/distroless/static-debian12:nonroot` + CA bundle | ~24 MB |
| **Total** | **~71 MB** |

(Layer sizes from `docker history`; the stripped binaries themselves are a
little smaller. The `Dockerfile` defaults `TARGETARCH` to `amd64`, so a plain
`docker build` on an arm64 machine produces an **amd64** binary inside an
arm64-labelled image — it runs there only under emulation. Pass
`--build-arg TARGETARCH=arm64`, or build with `docker buildx --platform`, if the
target is arm64.)

`ENTRYPOINT` is the server, so the image runs as the control plane by default.
A worker is the same image with the entrypoint overridden. Nothing else
differs — no worker-specific tag, no second build.

What this buys: one artefact to sign, scan and promote, and a guarantee that
server and worker were built from the same commit.

What it costs is the spare 24 MB binary, and not much else — but the reason is
worth knowing before you plan around image size. Measured, stripped:

| Binary | Size |
|---|---|
| worker via `pkg/primeflow/worker`, six flows | 19.0 MB |
| the same worker via the parent `pkg/primeflow` | 19.7 MB |
| worker with **no flows at all** | 19.6 MB |
| server | 21.9 MB |

Two things fall out of that table. The flows are ~130 KB: essentially all of a
worker is framework, and ~19 MB is the floor for any worker you build. And
importing the parent package instead of `pkg/primeflow/worker` costs only 0.7 MB
— the parent offers every mode from one package, so it links the server's whole
graph, but the linker then drops almost all of the code no `main` can reach.

The 0.7 MB is not why the split matters. Building against
`pkg/primeflow/worker` keeps `go-oidc`, `go-jose`, the cron parser and the
embedded time-zone database **out of the binary entirely**, so they do not turn
up when someone scans it.

> The server binary is generic. The **worker** binary is not: flows are Go
> functions compiled in.

---

## 2. What goes where

| | `mainvm` | `vm-worker1` |
|---|---|---|
| Runs | `primeflow server` | `primex-worker` |
| Needs Postgres | yes, directly | **no** |
| Credential held | DB DSN, admin seed, API token | one pool-scoped API key |
| Inbound ports | 443 (proxy) | **none** |
| Outbound | Postgres | HTTPS to `mainvm` |
| Loss of the VM means | outage | the pool's runs resume elsewhere |

A worker dials out and keeps one long-lived `GET` open for wake-ups. That is
the whole reason a site VM needs no inbound rule and no database credential.

---

## 3. Your flows need your image

The published image's worker is built from `./examples/primex-worker`, so it
knows exactly the flows that package registers. If you have your own flows —
and in production you do — build your own image. You do not fork this repo;
you depend on two packages:

```go
import (
    "github.com/DetroittTxP/primeflow/pkg/sdk"              // Flow, Task, Do, Sleep
    "github.com/DetroittTxP/primeflow/pkg/primeflow/worker" // Run, RunPush
)
```

That worker lives in a repository of its own:

```bash
go mod init github.com/you/my-worker
go get github.com/DetroittTxP/primeflow@v0.2.0
```

Only tags cut *after* the module path was renamed resolve — `v0.1.0` still
declares the old `github.com/primex/primeflow` and `go get` will refuse it. The
repository is private, so each developer machine and each CI runner needs the
module proxy bypassed and fetches sent over SSH, once:

```bash
go env -w GOPRIVATE=github.com/DetroittTxP/*
git config --global url."git@github.com:".insteadOf "https://github.com/"
```

[`myworker/`](../myworker/) in this repository is that skeleton already written
— its own `go.mod`, two working flows, a Dockerfile — meant to be copied out.

```dockerfile
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/worker ./cmd/myworker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/worker /usr/local/bin/worker
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/worker"]
```

Import `pkg/primeflow/worker`, not the parent `pkg/primeflow`, for the reason in
[§1](#1-one-image-two-entrypoints). Either way your image is about the size of
the stock one: `internal/...` is closed to code outside this module by Go's own
rule, so every worker links the same execution core. Budget ~19 MB plus your
flows and their dependencies, not a few hundred kilobytes.

A worker that leases a run for a flow it does not have registered does not fail
it: the run goes back to the queue as `AwaitingWorker` and is retried in 30
seconds. Pools may therefore run different flow sets safely, and a partial
rollout is not an outage.

**Changing a flow means rebuilding and redeploying the worker image only.** The
server holds no flow code and does not need to move.

---

## 4. Build and push

```bash
make docker                          # detroitttttxp/primeflow:$(git describe) and :latest
docker push detroitttttxp/primeflow:$(git describe --tags --always)
docker push detroitttttxp/primeflow:latest
```

Deploy the `git describe` tag, not `latest` — `latest` makes a rollback a
question of what was pulled when.

---

## 5. `mainvm` — the control plane

### Postgres

Postgres 14 or newer; older works but the dashboard's time-bucketed charts fall
back to plain totals. Managed is one less thing to run.

```bash
sudo -u postgres createuser --pwprompt primeflow
sudo -u postgres createdb --owner=primeflow primeflow
```

The server applies its schema on start, so a first boot needs no migration
step. `primeflow migrate` exists for applying it before anything serves
traffic; `-no-migrate` turns the automatic path off.

### `/etc/primeflow/server.env`

`chmod 600` — it holds the DSN and the machine token.

```ini
PRIMEFLOW_DATABASE_URL=postgres://primeflow:CHANGE_ME@10.0.0.5:5432/primeflow?sslmode=require
PRIMEFLOW_API_TOKEN=CHANGE_ME               # machine clients: CLI, scripts

# Seeds the first operator, on an empty database only. Ignored once any user
# exists, so it is safe to leave in place.
PRIMEFLOW_ADMIN_EMAIL=admin@example.com
PRIMEFLOW_ADMIN_PASSWORD=CHANGE_ME

# Believe X-Forwarded-For from the proxy, and mark session cookies Secure.
PRIMEFLOW_TRUSTED_PROXY_CIDRS=127.0.0.1/32
PRIMEFLOW_SESSION_TTL=168h
PRIMEFLOW_LOG_RETENTION=720h                # the janitor drops aged-out pf_logs partitions
PRIMEFLOW_LOG_LEVEL=info
```

> **Do not copy `PRIMEFLOW_HTTP_ADDR=127.0.0.1:8080` from the binary
> deployment.** Inside a container that binds to the container's own loopback
> and the published port reaches nothing. Leave it unset — the default `:8080`
> is correct — and restrict the exposure on the host, in `ports:`.

### Compose

```yaml
services:
  server:
    image: detroitttttxp/primeflow:1.4.0
    env_file: [/etc/primeflow/server.env]
    ports: ["127.0.0.1:8080:8080"]     # host loopback only; the proxy holds 443
    restart: unless-stopped
```

### The proxy

The server speaks plain HTTP; something terminates 443 in front of it.
[`sites/nginx.conf`](../sites/nginx.conf) is the reference and is meant to be
lifted. Two things have to be right and **both fail silently**:

```nginx
resolver 127.0.0.11 valid=10s;              # your resolver, outside Docker
set $upstream http://127.0.0.1:8080;

location ~ ^/api/v1/(worker/)?stream$ {     # both SSE endpoints
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

- **Buffering the SSE endpoints.** `/api/v1/worker/stream` is the worker wake-up
  channel: buffering it turns sub-second dispatch into the poll interval.
  `/api/v1/stream` is the console's live feed: buffering it freezes the
  dashboard while every other page loads normally. Neither reports an error.
- **Resolving the upstream once.** nginx resolves a name in `proxy_pass` at
  start-up. Behind Docker or any DNS-based discovery the server moves on a
  restart and the proxy keeps talking to the old address.

### Firewall

Inbound 443 is the entire public surface. Postgres, the published `8080` and
every worker's metrics port belong on loopback or a private interface.

---

## 6. `vm-worker1` — the worker

### The pool and its key

Create the pool and issue a key scoped to it. From the console: **Workers → Add
worker… → Issue key…**, which renders the `worker.env` and a systemd unit with
the key already in place. From the API:

```bash
curl -X POST $PRIMEFLOW_API_URL/api/v1/api-keys -d '{
  "name": "vm-worker1", "role": "api-worker", "pools": ["vm-worker1"]
}'
```

Or reuse [`sites/issue-keys.sh`](../sites/issue-keys.sh), which creates the
pool, issues or rotates the key, and writes the env file:

```bash
PRIMEFLOW_URL=https://primeflow.example.com \
PRIMEFLOW_ADMIN=admin@example.com \
SITES="vm-worker1" ./sites/issue-keys.sh
```

The `pools` list is a boundary the server enforces, in two different ways:
`/worker/lease` **silently filters** requested pools down to the key's, and the
per-run endpoints **refuse** with 403 and record the attempt in that key's
audit trail as `pool-denied:<pool>`.

### `/etc/primeflow/worker.env`

`chmod 600`. If you used `issue-keys.sh`, add `PRIMEFLOW_API_URL` — the script
leaves it out because Compose supplies it there.

```ini
PRIMEFLOW_API_URL=https://primeflow.example.com
PRIMEFLOW_WORKER_TOKEN=pmx_…
PRIMEFLOW_QUEUES=vm-worker1                 # must be inside the key's pools
PRIMEFLOW_WORKER_NAME=vm-worker1
PRIMEFLOW_CONCURRENCY=4                     # runs at once
PRIMEFLOW_LEASE=60s                         # how long a claim survives without a heartbeat
PRIMEFLOW_METRICS_ADDR=127.0.0.1:9090       # the only port a worker opens
```

No `PRIMEFLOW_DATABASE_URL`. Set beside the API variables it is ignored with a
warning, but the point is that the VM should not hold a credential it does not
use.

### Compose

```yaml
services:
  worker:
    image: detroitttttxp/primeflow:1.4.0
    entrypoint: ["/usr/local/bin/primex-worker"]   # or your own image, which needs none
    env_file: [/etc/primeflow/worker.env]
    restart: unless-stopped
    stop_grace_period: 120s      # room to reach the next checkpoint before SIGKILL
    # no ports: a worker only dials out
```

Or without Compose:

```bash
docker run -d --name primeflow-worker --restart unless-stopped \
  --stop-timeout 120 \
  --entrypoint /usr/local/bin/primex-worker \
  --env-file /etc/primeflow/worker.env \
  detroitttttxp/primeflow:1.4.0
```

`stop_grace_period` is the same knob as systemd's `TimeoutStopSec` and
Kubernetes' `terminationGracePeriodSeconds`: long enough to reach the next
checkpoint, harmless past it.

### Firewall

Nothing inbound. Outbound 443 to `mainvm` is the only rule a worker needs.

---

## 7. Verifying

```bash
curl -sS https://primeflow.example.com/api/v1/health     # open route, no credential
docker logs primeflow-worker
```

The worker's first two log lines tell you whether it worked:

```
worker store: primeflow api   url=https://primeflow.example.com  queues=["vm-worker1"]
wake-up stream connected
```

The worker then appears under **Workers** with its pools. Trigger a run on one
of them and watch the time from scheduled to running:

| Observed | Meaning |
|---|---|
| well under a second | the wake-up stream is doing its job |
| a steady ~2–15s | something between the worker and the server is buffering SSE — [§5](#the-proxy) |
| never starts | pool paused, or `PRIMEFLOW_QUEUES` outside the key's pools |

The Deployments and Runs pages badge the first two of those causes on the pool
column — `pool paused`, `no worker`, `at limit`, `no endpoint`.

---

## 8. Things that bite

- **`PRIMEFLOW_QUEUES` outside the key's `pools`.** Lanes outside the key are
  dropped from every lease with no error, so the worker looks perfectly healthy,
  heartbeats, and never takes a single run. There is no badge for this one: the
  worker *is* online. Check the key's pools first.
- **A paused pool.** Runs sit in `SCHEDULED` for as long as it stays paused.
- **Copying `PRIMEFLOW_HTTP_ADDR` into a container.** See [§5](#etcprimeflowserverenv).
- **No shell in the image.** Distroless has no `sh`, so `docker exec … sh`
  fails. Debug from `docker logs`, `/api/v1/health`, and the metrics port.
- **Remote mode runs workers only.** `primeflow server` and `primeflow migrate`
  refuse to start without a database rather than guess.
- **Rotating a key** — Settings → External API → Rotate, or
  `POST /api/v1/api-keys/{id}/rotate` — needs the env file updated and the
  container restarted.

---

## 9. Upgrades, rollback, backups

Migrations are additive and idempotent and run on start, so an upgrade is a new
tag and a restart:

```bash
docker compose pull && docker compose up -d
```

Roll back by deploying the previous tag. Because migrations only add, an older
server runs against a newer schema.

Workers can be restarted at any point, including mid-run. The interrupted run's
lease expires, the janitor marks it crashed, and it resumes **from its last
checkpoint** on whichever worker leases it next — completed tasks replay from
their stored results instead of executing again. With enough
`stop_grace_period` the common case reaches the next checkpoint cleanly and
never touches that path at all.

Back up Postgres. It holds runs, checkpoints, users, keys and settings — every
piece of state there is. Nothing on a worker is worth preserving, which is what
makes replacing one a non-event.
