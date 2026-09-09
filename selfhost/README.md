# PrimeFlow, self-hosted

Everything needed to run PrimeFlow on one machine — Postgres, NATS, the server
(API **and** the operator console), and a worker — plus the starting point for a
worker carrying your own flows.

Nothing here builds from source. The compose file pulls images, so this
directory is the whole distribution: copy it to a box, fill in `.env`, `up -d`.

| Path | What it is |
|---|---|
| [`docker-compose.yml`](docker-compose.yml) | the stack: Postgres, NATS, migrate, server + console, one worker |
| [`.env.example`](.env.example) | every knob; the two that must change are marked `CHANGE_ME` |
| [`worker/`](worker/) | a Go module of your own that imports the SDK — copy it out and put your flows in it |

---

## 1. Run it

```bash
cp .env.example .env && chmod 600 .env
$EDITOR .env                      # POSTGRES_PASSWORD, PRIMEFLOW_ADMIN_*, PRIMEFLOW_IMAGE
docker compose up -d
docker compose logs -f server
```

Compose refuses to start rather than defaulting a password, so a missing `.env`
fails immediately and loudly instead of coming up with a known credential.

Then open <http://localhost:8080> and log in with `PRIMEFLOW_ADMIN_EMAIL` /
`PRIMEFLOW_ADMIN_PASSWORD`. Check it from the shell first if you prefer:

```bash
curl -s localhost:8080/api/v1/health      # {"status":"ok",...}
```

**No registry?** `PRIMEFLOW_IMAGE` can name an image you built yourself. From a
checkout of the PrimeFlow repository:

```bash
make docker
```

which tags `detroitttttxp/primeflow:latest` locally — the default in
`.env.example`, so nothing else changes.

There is no seeding step. The bundled worker creates the lanes it serves and
registers its flows when it starts, so the console's **Flows** and **Work
Pools** pages have content on the first login. Create a deployment against one
of those flows, press **Run now**, and the run should reach a worker in well
under a second.

## 2. Put your own flows in it

Flow code never lives on the server. A worker is your binary: it imports two
packages and registers the flows it can execute. You do not fork PrimeFlow.

[`worker/`](worker/) is that binary, already written — its own `go.mod`, one
working flow, a Dockerfile whose build context is that directory alone. Copy it
out to a repository of your own:

```bash
cp -r worker ~/my-worker && cd ~/my-worker
go mod edit -module github.com/you/my-worker
$EDITOR greet.go                  # your flow goes here
docker build -t my-worker:dev .
```

Then uncomment the `my-worker` service at the bottom of
[`docker-compose.yml`](docker-compose.yml) and bring it up:

```bash
MY_WORKER_IMAGE=my-worker:dev docker compose up -d my-worker
```

Its flows appear under **Flows** as soon as it connects. Delete the stock
`worker` service once you no longer want the example flows.

Because the server holds no flow code, changing a flow rebuilds this image
alone — the server does not move. A worker that leases a run for a flow it has
not registered does not fail that run: it returns to the queue as
`AwaitingWorker` and is retried, which is what makes a partial rollout safe and
lets different pools run different flow sets.

Keep the SDK and the server image in step:

```bash
go get github.com/DetroittTxP/primeflow@v0.2.0 && go mod tidy
```

`go mod tidy` is not optional — `go get` records this module alone and writes no
`go.sum` entries for the indirect dependencies the SDK pulls in, so the build
stops with `missing go.sum entry`. Only tags cut after the module path was
renamed resolve; `v0.1.0` still declares the old `github.com/primex/primeflow`.

## 3. Start runs from your own application

The console and the CLI are not the only entry points. Anything that can make an
HTTP request starts a run through the External API — but it ships **off**, so
there are two steps, not one.

Enable it under **Settings → API Keys**, or from the shell:

```bash
curl -sS -X PUT localhost:8080/api/v1/settings/external-api \
  -H "Authorization: Bearer $PRIMEFLOW_API_TOKEN" \
  -H 'Content-Type: application/json' -d '{"enabled":true}'
```

Until then every external route answers `401 external API is disabled`, which is
the intended default: a fresh install exposes no unauthenticated surface beyond
`GET /api/v1/health` and `GET /metrics`.

Then issue a key. A key carries a **role**, not a list of scopes —
`api-trigger` is the one for an application that starts runs and reads them
back (`write:runs`, `read:runs`, `read:deployments`); `api-readonly` suits a BI
or audit pipeline, and `api-admin` can also write deployments and queues.

```bash
curl -sS -X POST localhost:8080/api/v1/api-keys \
  -H "Authorization: Bearer $PRIMEFLOW_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"my-app","role":"api-trigger"}'
```

The response's `secret` (a `pmx_…` string) is shown once. With it:

```bash
curl -X POST localhost:8080/api/external/v1/runs \
  -H "X-API-Key: $PRIMEFLOW_KEY" -H 'Content-Type: application/json' \
  -d '{"flow_name":"greet","work_queue":"default","parameters":{"name":"world"}}'
```

The reply carries the run id; `GET /api/external/v1/runs/{id}` returns its state
and result, and `/tasks`, `/logs` and `/artifacts` hang off the same path.

Machine clients that should act as an operator rather than as an application —
the `primeflow` CLI, your deployment scripts — use `PRIMEFLOW_API_TOKEN` as a
bearer token against `/api/v1/*` instead, and need none of the above:

```bash
export PRIMEFLOW_API_URL=http://localhost:8080
export PRIMEFLOW_API_TOKEN=...        # the value from your .env
primeflow runs
```

## 4. Operating it

**Upgrading** is a new tag and a restart. Migrations are additive and
idempotent, and the `migrate` service runs before the server and the workers:

```bash
$EDITOR .env                          # PRIMEFLOW_IMAGE=...:<new tag>
docker compose pull && docker compose up -d
```

**Backups.** Postgres holds everything — runs, checkpoints, users, keys,
settings. Nothing in a worker container is worth preserving.

```bash
docker compose exec -T postgres pg_dump -U primeflow primeflow | gzip > pf-$(date +%F).sql.gz
```

**Restarting a worker** is safe at any point, including mid-run. The lease
lapses, the janitor marks the run crashed, and it resumes from its last
checkpoint on whichever worker leases it next.

**Rotating a credential in `.env`** needs `docker compose up -d
--force-recreate`. A plain `restart` reuses the container and keeps the old
value: Compose reads the environment when a container is created, not when it
starts.

**Exposing it.** This stack publishes plain HTTP on `PRIMEFLOW_HTTP_PORT`,
which is right for localhost and wrong for anything else. Put TLS in front
before it leaves the machine — [`deploy/vm-server/`](../deploy/vm-server/) in
the PrimeFlow repository is this same stack with an nginx edge holding 443, and
it also sets `PRIMEFLOW_TRUSTED_PROXY_CIDRS`, without which the server neither
believes `X-Forwarded-For` nor marks session cookies `Secure`.

**Workers somewhere else.** A worker in this file reaches the orchestrator with
a database DSN, which is fine on one box. A worker on another machine should not
hold a database credential: give it `PRIMEFLOW_API_URL` and a pool-scoped
`PRIMEFLOW_WORKER_TOKEN` instead and it works over outbound 443 alone. See
[`deploy/vm-worker/`](../deploy/vm-worker/) and
[`deploy/README.md`](../deploy/README.md).

## Where things are documented

| Question | File |
|---|---|
| Writing flows: tasks, retries, sleeps, sub-flows | [`../README.md`](../README.md) |
| Why the deployment is shaped this way | [`../docs/DEPLOYMENT.md`](../docs/DEPLOYMENT.md) |
| Two VMs, TLS, pool keys | [`../deploy/README.md`](../deploy/README.md) |
| Kubernetes | [`../deploy/k8s/`](../deploy/k8s/) |
| Roles and API permissions | [`../docs/api_roles_and_permissions.md`](../docs/api_roles_and_permissions.md) |
