# myworker — a worker image carrying your own flows

The published `primeflow` image's worker is built from `examples/primex-worker`,
so it can only execute the flows that package registers. This directory is the
starting point for your own: a separate Go module that imports two packages and
registers two flows.

```
myworker/
  go.mod          module of its own — this is not a fork of primeflow
  main.go         flow registration + worker.Run
  healthcheck.go  http-healthcheck: probe URLs, fail if any is unhealthy
  ping.go         ping: report which worker and pool ran it
  Dockerfile      build context is the PARENT directory (see below)
```

## Build

The build context is the repository root, not this directory, because `go.mod`
carries `replace github.com/DetroittTxP/primeflow => ..`:

```bash
docker buildx build --platform linux/amd64 -f myworker/Dockerfile -t <registry>/myworker:$(git describe --tags --always) --push .
```

`--platform` is not optional. `TARGETARCH` defaults to `amd64`, so a plain
build on an arm64 machine produces an amd64 binary inside an arm64-*labelled*
image.

## Why the replace directive

The module path and the repository now agree, so the dependency resolves on its
own — `go get github.com/DetroittTxP/primeflow@v0.2.0` — as soon as a tag
carrying the renamed path is pushed. The replace is kept here because it is the
useful default while developing a flow and the engine together: it builds
against the working tree rather than the last release.

Drop it whenever you would rather pin a release:

```bash
go mod edit -dropreplace github.com/DetroittTxP/primeflow
go get github.com/DetroittTxP/primeflow@v0.2.0
```

That also narrows the Docker build context to `myworker/` alone — see the note
in the [Dockerfile](Dockerfile). If instead you move this directory out of the
primeflow tree and keep the replace, adjust `..` to point at your checkout.

The repository is private, so a machine resolving it from a tag needs to be
told to skip the module proxy and to fetch over SSH — once, per machine and in
CI:

```bash
go env -w GOPRIVATE=github.com/DetroittTxP/*
git config --global url."git@github.com:".insteadOf "https://github.com/"
```

## Run it

Two ways to reach the orchestrator, same as the stock worker:

```bash
# remote: a worker VM with nothing but outbound 443
PRIMEFLOW_API_URL=https://mainvm.example.com PRIMEFLOW_WORKER_TOKEN=<pool-scoped key> PRIMEFLOW_QUEUES=vm2 ./myworker
```

```bash
# beside the database
PRIMEFLOW_DATABASE_URL=postgres://primeflow:primeflow@localhost:5432/primeflow?sslmode=disable PRIMEFLOW_QUEUES=default ./myworker
```

`worker.Options{}` is left zero on purpose: it reads `PRIMEFLOW_QUEUES`,
`PRIMEFLOW_CONCURRENCY`, `PRIMEFLOW_WORKER_NAME` and the rest from the
environment, which is what the compose and Kubernetes manifests already set.
Swap the image in [`deploy/vm-worker/docker-compose.yml`](../deploy/vm-worker/docker-compose.yml)
and drop its `entrypoint:` line — this image's entrypoint is already the worker.

Deploy `ping` first on a new pool. It calls nothing external, so if it does not
start the problem is the pool, the key or the queue name, never the flow.

## Two things the flows demonstrate

**Artifacts before errors.** `http-healthcheck` writes its table and summary
*before* returning the failure. A run that ends failed drops its `result`, so
anything left there is lost exactly when you want to read it; artifacts survive.

**Checkpoints keyed by identity, not position.** Each probe is
`sdk.TaskKey("probe:"+url)` rather than the default positional key, so adding a
target next month replays the ones already recorded instead of shifting every
checkpoint after the insertion point.

## Changing flows

Rebuild and redeploy this image only. The server holds no flow code and does
not move. A worker that leases a run for a flow it does not have registered
does not fail it — the run returns to the queue as `AwaitingWorker` and retries
in 30 seconds, so a partial rollout is not an outage and different pools can
safely run different flow sets.
