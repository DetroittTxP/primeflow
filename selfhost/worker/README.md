# Your worker

A PrimeFlow worker is your own binary. It imports two packages — `pkg/sdk` to
define flows, `pkg/primeflow/worker` to run them — and the flows registered in
`main()` are the only ones it can execute. This is not a fork of PrimeFlow and
it does not need a checkout of that repository to build: the dependency comes
through the Go module proxy like any other.

```
go.mod      example.com/my-worker — rename it
main.go     flow registration + worker.Run
greet.go    the starter flow
Dockerfile  build context is this directory alone
```

## Make it yours

From the bundle root (the directory above this one):

```bash
cp -r worker ~/my-worker && cd ~/my-worker
go mod edit -module github.com/you/my-worker
$EDITOR greet.go
docker build -t my-worker:dev .
```

Then point the `my-worker` service in [`../docker-compose.yml`](../docker-compose.yml)
at the image. Its flows show up under **Flows** in the console as soon as it
connects — a worker registers its catalogue and creates the lanes it serves, so
there is no separate registration step.

## What `greet` is showing you

It is four lines of logic wrapped in the three moves every flow makes.

**Typed parameters.** `sdk.Params[GreetParams](c)` decodes what the console's
Run dialog or an External API caller sent. `sdk.ParamsSchema` in `main.go` takes
a *populated* example, not a zero value, because the non-zero fields become the
placeholders the console renders.

**Checkpointed tasks.** Each greeting is an `sdk.Task`, so a worker that dies
halfway resumes at the one it had not reached rather than starting over. The
explicit `sdk.TaskKey("greet:0")` matters: the default key is positional, so
changing the shape of a loop later would shift every checkpoint after the
change. Keyed by identity, a resumed run replays what it already recorded.

**Artifacts before the return.** A run that ends failed drops its `result`, so
anything left there is lost exactly when you most want to read it. `c.Markdown`
writes to the run's artifacts, which survive a failure — write them *before*
returning an error, not after.

Two more things worth knowing early:

- `sdk.Permanent(err)` marks input that will never become valid. It skips the
  retry budget instead of burning it re-running the same bad request.
- A worker that leases a run for a flow it has not registered does not fail it.
  The run goes back to the queue as `AwaitingWorker` and is retried, so a
  partial rollout is not an outage and different pools can run different flows.

## Keeping the SDK and the server in step

```bash
go get github.com/DetroittTxP/primeflow@v0.2.0 && go mod tidy
```

`go mod tidy` is not optional: `go get` records this module alone and writes no
`go.sum` entries for the indirect dependencies the SDK pulls in, so the build
stops with `missing go.sum entry`. Only tags cut after the module path was
renamed resolve — `v0.1.0` still declares the old `github.com/primex/primeflow`.

## Running it somewhere else

The compose file gives this worker a database DSN, which is right on one box.
A worker on another machine should not hold a database credential — give it the
worker API instead and outbound 443 is the only firewall rule it needs:

```bash
PRIMEFLOW_API_URL=https://primeflow.example.com \
PRIMEFLOW_WORKER_TOKEN=pmx_...   \
PRIMEFLOW_QUEUES=site-b ./worker
```

The token is a pool-scoped `api-worker` key, issued in the console under
**Workers → Add worker… → Issue key…**. Its pool list is a boundary the server
enforces: a lease for another pool comes back empty and a run in another pool is
refused with 403.

`worker.Options{}` is left zero on purpose — it reads `PRIMEFLOW_QUEUES`,
`PRIMEFLOW_CONCURRENCY`, `PRIMEFLOW_WORKER_NAME`, `PRIMEFLOW_LEASE` and the rest
from the environment, which is what the compose and Kubernetes manifests set.
