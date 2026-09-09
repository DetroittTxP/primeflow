# PrimeFlow architecture

Design notes: what each decision buys, and what it costs.

---

## 1. Why not just deploy Prefect

Prefect's open-source core is good, and PrimeFlow copies its shape deliberately.
The reasons to rebuild it rather than run it:

- **PrimeX is Go.** A Python orchestrator means a second runtime, a second
  dependency tree, and a second set of vulnerability scans in the image.
  PrimeFlow ships as one static binary.
- **The vCD client already exists in Go.** Flows that provision VMs want to call
  the code PrimeX already has, not re-implement it across a language boundary.
- **Operator queue control.** Prefect has work pools and concurrency limits but
  no per-run priority band and no "run this one next". For a CMP, where a
  customer-facing provisioning request must jump ahead of a nightly metering
  sweep, that control is the product.
- **Footprint.** Postgres, optionally Redis, and two Go processes. No broker to
  operate.

What is given up: Prefect's ecosystem, its Python integrations, and its Cloud
offering. If flows would mostly be data-science glue, Prefect is the better
choice. For CMP orchestration in a Go codebase, it is not.

---

## 2. The durability model

Three models were available.

**Deterministic replay (Temporal, Cadence).** Every step is recorded in an event
history and replayed instruction-for-instruction on recovery. Extremely strong,
but it makes the workflow function a restricted dialect: no `time.Now()`, no
maps with unordered iteration, no ungoverned goroutines, no library that does
any of those things. Every author has to learn the restrictions, and a violation
manifests as a mysterious non-determinism error in production.

**Checkpoint-and-replay (Prefect, and PrimeFlow).** The function is ordinary
code. Each `Task` persists its result under a deterministic key. On resume the
function runs again from the top, and completed tasks return their stored
results instead of executing. One rule replaces the whole dialect:

> Side effects go inside a `Task`. Code between tasks may run more than once.

**Explicit state machines.** Maximum control, but every workflow becomes a
hand-written state machine, which is exactly the work an orchestrator is
supposed to remove.

The middle option was chosen because the authoring rule is small enough to
state in one sentence and the failure mode of breaking it — a duplicated side
effect — is easier to reason about than a non-determinism panic.

### Checkpoint keys

The key defaults to `<task name>-<n>` where `n` counts how many times a task of
that name has been *reached* in this run. Reach order is stable across replays
as long as control flow is stable, which it is for the overwhelming majority of
flows.

It is not stable when the data driving a loop changes between attempts. That is
why `sdk.TaskKey` exists, and why the example metering flow keys on
`"collect:" + org + ":" + day` rather than on position: adding an org to the
list must not shift every subsequent key and re-run work that already succeeded.

### Durable waits

`sdk.Sleep` over the suspend threshold (30s by default) does not block. The task
persists the wake instant, then raises `SuspendError`. The engine catches it and
returns the run to `SCHEDULED` with `scheduled_at` set to the wake time, so a
flow can wait six hours for a VM to build without holding a worker slot. On
resume the wake instant is replayed from the checkpoint rather than restarted,
so a run that suspends repeatedly still wakes when it originally intended to.

---

## 3. Postgres as the queue

Queue order lives in the same transaction as run state. Dispatch is one
statement per lane:

```sql
WITH cap AS (SELECT paused, concurrency_limit,
                    (SELECT count(*) FROM pf_flow_runs
                      WHERE work_queue = $2 AND state IN ('RUNNING','PENDING')) AS active
               FROM pf_work_queues WHERE name = $2),
cand AS (SELECT r.id FROM pf_flow_runs r
          WHERE r.work_queue = $2 AND r.state = 'SCHEDULED'
            AND r.scheduled_at <= now() AND NOT r.cancel_requested
            AND NOT (SELECT paused FROM cap)
          ORDER BY r.queue_position ASC NULLS LAST,   -- operator pins first
                   r.priority DESC,                    -- then priority band
                   r.scheduled_at ASC                  -- then FIFO
          LIMIT (…remaining capacity…)
            FOR UPDATE OF r SKIP LOCKED)
UPDATE pf_flow_runs SET state='PENDING', worker_id=$1,
       lease_expires_at=now()+$4::interval, run_count=run_count+1
  FROM cand WHERE pf_flow_runs.id = cand.id
RETURNING …;
```

`SKIP LOCKED` is what makes this work: concurrent workers never block each other
and never claim the same run. The partial index
`(work_queue, queue_position NULLS LAST, priority DESC, scheduled_at) WHERE
state='SCHEDULED'` keeps it an index scan over waiting work only.

**Capped lanes are the exception.** `active` is counted in the same statement
that admits, and under `READ COMMITTED` each statement gets its own snapshot, so
two dispatchers racing both count the same total and both admit up to the full
headroom — a lane capped at two admits two *per worker*. The fix is one
transaction-scoped advisory lock on the lane, taken in an earlier statement of
the same transaction, so the second dispatcher counts under a snapshot that
already contains the first one's `PENDING` rows. Only lanes that set
`concurrency_limit` pay for it; everything else keeps the lock-free path.

**Why not Redis as the queue.** A Redis-backed queue means two systems that can
disagree: a run marked `RUNNING` in Postgres with no matching entry in Redis,
or a Redis entry for a run that was cancelled. Reconciling them is the kind of
bug that shows up at 3am. Keeping order in the same transaction as state removes
the failure mode entirely.

**Why Redis at all.** Without it, latency between "work is ready" and "a worker
notices" is the poll interval. Redis pub/sub carries three hints — new work on a
queue, cancel this run, and the UI's live event feed — none of which affect
correctness. Lose Redis and everything converges on the next poll. No Redis
configured at all and an in-process bus is used, which is why the whole system
can run as one binary plus Postgres.

**Cost.** Every worker polls every lane it watches on its interval. Hundreds of
workers on a two-second poll is real load. The mitigations are the Redis wake-up
(which lets the poll interval be long) and Postgres `LISTEN/NOTIFY` as a future
step for deployments that would rather not run Redis.

---

## 4. Leases, not locks

A worker claims a run by setting `worker_id` and `lease_expires_at`, then
extends it with heartbeats. Three things follow:

- A worker that dies loses its runs after one lease period. The janitor marks
  them `CRASHED` and reschedules them, and they resume from their checkpoints.
- A worker partitioned from Postgres fails its next renewal, learns it no longer
  owns the run, and cancels its local execution — so a network split does not
  produce two workers running the same flow.
- The lease period is the only tuning knob: shorter means faster recovery,
  longer means more tolerance for a slow database.

`CRASHED` is deliberately not terminal. A crash is not the flow's fault, so it
gets one attempt beyond the configured retry budget before it is called failed.

### Why workers cannot deadlock each other

- **Dispatch** is a `SELECT … FOR UPDATE SKIP LOCKED` (see §3). The row locks
  are acquired and released inside that one statement, and `SKIP LOCKED` means
  they never wait, so there is no circular wait between workers.
- **A capped lane** takes one transaction-scoped advisory lock before that
  statement, keyed on the lane name. Lock ordering is total and cannot cycle:
  the advisory lock is always first, no transaction holds two of them (each lane
  in a worker's list gets its own transaction, committed before the next opens),
  and everything locked afterwards is `SKIP LOCKED`. Uncapped lanes take no
  advisory lock, so the common path is unchanged.
- **Lease renewal, heartbeats, state writes** are each single-row `UPDATE`s,
  independent per run.
- **Leader election** (`pf_leader`) is one conditional `INSERT … ON CONFLICT`;
  no lock is held between calls.
- The only structure that could form a cycle is **sub-flow recursion**
  (`RunDeployment` from inside a flow). Before a child is created the engine
  walks `parent_run_id` upward: it refuses if the chain is already
  `PRIMEFLOW_MAX_SUBFLOW_DEPTH` deep, or if the target deployment is already an
  ancestor. So the run graph is a bounded DAG by construction.

---

## 4a. Sub-flows

`RunDeploymentAndWait` is built entirely from primitives that already exist:

1. A **task checkpoint** stores the child run id. The trigger carries an
   idempotency key `child:<parentRunID>:<taskKey>`, so a replaying parent gets
   the *same* child back instead of spawning a second one.
2. The parent then **suspends** (`SCHEDULED`, lease released) until the child
   settles. It is woken two ways: the engine, on writing any run to a terminal
   state, checks `parent_run_id` and — if no sibling is still unfinished —
   reschedules the parent; failing that, the parent's own 30-second suspend
   re-poll is the backstop.
3. On resume the parent replays from the top, the checkpoint yields the child
   id, `GetRunState` reads its outcome, and execution continues. A `FAILED` or
   `CANCELLED` child returns a permanent error so the parent does not burn its
   own retry budget on it.

There is a small race — a child that finishes in the window between the parent's
`CountUnfinishedChildren` check and its `Suspend` — but it is self-healing: the
child's reschedule of a still-`RUNNING` parent is a no-op, and the parent's next
replay re-checks. Worst case the parent waits one 30-second re-poll.

The child run also carries the parent's W3C `traceparent`, so `flow_run` spans
nest across the process boundary.

---

## 5. State transitions

Nine states with an enforced transition table
([`internal/core/state.go`](../internal/core/state.go)). Every write goes
through `SetFlowRunState`, which checks the table and then applies the update
conditionally on the state it read — so two workers racing to finish the same
run cannot both win.

`Force` bypasses the table and is reserved for the janitor, which has to be able
to settle a run whose worker vanished mid-transition. The engine will also fall
back to `Force` rather than leave a run stuck in `RUNNING` with no worker: a run
in that state is the one failure operators cannot diagnose from the console.

---

## 6. Scaling

**Workers** scale by adding replicas. `SKIP LOCKED` means no coordination is
needed. One Deployment per queue is the recommended shape: a slow lane scales
without touching the others, and pausing a lane drains it without a rollout.

The scaling *signal* is published, not acted on. A work queue carries
`min_workers`, `max_workers` and `target_ready_per_worker`; the server exposes
`primeflow_queue_desired_workers{queue} = clamp(ceil(ready/target), min, max)` at
`/metrics`, and a KEDA `ScaledObject` or an HPA external-metric scales the worker
Deployment to it. PrimeFlow never calls the Kubernetes API — keeping the binary
free of cluster credentials and portable to a plain `docker compose` or a
systemd unit. External teams run their own worker Deployment against their own
pool name; the `owner` field is how the console attributes it.

**Per-run isolation is a deployment switch, not a second architecture.**
`PRIMEFLOW_EXEC_MODE=process` makes a worker start a child process of its own
binary for each run it leases, instead of a goroutine; `=kubernetes` makes it a
Job instead of a process. Everything the
orchestrator sees is unchanged: the parent leases through the same statement, so
capped lanes still count correctly; the parent's heartbeat renews the lease and
carries cancellations, which it delivers to the child as a SIGTERM — the signal
the child turns into a cancelled run context, the same thing `Engine.Cancel`
does in-process. A child that dies without settling its run is the interesting
case: the parent expires the lease itself (`RenewLease` for zero) rather than
waiting out the period, which hands the run to the janitor's crash path a lease
sooner. That path is unchanged too, retry budget included.

What it buys is a blast radius of one run: a panic that escapes a flow, a
goroutine that outlives it, a leaked file handle, an OOM kill. What it costs is
a process start per run (a few milliseconds for a Go binary, but the flow's own
start-up on top), and the worker's `/metrics` listener no longer sees the
flow and task timings, because they now happen in a process that exits.
`sdk.Sleep` and `RunDeploymentAndWait` are unaffected in the sense that matters
— a wait beyond `SuspendThreshold` still gives up the process — but under this
mode giving it up means the child exits and a later one replays, so a run that
waits a long time in many short suspensions pays a start each time.

**The Kubernetes launcher is that same worker with a different launcher.** It
leases, heartbeats, renews and drains identically; `internal/kube` creates a Job
per leased run and blocks until it settles, so a worker slot is a pod and
`PRIMEFLOW_CONCURRENCY` is how many pods may exist at once. Two properties are
worth naming:

- **PrimeFlow still owns retries.** Every Job is rendered with `backoffLimit: 0`
  and `restartPolicy: Never`. A Job that restarted its own pod would re-enter
  the flow under a lease already counted as one attempt, which is the one way to
  get a run executed twice.
- **The pod renews the lease itself** (`PRIMEFLOW_LEASE_RENEW`), unlike a
  process-mode child, whose parent lives exactly as long as it does. A pod
  outlives a rolled or evicted launcher, and a run whose lease lapsed under a
  live pod would be crashed by the janitor and leased again — the same flow
  twice. Renewing from inside the pod closes that, and the renewal answer
  carries cancellations, so an operator's cancel reaches a pod whose launcher is
  gone.

A Job that never starts — an image that will not pull, a pod nothing will
schedule — is neither running nor failed, and would hold its lease for as long
as the cluster kept trying. `PRIMEFLOW_KUBE_START_DEADLINE` is the bound: the
launcher watches for the run to leave PENDING, and past the deadline deletes the
Job and lets the run take the crash path.

The server is still free of cluster credentials. The launcher holds them, runs
inside the cluster it launches into, and its Role grants `create`, `get`,
`list` and `delete` on Jobs in one namespace — see `deploy/k8s/job-launcher.yaml`. The run
pods get a service account with no permissions and no token mounted at all.

**Push pools** invert the flow for scale-to-zero. A pool with `pool_type='push'`
has no leased workers; the leader's dispatch loop `POST`s each ready run to the
pool's `push_endpoint` with an HMAC (`X-PrimeFlow-Signature`) over the body, and
holds the run with a 2-minute `worker_id='push-dispatch'` lease. The receiver —
your binary run as `RunPushWorker` — verifies the signature, `ClaimPushRun`
takes that one run `SCHEDULED → PENDING` under its own lease, and the engine
drives it to `RUNNING` and executes it exactly as a leased run — same state
writes, same events. If the receiver never claims, the hold lapses, the run
is re-dispatched, and eventually the janitor reclaims it as `CRASHED` — the same
recovery path as an abandoned pull run. No new failure mode.

### GitOps worker delivery

PrimeFlow does not launch workers, but it will write their manifests. A
`pf_worker_specs` row is the inputs the Add-worker wizard collects (name, image,
pools, concurrency, replicas, delivery kind, namespace). `internal/gitsync`
renders that row to Kubernetes YAML — the same Go code the console preview, the
"Sync now" button and the reconciler all call, so there is exactly one renderer.

The git engine (`internal/gitsync/git.go`, pure-Go [go-git]) clones the repo
from `Settings → Git connection` **into memory** (credentials and manifests
never touch local disk), writes the rendered files under the spec's path,
commits with the configured author, and pushes **straight to the branch** — no
pull request. A brand-new repo with no commits is bootstrapped in place. If the
worktree is unchanged after writing, the sync is a no-op that still reports the
current HEAD.

`auto_sync` specs are driven by a leader-elected reconciler (role `gitsync`,
`PRIMEFLOW_GITSYNC_INTERVAL`, default 2m): each tick it re-renders every
auto-sync spec, compares the tree hash to `last_synced_hash`, and pushes the
ones that drifted. A failed push records `sync_state='error'` and `last_error`
and is retried next tick — it never blocks the server. The distroless image is
unchanged: go-git is pure Go, so there is still no `git` binary or shell in the
runtime.

[go-git]: https://github.com/go-git/go-git

### Transports

The wake-up bus (`internal/bus`) has three interchangeable implementations,
selected NATS → Redis → in-process. It is strictly an accelerator: every message
is a hint ("new work on queue X", "cancel run Y"), correctness never depends on
delivery, and a consumer that misses one converges on its next poll. A dial
failure at start-up degrades to polling with a warning rather than refusing to
run.

**The server** scales horizontally for API traffic. The scheduler and automation
evaluator must not run N times, so both are leader-elected through a lease row
in `pf_leader`. Schedule materialisation is *additionally* idempotent on
`(deployment, instant)` via a unique key — belt and braces, because a lost
election during a network blip should cost nothing.

**The automation evaluator** reads the event log forward from a `BIGSERIAL`
cursor rather than subscribing to the bus, so a restart or a dropped message
costs nothing: it resumes exactly where it stopped. On first start it jumps to
the head of the log, because a newly deployed evaluator should react to what
happens next, not replay a month of history as if it were live.

---

## 6a. Workers that never see the database

A worker beside the database opens its own connection, which is simple and
right — until the worker is at another site. Then that VM holds a credential
that reads `pf_users` and `pf_api_keys`, can modify another site's runs, and
needs 5432 across a WAN. Revoking it means rotating the password everywhere.

`store.WorkerStore` names the eighteen methods the execution path actually uses,
and `internal/store/remote` implements them over `/api/v1/worker/*`. The trust
boundary moves from the database to the API, which is where Prefect has always
had it.

The handlers are thin on purpose: every rule already lives in the store, and a
handler that restated one would be a second copy to keep in step. Three things
are never taken from a request body — the caller's worker id, which comes from
the credential; `StateOpts.Force`, refused as the janitor's; and the instant a
cache lookup is measured against, so a skewed site clock cannot extend a cache
entry. `AppendEvent` is absent by design: automations act on the event log, so a
site able to write it could forge a failure against another site's flow and trip
an automation against a lane it has no other authority over. The server emits on
the worker's behalf, and attributes the caller while it is there — which also
fixed the older gap where a settled run had cleared `worker_id` and nothing
recorded who ran it.

**Authority is the key, not the id.** An `api-worker` key names the pools it may
touch. Lease requests are intersected with that list; run-addressed routes check
the run's lane, because a route addressed by id never mentions a queue and a
lease-only guard would miss it. The External API's existing controls apply
unchanged — IP allow-list, mutual TLS, per-key rate limit, audit trail.

**Wake-ups ride the same connection.** A site cannot reach NATS or Redis, so
`GET /api/v1/worker/stream` carries work and control notices over SSE, filtered
to the credential's pools. The bus contract is unchanged: every message is a
hint, nothing is replayed on reconnect, and a worker that receives none still
runs every flow at its poll interval. That interval defaults to 15s remotely
rather than 2s — the stream carries the latency, so the poll is a backstop.

**One conversation per interval.** Liveness, lease renewal and cancellation are
one call. Renewal used to be one call per held run and cancellation one read per
running run, which made a worker's request rate scale with how busy it was —
backwards for a link shared by every site.

**Cost.** Every `sdk.Task` checkpoint is now a round trip. Beside the database
that is a millisecond; across a WAN it is the link's latency times the number of
tasks, so a flow with many small tasks pays for them. Measure before moving a
chatty flow. Batching checkpoint writes would weaken the durability guarantee,
so it is a decision rather than an optimisation.

---

## 7. What will need attention first

- **`pf_logs` growth.** `pf_logs` is range-partitioned by month (migration 0004;
  legacy rows attach as a bounded `pf_logs_p0`). The janitor pre-creates the next
  two months and `DROP`s whole partitions once their entire range is past
  `PRIMEFLOW_LOG_RETENTION` — O(1), no vacuum churn — then a small batched delete
  trims the sub-partition tail. Converting an already-huge `pf_logs` does one
  validation scan on the ATTACH; do it in a maintenance window.
- **Poll amplification.** Fine to a few dozen workers per lane. Beyond that,
  `LISTEN/NOTIFY` or a longer poll interval leaning harder on Redis.
- **Result size.** Task results are stored as `jsonb` inline. Large payloads
  belong in object storage with a reference in the checkpoint.
- **Clock skew.** Schedules and leases use database time (`now()`), not worker
  time, so worker clock drift is already harmless. Timezone handling relies on
  the embedded `tzdata`, so containers need no tzdata package.
