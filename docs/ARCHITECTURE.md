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

- **Dispatch** is a single `SELECT … FOR UPDATE SKIP LOCKED` (see §3). The row
  lock is acquired and released inside that one statement. There is no second
  row locked in the same transaction, so there is no lock-ordering problem and
  no circular wait between workers.
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

## 7. What will need attention first

- **`pf_logs` growth.** Now bounded by a leader-run age-based delete
  (`PRIMEFLOW_LOG_RETENTION`, default 720h, batched 5k rows at a time). Native
  daily partitioning is the next step for very high volume; the delete job is
  what ships.
- **Poll amplification.** Fine to a few dozen workers per lane. Beyond that,
  `LISTEN/NOTIFY` or a longer poll interval leaning harder on Redis.
- **Result size.** Task results are stored as `jsonb` inline. Large payloads
  belong in object storage with a reference in the checkpoint.
- **Clock skew.** Schedules and leases use database time (`now()`), not worker
  time, so worker clock drift is already harmless. Timezone handling relies on
  the embedded `tzdata`, so containers need no tzdata package.
