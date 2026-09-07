-- PrimeFlow initial schema.
-- Postgres is the single source of truth for state and for queue ordering.
-- Redis (or NATS) is used only for low-latency wake-ups and fan-out; losing it
-- degrades latency, never correctness.

CREATE TABLE IF NOT EXISTS pf_flows (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    version     TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    tags        TEXT[] NOT NULL DEFAULT '{}',
    labels      JSONB  NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE TABLE IF NOT EXISTS pf_work_queues (
    name              TEXT PRIMARY KEY,
    description       TEXT NOT NULL DEFAULT '',
    concurrency_limit INTEGER,
    paused            BOOLEAN NOT NULL DEFAULT false,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO pf_work_queues (name, description)
VALUES ('default', 'Default work queue')
ON CONFLICT (name) DO NOTHING;

CREATE TABLE IF NOT EXISTS pf_deployments (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    flow_name     TEXT NOT NULL,
    description   TEXT NOT NULL DEFAULT '',
    parameters    JSONB,
    work_queue    TEXT NOT NULL DEFAULT 'default' REFERENCES pf_work_queues(name) ON UPDATE CASCADE,
    priority      INTEGER NOT NULL DEFAULT 50,
    schedule_kind TEXT NOT NULL DEFAULT '',
    schedule      TEXT NOT NULL DEFAULT '',
    timezone      TEXT NOT NULL DEFAULT 'UTC',
    paused        BOOLEAN NOT NULL DEFAULT false,
    tags          TEXT[] NOT NULL DEFAULT '{}',
    retries       INTEGER NOT NULL DEFAULT 0,
    retry_delay_ms BIGINT NOT NULL DEFAULT 0,
    timeout_ms    BIGINT NOT NULL DEFAULT 0,
    catchup       BOOLEAN NOT NULL DEFAULT false,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pf_deployments_schedule_idx
    ON pf_deployments (paused, schedule_kind)
    WHERE schedule_kind <> '';

CREATE TABLE IF NOT EXISTS pf_flow_runs (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    flow_name      TEXT NOT NULL,
    deployment_id  TEXT REFERENCES pf_deployments(id) ON DELETE SET NULL,
    parameters     JSONB,
    work_queue     TEXT NOT NULL DEFAULT 'default',

    state          TEXT NOT NULL,
    state_name     TEXT NOT NULL DEFAULT '',
    state_message  TEXT NOT NULL DEFAULT '',

    -- Dispatch controls. queue_position is NULL for ordinary runs and set to a
    -- small (possibly negative) integer when an operator pins a run to the
    -- front of its queue.
    priority       INTEGER NOT NULL DEFAULT 50,
    queue_position INTEGER,

    scheduled_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at     TIMESTAMPTZ,
    ended_at       TIMESTAMPTZ,

    run_count      INTEGER NOT NULL DEFAULT 0,
    retries        INTEGER NOT NULL DEFAULT 0,
    retry_delay_ms BIGINT  NOT NULL DEFAULT 0,
    timeout_ms     BIGINT  NOT NULL DEFAULT 0,

    worker_id        TEXT,
    lease_expires_at TIMESTAMPTZ,

    result          JSONB,
    tags            TEXT[] NOT NULL DEFAULT '{}',
    cancel_requested BOOLEAN NOT NULL DEFAULT false,

    -- Idempotency key for scheduler materialisation: one run per deployment
    -- per scheduled instant.
    idempotency_key TEXT UNIQUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The dispatch index. Ordering is: manually pinned runs first (ascending
-- position), then by descending priority, then oldest scheduled first.
CREATE INDEX IF NOT EXISTS pf_flow_runs_dispatch_idx
    ON pf_flow_runs (work_queue, queue_position NULLS LAST, priority DESC, scheduled_at ASC)
    WHERE state = 'SCHEDULED';

CREATE INDEX IF NOT EXISTS pf_flow_runs_state_idx     ON pf_flow_runs (state, updated_at DESC);
CREATE INDEX IF NOT EXISTS pf_flow_runs_lease_idx     ON pf_flow_runs (lease_expires_at) WHERE state IN ('RUNNING','PENDING');
CREATE INDEX IF NOT EXISTS pf_flow_runs_queue_idx     ON pf_flow_runs (work_queue, state);
CREATE INDEX IF NOT EXISTS pf_flow_runs_deployment_idx ON pf_flow_runs (deployment_id, scheduled_at DESC);
CREATE INDEX IF NOT EXISTS pf_flow_runs_created_idx    ON pf_flow_runs (created_at DESC);

CREATE TABLE IF NOT EXISTS pf_task_runs (
    id            TEXT PRIMARY KEY,
    flow_run_id   TEXT NOT NULL REFERENCES pf_flow_runs(id) ON DELETE CASCADE,
    task_key      TEXT NOT NULL,
    task_name     TEXT NOT NULL,
    state         TEXT NOT NULL,
    state_name    TEXT NOT NULL DEFAULT '',
    state_message TEXT NOT NULL DEFAULT '',
    result        JSONB,
    run_count     INTEGER NOT NULL DEFAULT 0,
    retries       INTEGER NOT NULL DEFAULT 0,
    cache_key     TEXT,
    cache_expires_at TIMESTAMPTZ,
    started_at    TIMESTAMPTZ,
    ended_at      TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- One checkpoint per deterministic key per flow run. This uniqueness is
    -- what makes replay-after-crash safe.
    UNIQUE (flow_run_id, task_key)
);

CREATE INDEX IF NOT EXISTS pf_task_runs_flow_idx  ON pf_task_runs (flow_run_id, created_at);
CREATE INDEX IF NOT EXISTS pf_task_runs_cache_idx ON pf_task_runs (cache_key, cache_expires_at)
    WHERE cache_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS pf_logs (
    id          BIGSERIAL PRIMARY KEY,
    flow_run_id TEXT NOT NULL REFERENCES pf_flow_runs(id) ON DELETE CASCADE,
    task_run_id TEXT,
    level       TEXT NOT NULL DEFAULT 'INFO',
    message     TEXT NOT NULL,
    fields      JSONB,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pf_logs_run_idx ON pf_logs (flow_run_id, id);

CREATE TABLE IF NOT EXISTS pf_artifacts (
    id          TEXT PRIMARY KEY,
    flow_run_id TEXT REFERENCES pf_flow_runs(id) ON DELETE CASCADE,
    task_run_id TEXT,
    key         TEXT NOT NULL,
    kind        TEXT NOT NULL DEFAULT 'markdown',
    description TEXT NOT NULL DEFAULT '',
    data        JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pf_artifacts_run_idx ON pf_artifacts (flow_run_id, created_at);
CREATE INDEX IF NOT EXISTS pf_artifacts_key_idx ON pf_artifacts (key, created_at DESC);

CREATE TABLE IF NOT EXISTS pf_events (
    id            TEXT PRIMARY KEY,
    -- seq gives automations a cheap, gap-tolerant cursor to read from.
    seq           BIGSERIAL,
    event         TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    payload       JSONB,
    occurred      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS pf_events_seq_idx  ON pf_events (seq);
CREATE INDEX IF NOT EXISTS pf_events_occurred_idx ON pf_events (occurred DESC);
CREATE INDEX IF NOT EXISTS pf_events_name_idx     ON pf_events (event, occurred DESC);
CREATE INDEX IF NOT EXISTS pf_events_resource_idx ON pf_events (resource_type, resource_id, occurred DESC);

CREATE TABLE IF NOT EXISTS pf_automations (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    description      TEXT NOT NULL DEFAULT '',
    enabled          BOOLEAN NOT NULL DEFAULT true,
    match_event      TEXT NOT NULL DEFAULT '',
    match_deployment TEXT NOT NULL DEFAULT '',
    match_flow       TEXT NOT NULL DEFAULT '',
    match_work_queue TEXT NOT NULL DEFAULT '',
    match_tag        TEXT NOT NULL DEFAULT '',
    threshold        INTEGER NOT NULL DEFAULT 1,
    window_ms        BIGINT  NOT NULL DEFAULT 0,
    action           TEXT NOT NULL,
    action_config    JSONB,
    last_fired_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pf_workers (
    id             TEXT PRIMARY KEY,
    name           TEXT NOT NULL,
    queues         TEXT[] NOT NULL DEFAULT '{}',
    concurrency    INTEGER NOT NULL DEFAULT 1,
    active_runs    INTEGER NOT NULL DEFAULT 0,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Advisory-lock helper table for singleton background loops (scheduler,
-- janitor) when several server replicas run behind a load balancer.
CREATE TABLE IF NOT EXISTS pf_leader (
    role       TEXT PRIMARY KEY,
    holder     TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
