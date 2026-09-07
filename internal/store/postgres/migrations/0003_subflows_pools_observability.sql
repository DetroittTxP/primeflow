-- Sub-flows, work-pool autoscaling metadata, parameter schemas, trace
-- propagation, and a bounded pf_logs.
--
-- Additive and idempotent, like 0001 and 0002.

-- ----------------------------------------------------------- sub-flows ---

ALTER TABLE pf_flow_runs ADD COLUMN IF NOT EXISTS
    parent_run_id TEXT REFERENCES pf_flow_runs(id) ON DELETE SET NULL;
ALTER TABLE pf_flow_runs ADD COLUMN IF NOT EXISTS parent_task_key TEXT;
-- W3C traceparent of the triggering span, so a child's flow_run span nests
-- under its parent's across process boundaries.
ALTER TABLE pf_flow_runs ADD COLUMN IF NOT EXISTS trace_context TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS pf_flow_runs_parent_idx
    ON pf_flow_runs (parent_run_id) WHERE parent_run_id IS NOT NULL;

-- ------------------------------------------------------ flow param schema ---

-- A compact, reflection-derived description of a flow's parameter struct, used
-- by the console's Flows page to render a typed quick-run form.
ALTER TABLE pf_flows ADD COLUMN IF NOT EXISTS params_schema JSONB;

-- --------------------------------------------------- work-pool autoscaling ---

-- A work queue doubles as a work pool: these columns let an operator declare the
-- scaling envelope, and the server exposes primeflow_queue_desired_workers so
-- KEDA / an HPA can act on it. PrimeFlow never launches workers itself.
ALTER TABLE pf_work_queues ADD COLUMN IF NOT EXISTS min_workers INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pf_work_queues ADD COLUMN IF NOT EXISTS max_workers INTEGER;
ALTER TABLE pf_work_queues ADD COLUMN IF NOT EXISTS target_ready_per_worker INTEGER NOT NULL DEFAULT 5;
-- Free-text owner so an external team's pool is attributable in the console.
ALTER TABLE pf_work_queues ADD COLUMN IF NOT EXISTS owner TEXT NOT NULL DEFAULT '';
-- 'pull' (workers poll) is the only implemented behaviour; 'push' is reserved.
ALTER TABLE pf_work_queues ADD COLUMN IF NOT EXISTS pool_type TEXT NOT NULL DEFAULT 'pull';

-- ------------------------------------------------------- log retention ---

CREATE INDEX IF NOT EXISTS pf_logs_ts_idx ON pf_logs (ts);

INSERT INTO pf_settings (key, value)
VALUES ('log_retention', '{"enabled": true, "max_age_hours": 720}')
ON CONFLICT (key) DO NOTHING;
