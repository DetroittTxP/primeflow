-- Native pf_logs partitioning, push work pools, OIDC / password-reset support.
-- Additive and idempotent, like 0001-0003.

-- ------------------------------------------------ pf_logs -> RANGE(ts) ---
--
-- Convert pf_logs to a range-partitioned table keyed on ts. Existing rows are
-- attached as a single bounded partition (pf_logs_p0) that the janitor drops
-- once its whole range is past retention; new rows land in monthly partitions
-- the janitor pre-creates. On a large existing pf_logs the ATTACH does one
-- validation scan — run this in a maintenance window (see README).

DO $$
DECLARE
    -- p0 (the legacy rows) covers everything up to the start of next month, so
    -- rows written earlier today are inside its range. Monthly partitions begin
    -- next month; the janitor extends them and drops p0 once it is fully aged.
    m_next  date := (date_trunc('month', now()) + interval '1 month')::date;
    m_next2 date := (date_trunc('month', now()) + interval '2 month')::date;
    m_next3 date := (date_trunc('month', now()) + interval '3 month')::date;
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        WHERE c.relname = 'pf_logs'
    ) THEN
        RETURN; -- already partitioned
    END IF;

    ALTER TABLE pf_logs RENAME TO pf_logs_p0;
    ALTER INDEX IF EXISTS pf_logs_run_idx RENAME TO pf_logs_p0_run_idx;
    ALTER INDEX IF EXISTS pf_logs_ts_idx  RENAME TO pf_logs_p0_ts_idx;
    -- The parent's (id, ts) PK propagates to every partition on ATTACH, and its
    -- index name (pf_logs_pkey) must be free first.
    ALTER TABLE pf_logs_p0 DROP CONSTRAINT IF EXISTS pf_logs_pkey;

    CREATE TABLE pf_logs (
        id          BIGINT NOT NULL DEFAULT nextval('pf_logs_id_seq'),
        flow_run_id TEXT NOT NULL REFERENCES pf_flow_runs(id) ON DELETE CASCADE,
        task_run_id TEXT,
        level       TEXT NOT NULL DEFAULT 'INFO',
        message     TEXT NOT NULL,
        fields      JSONB,
        ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (id, ts)
    ) PARTITION BY RANGE (ts);

    CREATE INDEX pf_logs_run_idx ON pf_logs (flow_run_id, id);
    CREATE INDEX pf_logs_ts_idx  ON pf_logs (ts);

    -- Re-home the id sequence onto the new parent so old partitions carry no
    -- dependency and can be DROP TABLE'd by the janitor.
    ALTER SEQUENCE pf_logs_id_seq OWNED BY pf_logs.id;

    -- Bound the legacy partition so it is droppable once fully past retention.
    EXECUTE format(
        'ALTER TABLE pf_logs_p0 ADD CONSTRAINT pf_logs_p0_ck CHECK (ts < %L)', m_next);
    EXECUTE format(
        'ALTER TABLE pf_logs ATTACH PARTITION pf_logs_p0 FOR VALUES FROM (MINVALUE) TO (%L)', m_next);

    EXECUTE format('CREATE TABLE pf_logs_%s PARTITION OF pf_logs FOR VALUES FROM (%L) TO (%L)',
        to_char(m_next, 'YYYY_MM'), m_next, m_next2);
    EXECUTE format('CREATE TABLE pf_logs_%s PARTITION OF pf_logs FOR VALUES FROM (%L) TO (%L)',
        to_char(m_next2, 'YYYY_MM'), m_next2, m_next3);
END $$;

-- ----------------------------------------------------- push work pools ---

ALTER TABLE pf_work_queues ADD COLUMN IF NOT EXISTS push_endpoint TEXT NOT NULL DEFAULT '';
ALTER TABLE pf_work_queues ADD COLUMN IF NOT EXISTS push_secret   TEXT NOT NULL DEFAULT '';

-- ---------------------------------------------------- auth: SSO + reset ---

ALTER TABLE pf_users ADD COLUMN IF NOT EXISTS auth_provider TEXT NOT NULL DEFAULT 'local';

CREATE TABLE IF NOT EXISTS pf_password_resets (
    token      TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES pf_users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS pf_password_resets_expires_idx ON pf_password_resets (expires_at);
