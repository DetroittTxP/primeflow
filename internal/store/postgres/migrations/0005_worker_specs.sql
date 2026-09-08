-- Worker specs: the persisted inputs to GitOps worker delivery.
--
-- A row is everything the Add-worker wizard collects. The server renders it to
-- Kubernetes manifests and (for git/argocd/flux delivery) commits them to the
-- configured GitOps repo. Additive and idempotent, like 0001-0004.

CREATE TABLE IF NOT EXISTS pf_worker_specs (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    image            TEXT NOT NULL DEFAULT 'primex/primeflow:latest',
    queues           TEXT[] NOT NULL DEFAULT '{}',
    concurrency      INTEGER NOT NULL DEFAULT 4,
    replicas         INTEGER NOT NULL DEFAULT 1,
    -- git | argocd | flux. 'script' delivery stays wizard-only (no server push).
    delivery         TEXT NOT NULL DEFAULT 'git',
    namespace        TEXT NOT NULL DEFAULT 'primeflow',
    -- Path within the repo. Blank means "<git base_path>/<name>", resolved at render.
    repo_path        TEXT NOT NULL DEFAULT '',
    auto_sync        BOOLEAN NOT NULL DEFAULT false,
    -- Extra env, merged over the derived defaults.
    env              JSONB NOT NULL DEFAULT '{}',
    -- {project, dest_server, revision}; only meaningful for delivery='argocd'.
    argocd           JSONB,
    -- sha256 of the last tree we rendered / pushed, and the commit it produced.
    last_render_hash TEXT NOT NULL DEFAULT '',
    last_synced_hash TEXT NOT NULL DEFAULT '',
    last_synced_sha  TEXT NOT NULL DEFAULT '',
    last_synced_at   TIMESTAMPTZ,
    -- pending | synced | drift | error
    sync_state       TEXT NOT NULL DEFAULT 'pending',
    last_error       TEXT NOT NULL DEFAULT '',
    created_by       TEXT NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The reconciler only ever scans auto-sync rows.
CREATE INDEX IF NOT EXISTS pf_worker_specs_autosync_idx
    ON pf_worker_specs (auto_sync) WHERE auto_sync;
