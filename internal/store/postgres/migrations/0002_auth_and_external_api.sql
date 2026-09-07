-- Operator accounts, browser sessions, and the External API subsystem
-- (issued keys, per-key security controls, an audit trail, and a settings row
-- holding the master switch).
--
-- Every statement is idempotent so this runs on every process start alongside
-- 0001, exactly like the rest of the schema.

-- ------------------------------------------------------------------ users ---

CREATE TABLE IF NOT EXISTS pf_users (
    id            TEXT PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    -- 'admin' | 'operator' | 'viewer'. admin adds user / API-key / settings
    -- management on top of what operator can do; viewer is read-only.
    role          TEXT NOT NULL DEFAULT 'viewer',
    active        BOOLEAN NOT NULL DEFAULT true,
    last_login_at TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Case-insensitive uniqueness: nobody wants Alice@x and alice@x to be two logins.
CREATE UNIQUE INDEX IF NOT EXISTS pf_users_email_lower_idx ON pf_users (lower(email));

-- --------------------------------------------------------------- sessions ---

CREATE TABLE IF NOT EXISTS pf_sessions (
    id           TEXT PRIMARY KEY,               -- 32 random bytes, base64url
    user_id      TEXT NOT NULL REFERENCES pf_users(id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL,
    ip           TEXT NOT NULL DEFAULT '',
    user_agent   TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS pf_sessions_user_idx    ON pf_sessions (user_id);
CREATE INDEX IF NOT EXISTS pf_sessions_expires_idx ON pf_sessions (expires_at);

-- --------------------------------------------------------------- settings ---

CREATE TABLE IF NOT EXISTS pf_settings (
    key        TEXT PRIMARY KEY,
    value      JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The External API is off until an admin turns it on.
INSERT INTO pf_settings (key, value)
VALUES ('external_api', '{"enabled": false, "default_rate_limit_per_min": 600}')
ON CONFLICT (key) DO NOTHING;

-- -------------------------------------------------------------- api keys ---

CREATE TABLE IF NOT EXISTS pf_api_keys (
    id                 TEXT PRIMARY KEY,
    name               TEXT NOT NULL,
    description        TEXT NOT NULL DEFAULT '',
    owner_email        TEXT NOT NULL DEFAULT '',
    -- The visible half: 8 hex chars, shown as "pmx_87b703f0…" in the console and
    -- used to look a key up before the constant-time hash compare.
    prefix             TEXT NOT NULL UNIQUE,
    -- hex sha256 of the full secret. The secret is 24 random bytes, so a fast
    -- hash is the right choice here (unlike a password).
    secret_hash        TEXT NOT NULL,
    role               TEXT NOT NULL,
    active             BOOLEAN NOT NULL DEFAULT true,
    expires_at         TIMESTAMPTZ,
    -- NULL means "use the role / global default".
    rate_limit_per_min INTEGER,
    ip_allowlist       TEXT[] NOT NULL DEFAULT '{}',
    require_mtls        BOOLEAN NOT NULL DEFAULT false,
    redact_pii         BOOLEAN NOT NULL DEFAULT false,
    last_used_at       TIMESTAMPTZ,
    created_by         TEXT NOT NULL DEFAULT '',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS pf_api_keys_created_idx ON pf_api_keys (created_at DESC);

-- Per-key audit trail: creation, rotation, edits, and denied auth attempts. The
-- console's "History" view reads straight from this.
CREATE TABLE IF NOT EXISTS pf_api_key_events (
    id         BIGSERIAL PRIMARY KEY,
    api_key_id TEXT NOT NULL REFERENCES pf_api_keys(id) ON DELETE CASCADE,
    at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor      TEXT NOT NULL DEFAULT '',   -- operator email, or 'system'
    action     TEXT NOT NULL,              -- created|rotated|updated|activated|deactivated|deleted|auth-denied
    detail     JSONB
);

CREATE INDEX IF NOT EXISTS pf_api_key_events_key_idx ON pf_api_key_events (api_key_id, id DESC);
