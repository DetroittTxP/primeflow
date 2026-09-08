-- Pool-scoped worker credentials.
--
-- A worker that reaches the orchestrator over its API needs a credential of its
-- own: not the shared admin bearer, which would hand every site full authority,
-- but a key bound to the lanes that site actually runs. Additive and idempotent,
-- like 0001-0004.

-- Empty means unrestricted, which is what every key issued before this migration
-- was, so existing keys keep behaving exactly as they did.
ALTER TABLE pf_api_keys ADD COLUMN IF NOT EXISTS pools text[] NOT NULL DEFAULT '{}';
