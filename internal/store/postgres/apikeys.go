package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

// ------------------------------------------------------- external settings ---

// GetExternalAPISettings reads the pf_settings row keyed "external_api",
// returning zero values (disabled) if it is somehow missing.
func (s *Store) GetExternalAPISettings(ctx context.Context) (core.ExternalAPISettings, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM pf_settings WHERE key = 'external_api'`).Scan(&raw)
	if err != nil {
		if mapErr(err) == store.ErrNotFound {
			return core.ExternalAPISettings{}, nil
		}
		return core.ExternalAPISettings{}, mapErr(err)
	}
	var out core.ExternalAPISettings
	_ = json.Unmarshal(raw, &out)
	return out, nil
}

// PutExternalAPISettings overwrites the settings row.
func (s *Store) PutExternalAPISettings(ctx context.Context, in core.ExternalAPISettings) error {
	raw, _ := json.Marshal(in)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO pf_settings (key, value, updated_at)
VALUES ('external_api', $1, now())
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, raw)
	return mapErr(err)
}

// ------------------------------------------------------------- api keys ---

const apiKeyCols = `id, name, description, owner_email, prefix, secret_hash, role, active,
	expires_at, rate_limit_per_min, ip_allowlist, require_mtls, redact_pii,
	last_used_at, created_by, created_at, updated_at`

func scanAPIKey(sc interface{ Scan(...any) error }) (*core.APIKey, error) {
	var k core.APIKey
	if err := sc.Scan(&k.ID, &k.Name, &k.Description, &k.OwnerEmail, &k.Prefix, &k.SecretHash,
		&k.Role, &k.Active, &k.ExpiresAt, &k.RateLimitPerMin, pq.Array(&k.IPAllowlist),
		&k.RequireMTLS, &k.RedactPII, &k.LastUsedAt, &k.CreatedBy,
		&k.CreatedAt, &k.UpdatedAt); err != nil {
		return nil, err
	}
	return &k, nil
}

// CreateAPIKey inserts a new key. Prefix is UNIQUE, so a (vanishingly unlikely)
// collision surfaces as store.ErrConflict and the caller can retry.
func (s *Store) CreateAPIKey(ctx context.Context, k *core.APIKey) error {
	const q = `
INSERT INTO pf_api_keys
  (id, name, description, owner_email, prefix, secret_hash, role, active, expires_at,
   rate_limit_per_min, ip_allowlist, require_mtls, redact_pii, created_by)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
RETURNING created_at, updated_at`
	return mapErr(s.db.QueryRowContext(ctx, q,
		k.ID, k.Name, k.Description, k.OwnerEmail, k.Prefix, k.SecretHash, k.Role, k.Active,
		nullTime(k.ExpiresAt), k.RateLimitPerMin, textArray(k.IPAllowlist), k.RequireMTLS,
		k.RedactPII, k.CreatedBy).Scan(&k.CreatedAt, &k.UpdatedAt))
}

// GetAPIKey loads a key by id.
func (s *Store) GetAPIKey(ctx context.Context, id string) (*core.APIKey, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+apiKeyCols+` FROM pf_api_keys WHERE id = $1`, id)
	k, err := scanAPIKey(row)
	return k, mapErr(err)
}

// GetAPIKeyByPrefix loads a key by its lookup prefix — the hot path on every
// External API request.
func (s *Store) GetAPIKeyByPrefix(ctx context.Context, prefix string) (*core.APIKey, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+apiKeyCols+` FROM pf_api_keys WHERE prefix = $1`, prefix)
	k, err := scanAPIKey(row)
	return k, mapErr(err)
}

// ListAPIKeys returns keys newest first, optionally filtered.
func (s *Store) ListAPIKeys(ctx context.Context, f store.APIKeyFilter) ([]core.APIKey, error) {
	var where []string
	var args []any
	add := func(frag string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(frag, len(args)))
	}
	if f.Role != "" {
		add("role = $%d", f.Role)
	}
	if f.Active != nil {
		add("active = $%d", *f.Active)
	}
	if f.Search != "" {
		args = append(args, "%"+f.Search+"%")
		n := len(args)
		where = append(where, fmt.Sprintf(
			"(name ILIKE $%[1]d OR description ILIKE $%[1]d OR owner_email ILIKE $%[1]d OR prefix ILIKE $%[1]d)", n))
	}
	q := `SELECT ` + apiKeyCols + ` FROM pf_api_keys`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, " AND ")
	}
	q += ` ORDER BY created_at DESC`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []core.APIKey
	for rows.Next() {
		k, err := scanAPIKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *k)
	}
	return out, rows.Err()
}

// UpdateAPIKey writes the mutable fields of a key. The secret is not among them —
// use SetAPIKeySecret for rotation.
func (s *Store) UpdateAPIKey(ctx context.Context, k *core.APIKey) error {
	const q = `
UPDATE pf_api_keys SET
   name = $2, description = $3, owner_email = $4, role = $5, active = $6, expires_at = $7,
   rate_limit_per_min = $8, ip_allowlist = $9, require_mtls = $10, redact_pii = $11,
   updated_at = now()
WHERE id = $1
RETURNING updated_at`
	err := s.db.QueryRowContext(ctx, q, k.ID, k.Name, k.Description, k.OwnerEmail, k.Role,
		k.Active, nullTime(k.ExpiresAt), k.RateLimitPerMin, textArray(k.IPAllowlist),
		k.RequireMTLS, k.RedactPII).Scan(&k.UpdatedAt)
	return mapErr(err)
}

// SetAPIKeySecret rotates a key in place: new prefix, new hash, same id and
// controls. The old secret stops working the instant this commits.
func (s *Store) SetAPIKeySecret(ctx context.Context, id, prefix, secretHash string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE pf_api_keys SET prefix = $2, secret_hash = $3, updated_at = now() WHERE id = $1`,
		id, prefix, secretHash)
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// DeleteAPIKey removes a key and, by cascade, its audit trail.
func (s *Store) DeleteAPIKey(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM pf_api_keys WHERE id = $1`, id)
	if err != nil {
		return mapErr(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return store.ErrNotFound
	}
	return nil
}

// TouchAPIKey stamps last_used_at. Best-effort: a failure here never fails the
// request the key just authorised.
func (s *Store) TouchAPIKey(ctx context.Context, id string, at time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE pf_api_keys SET last_used_at = $2 WHERE id = $1`, id, at.UTC())
	return mapErr(err)
}

// ---------------------------------------------------------- key audit trail ---

// AppendAPIKeyEvent records one line of a key's history.
func (s *Store) AppendAPIKeyEvent(ctx context.Context, e *core.APIKeyEvent) error {
	const q = `
INSERT INTO pf_api_key_events (api_key_id, at, actor, action, detail)
VALUES ($1, now(), $2, $3, $4)
RETURNING id, at`
	return mapErr(s.db.QueryRowContext(ctx, q, e.APIKeyID, e.Actor, e.Action,
		nullJSON(e.Detail)).Scan(&e.ID, &e.At))
}

// ListAPIKeyEvents returns a key's history, newest first.
func (s *Store) ListAPIKeyEvents(ctx context.Context, apiKeyID string, limit int) ([]core.APIKeyEvent, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, api_key_id, at, actor, action, detail FROM pf_api_key_events
		 WHERE api_key_id = $1 ORDER BY id DESC LIMIT $2`, apiKeyID, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []core.APIKeyEvent
	for rows.Next() {
		var e core.APIKeyEvent
		if err := rows.Scan(&e.ID, &e.APIKeyID, &e.At, &e.Actor, &e.Action, scanJSON(&e.Detail)); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// TrimAPIKeyEvents keeps only the newest `keep` rows per key, so the mostly
// append-only denial log cannot grow without bound. Run from the janitor.
func (s *Store) TrimAPIKeyEvents(ctx context.Context, keep int) (int, error) {
	if keep <= 0 {
		keep = 200
	}
	const q = `
DELETE FROM pf_api_key_events e
USING (
    SELECT id, row_number() OVER (PARTITION BY api_key_id ORDER BY id DESC) AS rn
    FROM pf_api_key_events
) ranked
WHERE e.id = ranked.id AND ranked.rn > $1`
	res, err := s.db.ExecContext(ctx, q, keep)
	if err != nil {
		return 0, mapErr(err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// nullTime sends NULL for a nil *time.Time rather than the zero instant.
func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}
