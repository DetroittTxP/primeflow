package postgres

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

// ------------------------------------------------------------------ users ---

const userCols = `id, email, role, active, auth_provider, last_login_at, created_at, updated_at`

func scanUser(sc interface{ Scan(...any) error }) (*core.User, error) {
	var u core.User
	if err := sc.Scan(&u.ID, &u.Email, &u.Role, &u.Active, &u.AuthProvider, &u.LastLoginAt,
		&u.CreatedAt, &u.UpdatedAt); err != nil {
		return nil, err
	}
	return &u, nil
}

// CreateUser inserts an operator account. Email uniqueness is case-insensitive
// (enforced by a functional index), so a clash surfaces as store.ErrConflict.
func (s *Store) CreateUser(ctx context.Context, in store.UserInput) (*core.User, error) {
	if in.PasswordHash == "" {
		return nil, errors.New("postgres: CreateUser needs a password hash")
	}
	provider := in.AuthProvider
	if provider == "" {
		provider = "local"
	}
	const q = `
INSERT INTO pf_users (id, email, password_hash, role, active, auth_provider)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING ` + userCols
	row := s.db.QueryRowContext(ctx, q, in.ID, strings.TrimSpace(in.Email),
		in.PasswordHash, in.Role, in.Active, provider)
	u, err := scanUser(row)
	return u, mapErr(err)
}

// GetUser loads an account by id.
func (s *Store) GetUser(ctx context.Context, id string) (*core.User, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+userCols+` FROM pf_users WHERE id = $1`, id)
	u, err := scanUser(row)
	return u, mapErr(err)
}

// GetUserByEmail loads an active-or-not account by email, case-insensitively.
func (s *Store) GetUserByEmail(ctx context.Context, email string) (*core.User, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+userCols+` FROM pf_users WHERE lower(email) = lower($1)`, strings.TrimSpace(email))
	u, err := scanUser(row)
	return u, mapErr(err)
}

// GetUserAuth returns the account plus its password hash for a login check.
func (s *Store) GetUserAuth(ctx context.Context, email string) (*core.User, string, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+userCols+`, password_hash FROM pf_users WHERE lower(email) = lower($1)`,
		strings.TrimSpace(email))
	var u core.User
	var hash string
	if err := row.Scan(&u.ID, &u.Email, &u.Role, &u.Active, &u.AuthProvider, &u.LastLoginAt,
		&u.CreatedAt, &u.UpdatedAt, &hash); err != nil {
		return nil, "", mapErr(err)
	}
	return &u, hash, nil
}

// CreatePasswordReset issues a single-use reset token valid for ttl.
func (s *Store) CreatePasswordReset(ctx context.Context, userID string, ttl time.Duration) (string, time.Time, error) {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return "", time.Time{}, err
	}
	tok := base64.RawURLEncoding.EncodeToString(token)
	expires := time.Now().Add(ttl).UTC()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO pf_password_resets (token, user_id, expires_at) VALUES ($1,$2,$3)`,
		tok, userID, expires)
	return tok, expires, mapErr(err)
}

// ConsumePasswordReset validates a token (unused, unexpired), marks it used, and
// returns the user id. store.ErrNotFound for an unknown / spent / expired token.
func (s *Store) ConsumePasswordReset(ctx context.Context, token string) (string, error) {
	var userID string
	err := s.db.QueryRowContext(ctx, `
UPDATE pf_password_resets
   SET used_at = now()
 WHERE token = $1 AND used_at IS NULL AND expires_at > now()
RETURNING user_id`, token).Scan(&userID)
	return userID, mapErr(err)
}

// DeleteExpiredPasswordResets is run by the janitor.
func (s *Store) DeleteExpiredPasswordResets(ctx context.Context, now time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM pf_password_resets WHERE expires_at <= $1 OR used_at IS NOT NULL`, now.UTC())
	if err != nil {
		return 0, mapErr(err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ListUsers returns every account, oldest first.
func (s *Store) ListUsers(ctx context.Context) ([]core.User, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+userCols+` FROM pf_users ORDER BY created_at`)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	out := []core.User{}
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *u)
	}
	return out, rows.Err()
}

// CountUsers is used at start-up to decide whether to seed a bootstrap admin.
func (s *Store) CountUsers(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM pf_users`).Scan(&n)
	return n, mapErr(err)
}

// UpdateUser changes role, active flag and/or password. Any nil / empty argument
// leaves that column alone.
func (s *Store) UpdateUser(ctx context.Context, id string, role *string, active *bool, passwordHash string) (*core.User, error) {
	sets := []string{"updated_at = now()"}
	args := []any{id}
	add := func(frag string, v any) {
		args = append(args, v)
		sets = append(sets, fmt.Sprintf(frag, len(args)))
	}
	if role != nil {
		add("role = $%d", *role)
	}
	if active != nil {
		add("active = $%d", *active)
	}
	if passwordHash != "" {
		add("password_hash = $%d", passwordHash)
	}
	q := `UPDATE pf_users SET ` + strings.Join(sets, ", ") + ` WHERE id = $1 RETURNING ` + userCols
	row := s.db.QueryRowContext(ctx, q, args...)
	u, err := scanUser(row)
	return u, mapErr(err)
}

// TouchUserLogin stamps a successful login.
func (s *Store) TouchUserLogin(ctx context.Context, id string, at time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE pf_users SET last_login_at = $2, updated_at = now() WHERE id = $1`, id, at.UTC())
	return mapErr(err)
}

// --------------------------------------------------------------- sessions ---

// CreateSession stores a new browser session.
func (s *Store) CreateSession(ctx context.Context, se *core.Session) error {
	const q = `
INSERT INTO pf_sessions (id, user_id, created_at, last_seen_at, expires_at, ip, user_agent)
VALUES ($1, $2, now(), now(), $3, $4, $5)
RETURNING created_at, last_seen_at`
	return mapErr(s.db.QueryRowContext(ctx, q, se.ID, se.UserID, se.ExpiresAt.UTC(),
		se.IP, se.UserAgent).Scan(&se.CreatedAt, &se.LastSeenAt))
}

// GetSession resolves a session cookie to its session and user, rejecting
// expired sessions and inactive users with store.ErrNotFound.
func (s *Store) GetSession(ctx context.Context, id string, now time.Time) (*core.Session, *core.User, error) {
	const q = `
SELECT se.id, se.user_id, se.created_at, se.last_seen_at, se.expires_at, se.ip, se.user_agent,
       u.id, u.email, u.role, u.active, u.last_login_at, u.created_at, u.updated_at
FROM pf_sessions se
JOIN pf_users u ON u.id = se.user_id
WHERE se.id = $1 AND se.expires_at > $2 AND u.active`
	var se core.Session
	var u core.User
	err := s.db.QueryRowContext(ctx, q, id, now.UTC()).Scan(
		&se.ID, &se.UserID, &se.CreatedAt, &se.LastSeenAt, &se.ExpiresAt, &se.IP, &se.UserAgent,
		&u.ID, &u.Email, &u.Role, &u.Active, &u.LastLoginAt, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		return nil, nil, mapErr(err)
	}
	return &se, &u, nil
}

// TouchSession slides a session's expiry forward on activity.
func (s *Store) TouchSession(ctx context.Context, id string, lastSeen, expires time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE pf_sessions SET last_seen_at = $2, expires_at = $3 WHERE id = $1`,
		id, lastSeen.UTC(), expires.UTC())
	return mapErr(err)
}

// DeleteSession logs one session out.
func (s *Store) DeleteSession(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pf_sessions WHERE id = $1`, id)
	return mapErr(err)
}

// DeleteUserSessions logs a user out everywhere — used when an account is
// deactivated or its role changes under it.
func (s *Store) DeleteUserSessions(ctx context.Context, userID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pf_sessions WHERE user_id = $1`, userID)
	return mapErr(err)
}

// DeleteExpiredSessions is run periodically by the janitor.
func (s *Store) DeleteExpiredSessions(ctx context.Context, now time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM pf_sessions WHERE expires_at <= $1`, now.UTC())
	if err != nil {
		return 0, mapErr(err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
