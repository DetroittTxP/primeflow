package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/store"
)

// GetGitConnection reads the pf_settings row keyed "git_connection". The stored
// token is not returned; HasToken reports whether one is set. A missing row is
// an empty, unconfigured connection, not an error.
func (s *Store) GetGitConnection(ctx context.Context) (core.GitConnection, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM pf_settings WHERE key='git_connection'`).Scan(&raw)
	if err != nil {
		if mapErr(err) == store.ErrNotFound {
			return core.GitConnection{Branch: "main"}, nil
		}
		return core.GitConnection{}, mapErr(err)
	}
	var out core.GitConnection
	_ = json.Unmarshal(raw, &out)
	out.HasToken = out.Token != ""
	out.Token = ""
	if out.Branch == "" {
		out.Branch = "main"
	}
	return out, nil
}

// gitToken returns the stored token for server-side git operations. Kept
// separate from GetGitConnection so the secret never rides along a read that a
// handler might echo.
func (s *Store) gitToken(ctx context.Context) (string, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM pf_settings WHERE key='git_connection'`).Scan(&raw)
	if err != nil {
		return "", mapErr(err)
	}
	var out core.GitConnection
	_ = json.Unmarshal(raw, &out)
	return out.Token, nil
}

// PutGitConnection overwrites the git-connection setting. An empty Token keeps
// the previously stored one; the caller decides whether a rotation is intended.
func (s *Store) PutGitConnection(ctx context.Context, in core.GitConnection) error {
	if in.Token == "" {
		if prev, err := s.gitToken(ctx); err == nil {
			in.Token = prev
		}
	}
	in.HasToken = in.Token != ""
	in.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	raw, _ := json.Marshal(in)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO pf_settings (key, value, updated_at) VALUES ('git_connection', $1, now())
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, raw)
	return mapErr(err)
}
