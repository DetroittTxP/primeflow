package server

import (
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/store"
)

// getFlow powers the console's Flows drawer: the newest registered version of a
// flow, every version it has seen, its parameter schema, and its recent runs.
func (s *Server) getFlow(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	all, err := s.store.ListFlows(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	var versions []core.Flow
	for _, f := range all {
		if f.Name == name {
			versions = append(versions, f)
		}
	}
	if len(versions) == 0 {
		writeErr(w, http.StatusNotFound, store.ErrNotFound)
		return
	}
	// Newest first; the head carries the schema the UI renders.
	sort.Slice(versions, func(i, j int) bool { return versions[i].UpdatedAt.After(versions[j].UpdatedAt) })
	head := versions[0]

	runs, err := s.store.ListFlowRuns(r.Context(), store.FlowRunFilter{
		FlowNames: []string{name}, Limit: 20,
	})
	if err != nil {
		fail(w, err)
		return
	}
	total, _ := s.store.CountFlowRuns(r.Context(), store.FlowRunFilter{FlowNames: []string{name}})

	writeJSON(w, http.StatusOK, map[string]any{
		"name":          head.Name,
		"description":   head.Description,
		"tags":          head.Tags,
		"params_schema": head.ParamsSchema,
		"versions":      versions,
		"recent_runs":   runs,
		"total_runs":    total,
	})
}

// runChildren lists the sub-flow runs a run started.
func (s *Server) runChildren(w http.ResponseWriter, r *http.Request) {
	kids, err := s.store.ListChildRuns(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, kids)
}

// ------------------------------------------------------- log retention ---

func (s *Server) getLogRetention(w http.ResponseWriter, r *http.Request) {
	lr, err := s.store.GetLogRetention(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, lr)
}

func (s *Server) putLogRetention(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Enabled     *bool `json:"enabled,omitempty"`
		MaxAgeHours *int  `json:"max_age_hours,omitempty"`
	}
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	cur, err := s.store.GetLogRetention(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	if b.Enabled != nil {
		cur.Enabled = *b.Enabled
	}
	if b.MaxAgeHours != nil {
		if *b.MaxAgeHours < 1 {
			writeErr(w, http.StatusBadRequest, errors.New("max_age_hours must be >= 1"))
			return
		}
		cur.MaxAgeHours = *b.MaxAgeHours
	}
	if err := s.store.PutLogRetention(r.Context(), cur); err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "settings.log-retention-changed", "settings", "log_retention", mustJSON(cur))
	writeJSON(w, http.StatusOK, cur)
}

// getGitConnection returns the GitOps target repo config for the console's
// Settings → Git connection tab. The stored token is never included.
func (s *Server) getGitConnection(w http.ResponseWriter, r *http.Request) {
	gc, err := s.store.GetGitConnection(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, gc)
}

// putGitConnection saves the GitOps target repo. Only fields present in the body
// are changed; an omitted or empty token keeps the stored one. The provider is
// re-derived from the repo URL on every save.
func (s *Server) putGitConnection(w http.ResponseWriter, r *http.Request) {
	var b struct {
		RepoURL     *string `json:"repo_url,omitempty"`
		Branch      *string `json:"branch,omitempty"`
		BasePath    *string `json:"base_path,omitempty"`
		AutoSync    *bool   `json:"auto_sync,omitempty"`
		AuthorName  *string `json:"author_name,omitempty"`
		AuthorEmail *string `json:"author_email,omitempty"`
		Token       *string `json:"token,omitempty"`
	}
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	cur, err := s.store.GetGitConnection(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	cur.Token = "" // GetGitConnection strips it; keep it that way so an
	// omitted token in the body means "keep stored" (handled in the store).
	if b.RepoURL != nil {
		cur.RepoURL = strings.TrimSpace(*b.RepoURL)
	}
	if b.Branch != nil {
		cur.Branch = strings.TrimSpace(*b.Branch)
	}
	if b.BasePath != nil {
		cur.BasePath = strings.Trim(strings.TrimSpace(*b.BasePath), "/")
	}
	if b.AutoSync != nil {
		cur.AutoSync = *b.AutoSync
	}
	if b.AuthorName != nil {
		cur.AuthorName = strings.TrimSpace(*b.AuthorName)
	}
	if b.AuthorEmail != nil {
		cur.AuthorEmail = strings.TrimSpace(*b.AuthorEmail)
	}
	if b.Token != nil && strings.TrimSpace(*b.Token) != "" {
		cur.Token = strings.TrimSpace(*b.Token)
	}
	switch {
	case cur.RepoURL == "":
		cur.Provider = ""
	case strings.Contains(cur.RepoURL, "gitlab"):
		cur.Provider = "gitlab"
	case strings.Contains(cur.RepoURL, "github"):
		cur.Provider = "github"
	default:
		cur.Provider = "other"
	}
	if err := s.store.PutGitConnection(r.Context(), cur); err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "settings.git-connection-changed", "settings", "git_connection", nil)
	// Re-read so the response carries has_token / updated_at and no secret.
	out, _ := s.store.GetGitConnection(r.Context())
	writeJSON(w, http.StatusOK, out)
}
