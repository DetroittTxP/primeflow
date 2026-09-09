package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/gitsync"
)

// workerSpecBody is the create/update payload. On the {id} route, omitted
// fields keep their stored value (auto_sync is a pointer so "off" is
// distinguishable from "absent"); on a create, zero values fall back to the
// column defaults.
type workerSpecBody struct {
	Name        string                 `json:"name"`
	Image       string                 `json:"image"`
	Queues      []string               `json:"queues"`
	Concurrency int                    `json:"concurrency"`
	Replicas    int                    `json:"replicas"`
	Delivery    string                 `json:"delivery"`
	Namespace   string                 `json:"namespace"`
	RepoPath    string                 `json:"repo_path"`
	AutoSync    *bool                  `json:"auto_sync"`
	Env         map[string]string      `json:"env"`
	ArgoCD      *core.WorkerSpecArgoCD `json:"argocd"`
}

var validDelivery = map[string]bool{"git": true, "argocd": true, "flux": true}

// workerSpecView is a spec plus its rendered manifests, so the console can show
// a live preview without a second round trip.
type workerSpecView struct {
	*core.WorkerSpec
	Rendered map[string]string `json:"rendered,omitempty"`
	RepoPath string            `json:"resolved_repo_path,omitempty"`
}

func (s *Server) renderView(r *http.Request, ws *core.WorkerSpec) *workerSpecView {
	v := &workerSpecView{WorkerSpec: ws}
	gc, err := s.store.GetGitConnection(r.Context())
	if err != nil {
		return v
	}
	v.RepoPath = gitsync.RepoPath(*ws, gc)
	files, err := gitsync.Render(*ws, gc)
	if err != nil {
		return v
	}
	v.Rendered = make(map[string]string, len(files))
	for p, b := range files {
		v.Rendered[p] = string(b)
	}
	return v
}

func (s *Server) listWorkerSpecs(w http.ResponseWriter, r *http.Request) {
	specs, err := s.store.ListWorkerSpecs(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	if specs == nil {
		specs = []core.WorkerSpec{}
	}
	writeJSON(w, http.StatusOK, specs)
}

func (s *Server) getWorkerSpec(w http.ResponseWriter, r *http.Request) {
	ws, err := s.store.GetWorkerSpec(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.renderView(r, ws))
}

func (s *Server) upsertWorkerSpec(w http.ResponseWriter, r *http.Request) {
	var b workerSpecBody
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}

	id := r.PathValue("id")
	var ws *core.WorkerSpec
	if id != "" {
		cur, err := s.store.GetWorkerSpec(r.Context(), id)
		if err != nil {
			fail(w, err)
			return
		}
		ws = cur
	} else {
		ws = &core.WorkerSpec{}
		if p := principalFrom(r.Context()); p != nil {
			ws.CreatedBy = p.Email
		}
	}

	if b.Name != "" {
		ws.Name = strings.TrimSpace(b.Name)
	}
	if b.Image != "" {
		ws.Image = strings.TrimSpace(b.Image)
	}
	if len(b.Queues) > 0 {
		ws.Queues = b.Queues
	}
	if b.Concurrency > 0 {
		ws.Concurrency = b.Concurrency
	}
	if b.Replicas > 0 {
		ws.Replicas = b.Replicas
	}
	if b.Delivery != "" {
		ws.Delivery = strings.TrimSpace(b.Delivery)
	}
	if b.Namespace != "" {
		ws.Namespace = strings.TrimSpace(b.Namespace)
	}
	if b.RepoPath != "" {
		ws.RepoPath = strings.Trim(strings.TrimSpace(b.RepoPath), "/")
	}
	if b.AutoSync != nil {
		ws.AutoSync = *b.AutoSync
	}
	if b.Env != nil {
		ws.Env = b.Env
	}
	if b.ArgoCD != nil {
		ws.ArgoCD = b.ArgoCD
	}

	if ws.Name == "" {
		writeErr(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	if len(ws.Queues) == 0 {
		writeErr(w, http.StatusBadRequest, errors.New("at least one work pool (queue) is required"))
		return
	}
	if ws.Delivery == "" {
		ws.Delivery = "git"
	}
	if !validDelivery[ws.Delivery] {
		writeErr(w, http.StatusBadRequest, errors.New(`delivery must be "git", "argocd" or "flux"`))
		return
	}

	created := ws.ID == ""
	if err := s.store.UpsertWorkerSpec(r.Context(), ws); err != nil {
		fail(w, err)
		return
	}

	evt := "worker-spec.updated"
	code := http.StatusOK
	if created {
		evt, code = "worker-spec.created", http.StatusCreated
	}
	s.events.Emit(r.Context(), evt, "worker-spec", ws.ID, mustJSON(map[string]any{"name": ws.Name}))
	writeJSON(w, code, s.renderView(r, ws))
}

func (s *Server) deleteWorkerSpec(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ws, err := s.store.GetWorkerSpec(r.Context(), id)
	if err != nil {
		fail(w, err)
		return
	}
	if err := s.store.DeleteWorkerSpec(r.Context(), id); err != nil {
		fail(w, err)
		return
	}
	s.events.Emit(r.Context(), "worker-spec.deleted", "worker-spec", id, mustJSON(map[string]any{"name": ws.Name}))
	w.WriteHeader(http.StatusNoContent)
}

// syncWorkerSpec renders the spec and commits it to the GitOps repo now,
// synchronously — the manual counterpart to the auto-sync reconciler.
func (s *Server) syncWorkerSpec(w http.ResponseWriter, r *http.Request) {
	if s.gitEngine == nil {
		writeErr(w, http.StatusServiceUnavailable, errors.New("server-side git sync is not enabled"))
		return
	}
	ws, err := s.store.GetWorkerSpec(r.Context(), r.PathValue("id"))
	if err != nil {
		fail(w, err)
		return
	}
	gc, gcErr := s.store.GetGitConnection(r.Context())
	if gcErr != nil {
		fail(w, gcErr)
		return
	}
	if gc.RepoURL == "" {
		writeErr(w, http.StatusConflict, errors.New("no git connection configured (Settings → Git connection)"))
		return
	}

	res, syncErr := s.gitEngine.Sync(r.Context(), *ws)
	if err := s.store.SetWorkerSpecSyncResult(r.Context(), ws.ID, res); err != nil {
		fail(w, err)
		return
	}
	if syncErr != nil {
		s.events.Emit(r.Context(), "worker-spec.sync-failed", "worker-spec", ws.ID,
			mustJSON(map[string]any{"name": ws.Name, "error": syncErr.Error()}))
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"sync_state": core.SyncError, "last_error": syncErr.Error(),
		})
		return
	}
	s.events.Emit(r.Context(), "worker-spec.synced", "worker-spec", ws.ID,
		mustJSON(map[string]any{"name": ws.Name, "commit_sha": res.CommitSHA}))

	fresh, _ := s.store.GetWorkerSpec(r.Context(), ws.ID)
	writeJSON(w, http.StatusOK, s.renderView(r, fresh))
}

// previewWorkerSpec renders an unsaved spec body — the wizard uses it for a
// live preview before the operator commits to saving.
func (s *Server) previewWorkerSpec(w http.ResponseWriter, r *http.Request) {
	var b workerSpecBody
	if err := decode(r, &b); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	ws := core.WorkerSpec{
		Name: strings.TrimSpace(b.Name), Image: strings.TrimSpace(b.Image),
		Queues: b.Queues, Concurrency: b.Concurrency, Replicas: b.Replicas,
		Delivery: b.Delivery, Namespace: strings.TrimSpace(b.Namespace),
		RepoPath: strings.Trim(strings.TrimSpace(b.RepoPath), "/"),
		Env:      b.Env, ArgoCD: b.ArgoCD,
	}
	if ws.Image == "" {
		ws.Image = "primex/primeflow:latest"
	}
	if ws.Concurrency <= 0 {
		ws.Concurrency = 4
	}
	if ws.Replicas <= 0 {
		ws.Replicas = 1
	}
	if ws.Namespace == "" {
		ws.Namespace = "primeflow"
	}
	if b.AutoSync != nil {
		ws.AutoSync = *b.AutoSync
	}
	if ws.Delivery == "" || !validDelivery[ws.Delivery] {
		ws.Delivery = "git"
	}
	if ws.Name == "" {
		writeErr(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	gc, _ := s.store.GetGitConnection(r.Context())
	files, err := gitsync.Render(ws, gc)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	out := make(map[string]string, len(files))
	for p, bts := range files {
		out[p] = string(bts)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"resolved_repo_path": gitsync.RepoPath(ws, gc),
		"rendered":           out,
	})
}
