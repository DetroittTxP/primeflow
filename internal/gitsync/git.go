package gitsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-git/go-billy/v5/memfs"
	billyutil "github.com/go-git/go-billy/v5/util"
	git "github.com/go-git/go-git/v5"
	gitconfig "github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/memory"

	"github.com/primex/primeflow/internal/core"
)

// ErrNoGitConnection is returned when a sync is attempted with no repo URL set
// in Settings -> Git connection.
var ErrNoGitConnection = errors.New("no git connection configured")

// SpecStore is the slice of the store the engine needs: the git connection
// with its (otherwise write-only) token.
type SpecStore interface {
	GitConnectionWithToken(ctx context.Context) (core.GitConnection, string, error)
}

// Engine renders a worker spec and commits the result to the GitOps repo. It
// clones into memory, so credentials and manifests never touch local disk, and
// works with any HTTPS git host (GitHub, GitLab, Gitea, ...) plus file:// for
// tests. It never opens a pull request — it commits straight to the branch.
type Engine struct {
	store SpecStore
	log   *slog.Logger
}

// NewEngine builds an Engine over the store.
func NewEngine(store SpecStore, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	return &Engine{store: store, log: log}
}

func authFor(token string) transport.AuthMethod {
	if strings.TrimSpace(token) == "" {
		return nil
	}
	// Username is ignored by GitHub/GitLab for PAT auth but must be non-empty.
	return &githttp.BasicAuth{Username: "oauth2", Password: token}
}

func signature(gc core.GitConnection) *object.Signature {
	name := strings.TrimSpace(gc.AuthorName)
	if name == "" {
		name = "PrimeFlow"
	}
	email := strings.TrimSpace(gc.AuthorEmail)
	if email == "" {
		email = "primeflow@localhost"
	}
	return &object.Signature{Name: name, Email: email, When: time.Now().UTC()}
}

// Sync renders spec and pushes it. A clean worktree after writing the files is
// a no-op that still reports SyncSynced with the current HEAD. Any failure
// returns a SyncError result (with the tree hash filled in) alongside the error
// so the caller can persist it.
func (e *Engine) Sync(ctx context.Context, spec core.WorkerSpec) (core.WorkerSpecSyncResult, error) {
	now := time.Now().UTC()
	fail := func(err error, hash string) (core.WorkerSpecSyncResult, error) {
		return core.WorkerSpecSyncResult{State: core.SyncError, Hash: hash, Err: err.Error(), At: now}, err
	}

	gc, token, err := e.store.GitConnectionWithToken(ctx)
	if err != nil {
		return fail(err, "")
	}
	if strings.TrimSpace(gc.RepoURL) == "" {
		return fail(ErrNoGitConnection, "")
	}
	branch := strings.TrimSpace(gc.Branch)
	if branch == "" {
		branch = "main"
	}

	files, err := Render(spec, gc)
	if err != nil {
		return fail(err, "")
	}
	hash := TreeHash(files)
	auth := authFor(token)
	branchRef := plumbing.NewBranchReferenceName(branch)

	fs := memfs.New()
	repo, err := git.CloneContext(ctx, memory.NewStorage(), fs, &git.CloneOptions{
		URL:           gc.RepoURL,
		Auth:          auth,
		ReferenceName: branchRef,
		SingleBranch:  true,
		Depth:         1,
	})
	fresh := false
	if errors.Is(err, transport.ErrEmptyRemoteRepository) {
		// Brand-new repo with no commits yet: start one on a clean storer/fs.
		fs = memfs.New()
		repo, err = git.InitWithOptions(memory.NewStorage(), fs, git.InitOptions{DefaultBranch: branchRef})
		if err == nil {
			_, err = repo.CreateRemote(&gitconfig.RemoteConfig{Name: "origin", URLs: []string{gc.RepoURL}})
		}
		fresh = true
	}
	if err != nil {
		return fail(fmt.Errorf("clone %s: %w", gc.RepoURL, err), hash)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return fail(err, hash)
	}
	for path, data := range files {
		if err := billyutil.WriteFile(wt.Filesystem, path, data, 0o644); err != nil {
			return fail(fmt.Errorf("write %s: %w", path, err), hash)
		}
	}
	if err := wt.AddWithOptions(&git.AddOptions{All: true}); err != nil {
		return fail(err, hash)
	}

	status, err := wt.Status()
	if err != nil {
		return fail(err, hash)
	}
	if status.IsClean() && !fresh {
		head, err := repo.Head()
		if err != nil {
			return fail(err, hash)
		}
		e.log.Debug("worker spec already in sync", "spec", spec.Name, "sha", head.Hash().String())
		return core.WorkerSpecSyncResult{State: core.SyncSynced, Hash: hash, CommitSHA: head.Hash().String(), At: now}, nil
	}

	msg := fmt.Sprintf("chore(primeflow): sync worker %s (pools: %s)",
		spec.Name, strings.Join(spec.Queues, ","))
	commit, err := wt.Commit(msg, &git.CommitOptions{Author: signature(gc)})
	if err != nil {
		return fail(fmt.Errorf("commit: %w", err), hash)
	}
	refspec := gitconfig.RefSpec(fmt.Sprintf("%s:%s", branchRef, branchRef))
	err = repo.PushContext(ctx, &git.PushOptions{
		RemoteName: "origin",
		Auth:       auth,
		RefSpecs:   []gitconfig.RefSpec{refspec},
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return fail(fmt.Errorf("push %s: %w", branch, err), hash)
	}

	e.log.Info("worker spec synced", "spec", spec.Name, "branch", branch, "sha", commit.String())
	return core.WorkerSpecSyncResult{State: core.SyncSynced, Hash: hash, CommitSHA: commit.String(), At: now}, nil
}
