package gitsync

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/primex/primeflow/internal/core"
)

type fakeConn struct {
	gc    core.GitConnection
	token string
}

func (f fakeConn) GitConnectionWithToken(ctx context.Context) (core.GitConnection, string, error) {
	return f.gc, f.token, nil
}

// TestEngineSyncFileRepo exercises the full clone/commit/push against a local
// bare repo (the file transport needs no auth and no network).
func TestEngineSyncFileRepo(t *testing.T) {
	bare := filepath.Join(t.TempDir(), "gitops.git")
	if _, err := git.PlainInitWithOptions(bare, &git.PlainInitOptions{
		InitOptions: git.InitOptions{DefaultBranch: plumbing.NewBranchReferenceName("main")},
		Bare:        true,
	}); err != nil {
		t.Fatalf("init bare: %v", err)
	}
	eng := NewEngine(fakeConn{gc: core.GitConnection{
		RepoURL: bare, Branch: "main",
		AuthorName: "Test", AuthorEmail: "test@example.com",
	}}, nil)

	spec := core.WorkerSpec{
		Name: "w1", Image: "primex/primeflow:latest",
		Queues: []string{"default"}, Concurrency: 4, Replicas: 1,
		Delivery: "git", Namespace: "primeflow",
	}
	ctx := context.Background()

	// 1. first sync bootstraps the empty repo and commits.
	res, err := eng.Sync(ctx, spec)
	if err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if res.State != core.SyncSynced || res.CommitSHA == "" {
		t.Fatalf("first sync result: %+v", res)
	}

	// 2. same spec ⇒ clean worktree ⇒ no new commit.
	res2, err := eng.Sync(ctx, spec)
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if res2.CommitSHA != res.CommitSHA {
		t.Fatalf("no-op sync made a new commit: %s -> %s", res.CommitSHA, res2.CommitSHA)
	}

	// 3. change the spec ⇒ a fresh commit.
	spec.Concurrency = 8
	res3, err := eng.Sync(ctx, spec)
	if err != nil {
		t.Fatalf("third sync: %v", err)
	}
	if res3.CommitSHA == res.CommitSHA {
		t.Fatalf("changed spec did not produce a new commit")
	}

	// verify the pushed content by cloning the bare repo out.
	work := filepath.Join(t.TempDir(), "checkout")
	if _, err := git.PlainClone(work, false, &git.CloneOptions{URL: bare}); err != nil {
		t.Fatalf("clone back: %v", err)
	}
	sec, err := os.ReadFile(filepath.Join(work, "workers", "w1", "secret.yaml"))
	if err != nil {
		t.Fatalf("read secret.yaml: %v", err)
	}
	if !strings.Contains(string(sec), `PRIMEFLOW_CONCURRENCY: "8"`) {
		t.Fatalf("pushed secret.yaml did not reflect the change:\n%s", sec)
	}
	if _, err := os.Stat(filepath.Join(work, "workers", "w1", "deployment.yaml")); err != nil {
		t.Fatalf("deployment.yaml not pushed: %v", err)
	}
}

func TestEngineSyncNoConnection(t *testing.T) {
	eng := NewEngine(fakeConn{gc: core.GitConnection{}}, nil)
	res, err := eng.Sync(context.Background(), core.WorkerSpec{Name: "x", Queues: []string{"default"}, Delivery: "git"})
	if err == nil {
		t.Fatal("expected an error with no repo configured")
	}
	if res.State != core.SyncError {
		t.Fatalf("state = %q, want error", res.State)
	}
}
