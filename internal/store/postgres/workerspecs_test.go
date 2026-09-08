package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/store"
)

func TestWorkerSpecCRUD(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	ws := &core.WorkerSpec{
		Name: "primex-worker-1", Image: "primex/primeflow:latest",
		Queues: []string{"default", "vcd"}, Concurrency: 6, Replicas: 2,
		Delivery: "git", Namespace: "primeflow", CreatedBy: "admin@x",
		Env: map[string]string{"FOO": "bar"},
	}
	if err := st.UpsertWorkerSpec(ctx, ws); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if ws.ID == "" || ws.CreatedAt.IsZero() {
		t.Fatalf("insert did not populate id/created_at: %+v", ws)
	}
	if ws.SyncState != core.SyncPending {
		t.Fatalf("new spec should start pending, got %q", ws.SyncState)
	}

	got, err := st.GetWorkerSpecByName(ctx, "primex-worker-1")
	if err != nil {
		t.Fatalf("get by name: %v", err)
	}
	if got.Concurrency != 6 || len(got.Queues) != 2 || got.Env["FOO"] != "bar" || got.CreatedBy != "admin@x" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	// update keeps the id and does not touch sync bookkeeping
	got.Concurrency = 10
	got.AutoSync = true
	if err := st.UpsertWorkerSpec(ctx, got); err != nil {
		t.Fatalf("update: %v", err)
	}
	again, _ := st.GetWorkerSpec(ctx, ws.ID)
	if again.Concurrency != 10 || !again.AutoSync {
		t.Fatalf("update not persisted: %+v", again)
	}

	// unique name is enforced
	dup := &core.WorkerSpec{Name: "primex-worker-1", Queues: []string{"default"}, Delivery: "git"}
	if err := st.UpsertWorkerSpec(ctx, dup); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("duplicate name err = %v, want ErrConflict", err)
	}

	// auto-sync filter
	list, err := st.WorkerSpecsForAutoSync(ctx)
	if err != nil || len(list) != 1 || list[0].Name != "primex-worker-1" {
		t.Fatalf("auto-sync filter: %v / %+v", err, list)
	}

	// sync result write
	res := core.WorkerSpecSyncResult{
		State: core.SyncSynced, Hash: "abc123", CommitSHA: "deadbeef", At: time.Now().UTC(),
	}
	if err := st.SetWorkerSpecSyncResult(ctx, ws.ID, res); err != nil {
		t.Fatalf("set sync result: %v", err)
	}
	synced, _ := st.GetWorkerSpec(ctx, ws.ID)
	if synced.SyncState != core.SyncSynced || synced.LastSyncedSHA != "deadbeef" ||
		synced.LastSyncedHash != "abc123" || synced.LastSyncedAt == nil {
		t.Fatalf("sync result not persisted: %+v", synced)
	}

	// an error result records the message but leaves last_synced_* alone
	if err := st.SetWorkerSpecSyncResult(ctx, ws.ID, core.WorkerSpecSyncResult{
		State: core.SyncError, Hash: "xyz", Err: "boom", At: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("set error result: %v", err)
	}
	errd, _ := st.GetWorkerSpec(ctx, ws.ID)
	if errd.SyncState != core.SyncError || errd.LastError != "boom" || errd.LastSyncedSHA != "deadbeef" {
		t.Fatalf("error result clobbered synced state: %+v", errd)
	}

	// delete
	if err := st.DeleteWorkerSpec(ctx, ws.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := st.GetWorkerSpec(ctx, ws.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("after delete err = %v, want ErrNotFound", err)
	}
	if err := st.DeleteWorkerSpec(ctx, ws.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("double delete err = %v, want ErrNotFound", err)
	}
}

func TestGitConnectionWithToken(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	if err := st.PutGitConnection(ctx, core.GitConnection{
		RepoURL: "https://github.com/acme/gitops.git", Branch: "main", Token: "ghp_secret",
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// the plain read strips the token
	pub, _ := st.GetGitConnection(ctx)
	if pub.Token != "" || !pub.HasToken {
		t.Fatalf("GetGitConnection leaked or lost the token: %+v", pub)
	}
	// the internal read returns it for the git engine
	gc, tok, err := st.GitConnectionWithToken(ctx)
	if err != nil || tok != "ghp_secret" || gc.RepoURL != "https://github.com/acme/gitops.git" {
		t.Fatalf("GitConnectionWithToken: %v / tok=%q / %+v", err, tok, gc)
	}
}
