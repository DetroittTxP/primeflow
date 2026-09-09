package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/store"
)

func TestPushPoolStoreFlow(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()

	if err := st.UpsertWorkQueue(ctx, &core.WorkQueue{
		Name: "serverless", PoolType: "push",
		PushEndpoint: "https://recv.example/run", PushSecret: "s3cr3t",
	}); err != nil {
		t.Fatalf("upsert push pool: %v", err)
	}
	got, _ := st.GetWorkQueue(ctx, "serverless")
	if got.PoolType != "push" || got.PushEndpoint != "https://recv.example/run" || !got.HasPushSecret {
		t.Fatalf("push pool not persisted: %+v", got)
	}
	// An empty secret on a later upsert keeps the stored one.
	if err := st.UpsertWorkQueue(ctx, &core.WorkQueue{
		Name: "serverless", PoolType: "push", PushEndpoint: "https://recv.example/run2", PushSecret: "",
	}); err != nil {
		t.Fatalf("re-upsert: %v", err)
	}
	got, _ = st.GetWorkQueue(ctx, "serverless")
	if got.PushSecret != "s3cr3t" || got.PushEndpoint != "https://recv.example/run2" {
		t.Fatalf("secret not preserved / endpoint not updated: %+v", got)
	}

	// Two ready runs, ordered by priority then age.
	older := mkRun(t, st, "serverless", 50, time.Now().Add(-2*time.Minute))
	urgent := mkRun(t, st, "serverless", 90, time.Now().Add(-time.Minute))
	future := mkRun(t, st, "serverless", 50, time.Now().Add(time.Hour)) // not ready

	ready, err := st.PushReadyRuns(ctx, "serverless", 10)
	if err != nil {
		t.Fatalf("PushReadyRuns: %v", err)
	}
	if len(ready) != 2 || ready[0].ID != urgent.ID || ready[1].ID != older.ID {
		t.Fatalf("ready order wrong: %v", ids(ready))
	}
	_ = future

	// Dispatch-hold the urgent one: it drops out of the ready set.
	if err := st.MarkPushDispatched(ctx, urgent.ID, 2*time.Minute); err != nil {
		t.Fatalf("MarkPushDispatched: %v", err)
	}
	ready, _ = st.PushReadyRuns(ctx, "serverless", 10)
	if len(ready) != 1 || ready[0].ID != older.ID {
		t.Fatalf("held run still listed as ready: %v", ids(ready))
	}
	// A second dispatch attempt on the held run is a conflict.
	if err := st.MarkPushDispatched(ctx, urgent.ID, time.Minute); err != store.ErrConflict {
		t.Fatalf("expected ErrConflict re-dispatching a held run, got %v", err)
	}

	// The receiver claims it: SCHEDULED -> PENDING with a lease.
	claimed, err := st.ClaimPushRun(ctx, urgent.ID, "recv-1", time.Minute)
	if err != nil {
		t.Fatalf("ClaimPushRun: %v", err)
	}
	if claimed.State != core.StatePending || claimed.WorkerID == nil || *claimed.WorkerID != "recv-1" {
		t.Fatalf("claim did not transition/assign: %+v", claimed)
	}
	if claimed.RunCount != 1 {
		t.Fatalf("attempt not bumped: %d", claimed.RunCount)
	}

	// ClearPushDispatch puts a merely-held (not claimed) run back.
	if err := st.MarkPushDispatched(ctx, older.ID, time.Minute); err != nil {
		t.Fatalf("hold older: %v", err)
	}
	if err := st.ClearPushDispatch(ctx, older.ID); err != nil {
		t.Fatalf("ClearPushDispatch: %v", err)
	}
	ready, _ = st.PushReadyRuns(ctx, "serverless", 10)
	if len(ready) != 1 || ready[0].ID != older.ID {
		t.Fatalf("cleared run not ready again: %v", ids(ready))
	}
}

func ids(rs []core.FlowRun) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}
