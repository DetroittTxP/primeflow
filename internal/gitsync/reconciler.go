package gitsync

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/DetroittTxP/primeflow/internal/core"
	"github.com/DetroittTxP/primeflow/internal/events"
	"github.com/DetroittTxP/primeflow/internal/store"
)

// Reconciler pushes auto-sync worker specs whose rendered manifests have
// drifted from what was last committed. It is leader-elected (role "gitsync")
// so only one replica writes to the repo.
type Reconciler struct {
	store  store.Store
	engine *Engine
	events *events.Emitter
	log    *slog.Logger
	holder string
	every  time.Duration
}

// NewReconciler builds the auto-sync loop. every <= 0 defaults to 2 minutes.
func NewReconciler(st store.Store, engine *Engine, ev *events.Emitter, log *slog.Logger, holder string, every time.Duration) *Reconciler {
	if every <= 0 {
		every = 2 * time.Minute
	}
	if log == nil {
		log = slog.Default()
	}
	return &Reconciler{store: st, engine: engine, events: ev, log: log, holder: holder, every: every}
}

// Run ticks until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) error {
	t := time.NewTicker(r.every)
	defer t.Stop()
	r.log.Info("gitops reconciler starting", "interval", r.every)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			lead, err := r.store.AcquireLeadership(ctx, "gitsync", r.holder, 2*r.every)
			if err != nil {
				r.log.Warn("gitsync leader election failed", "err", err)
				continue
			}
			if !lead {
				continue
			}
			r.tick(ctx)
		}
	}
}

func (r *Reconciler) tick(ctx context.Context) {
	// Cheap guard: nothing to do until a repo is configured.
	if gc, err := r.store.GetGitConnection(ctx); err != nil || gc.RepoURL == "" {
		return
	}
	specs, err := r.store.WorkerSpecsForAutoSync(ctx)
	if err != nil {
		r.log.Warn("gitsync: list auto-sync specs failed", "err", err)
		return
	}
	gc, _, err := r.store.GitConnectionWithToken(ctx)
	if err != nil {
		r.log.Warn("gitsync: read git connection failed", "err", err)
		return
	}
	for i := range specs {
		spec := specs[i]
		files, err := Render(spec, gc)
		if err != nil {
			r.log.Warn("gitsync: render failed", "spec", spec.Name, "err", err)
			continue
		}
		if TreeHash(files) == spec.LastSyncedHash && spec.SyncState == core.SyncSynced {
			continue // already converged
		}
		res, err := r.engine.Sync(ctx, spec)
		if err != nil {
			r.log.Warn("gitsync: sync failed", "spec", spec.Name, "err", err)
		}
		if serr := r.store.SetWorkerSpecSyncResult(ctx, spec.ID, res); serr != nil {
			r.log.Warn("gitsync: persist result failed", "spec", spec.Name, "err", serr)
			continue
		}
		if err == nil && r.events != nil {
			payload, _ := json.Marshal(map[string]any{
				"name": spec.Name, "commit_sha": res.CommitSHA, "auto": true,
			})
			r.events.Emit(ctx, "worker-spec.synced", "worker-spec", spec.ID, payload)
		}
	}
}
