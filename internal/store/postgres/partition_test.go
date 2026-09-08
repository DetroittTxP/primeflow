package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/primex/primeflow/internal/core"
)

func TestLogPartitionMaintenance(t *testing.T) {
	st := newStore(t)
	ctx := context.Background()
	run := mkRun(t, st, "default", 50, time.Now())

	// 0004 makes pf_logs partitioned.
	var kind string
	if err := st.DB().QueryRowContext(ctx,
		`SELECT relkind FROM pg_class WHERE relname='pf_logs'`).Scan(&kind); err != nil {
		t.Fatalf("relkind: %v", err)
	}
	if kind != "p" {
		t.Fatalf("pf_logs relkind = %q, want partitioned 'p'", kind)
	}

	// EnsureLogPartitions is idempotent and extends coverage forward.
	for i := 0; i < 2; i++ {
		if err := st.EnsureLogPartitions(ctx, 6); err != nil {
			t.Fatalf("EnsureLogPartitions call %d: %v", i, err)
		}
	}
	// A write 5 months out now lands in a real partition.
	future := time.Now().AddDate(0, 5, 0).UTC()
	if _, err := st.DB().ExecContext(ctx,
		`INSERT INTO pf_logs (flow_run_id, level, message, ts) VALUES ($1,'INFO','future',$2)`,
		run.ID, future); err != nil {
		t.Fatalf("insert into a future partition: %v", err)
	}

	// A cutoff before all coverage drops nothing.
	if dropped, err := st.DropLogPartitionsOlderThan(ctx, time.Now().AddDate(-5, 0, 0)); err != nil {
		t.Fatalf("drop (none expected): %v", err)
	} else if len(dropped) != 0 {
		t.Fatalf("expected nothing dropped, got %v", dropped)
	}

	// Create a genuinely-old month partition (three months back) with a log in
	// it, then a cutoff that leaves its whole range behind.
	old := time.Date(time.Now().Year(), time.Now().Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -3, 0)
	oldName := "pf_logs_" + old.Format("2006_01")
	// It may already exist (newStore seeds a window); drop any prior copy first.
	st.DB().ExecContext(ctx, `DROP TABLE IF EXISTS `+oldName)
	if _, err := st.DB().ExecContext(ctx,
		`CREATE TABLE `+oldName+` PARTITION OF pf_logs FOR VALUES FROM ('`+old.Format("2006-01-02")+`') TO ('`+old.AddDate(0, 1, 0).Format("2006-01-02")+`')`); err != nil {
		// If it overlaps p0 (fresh DB where p0 still covers the past), the drop
		// path is exercised on p0 instead below — skip creating our own.
		t.Logf("could not create %s (%v); testing drop on p0 instead", oldName, err)
	} else {
		st.DB().ExecContext(ctx,
			`INSERT INTO pf_logs (flow_run_id, level, message, ts) VALUES ($1,'INFO','old',$2)`,
			run.ID, old.AddDate(0, 0, 5))
	}

	cutoff := old.AddDate(0, 2, 0) // still in the past, but past 'old'/p0 bounds
	dropped, err := st.DropLogPartitionsOlderThan(ctx, cutoff)
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if len(dropped) == 0 {
		t.Fatalf("expected at least one partition dropped for cutoff %s", cutoff)
	}
	for _, name := range dropped {
		var n int
		st.DB().QueryRowContext(ctx, `SELECT count(*) FROM pg_class WHERE relname=$1`, name).Scan(&n)
		if n != 0 {
			t.Fatalf("partition %s still present after drop", name)
		}
	}

	// The future log survives.
	logs, _ := st.ListLogs(ctx, run.ID, 0, 100)
	hasFuture := false
	for _, l := range logs {
		if l.Message == "future" {
			hasFuture = true
		}
	}
	if !hasFuture {
		t.Fatalf("future log not retained after drop: %+v", logs)
	}

	// Restore current coverage and confirm writes still work.
	if err := st.EnsureLogPartitions(ctx, 2); err != nil {
		t.Fatalf("EnsureLogPartitions after drop: %v", err)
	}
	if err := st.AppendLogs(ctx, []core.LogRecord{
		{FlowRunID: run.ID, Level: "INFO", Message: "now", Timestamp: time.Now().UTC()},
	}); err != nil {
		t.Fatalf("AppendLogs after maintenance: %v", err)
	}
}
