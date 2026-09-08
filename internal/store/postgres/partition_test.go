package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"slices"
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

	// 0004 attaches the pre-partitioning table as pf_logs_p0 spanning
	// (MINVALUE, start of the month after the migration). On a freshly migrated
	// database that bound is still ahead of "now", so p0 is where the current
	// month's rows live and no cutoff inside its range may retire it — only one
	// past its bound, which is how the janitor eventually drops it. Check both
	// sides of the bound; doing so also leaves the table in the steady-state
	// monthly layout for the rest of the test. p0 is absent once an earlier run
	// has retired it (newStore seeds monthly partitions in its place).
	var p0Hi sql.NullTime
	err := st.DB().QueryRowContext(ctx, `
SELECT substring(pg_get_expr(c.relpartbound, c.oid) from $re$TO \('([^']+)'\)$re$)::timestamptz
  FROM pg_class c WHERE c.relname = 'pf_logs_p0'`).Scan(&p0Hi)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Already retired by an earlier run.
	case err != nil:
		t.Fatalf("p0 bound: %v", err)
	case !p0Hi.Valid:
		t.Fatalf("could not parse the upper bound of pf_logs_p0")
	default:
		hi := p0Hi.Time.UTC()
		if dropped, err := st.DropLogPartitionsOlderThan(ctx, hi.Add(-time.Second)); err != nil {
			t.Fatalf("drop inside p0's range: %v", err)
		} else if len(dropped) != 0 {
			t.Fatalf("cutoff inside p0's range (TO %s) dropped %v, want nothing", hi, dropped)
		}
		dropped, err := st.DropLogPartitionsOlderThan(ctx, hi.Add(time.Second))
		if err != nil {
			t.Fatalf("drop past p0's range: %v", err)
		}
		if !slices.Equal(dropped, []string{"pf_logs_p0"}) {
			t.Fatalf("cutoff just past p0's range (TO %s) dropped %v, want [pf_logs_p0]", hi, dropped)
		}
	}

	// Create a genuinely-old month partition (three months back) with a log in
	// it, then a cutoff that leaves its whole range behind.
	now := time.Now().UTC()
	old := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -3, 0)
	oldName := "pf_logs_" + old.Format("2006_01")
	// newStore may already have seeded it; start from a known copy.
	if _, err := st.DB().ExecContext(ctx, `DROP TABLE IF EXISTS `+oldName); err != nil {
		t.Fatalf("drop prior %s: %v", oldName, err)
	}
	if _, err := st.DB().ExecContext(ctx,
		`CREATE TABLE `+oldName+` PARTITION OF pf_logs FOR VALUES FROM ('`+old.Format("2006-01-02")+`') TO ('`+old.AddDate(0, 1, 0).Format("2006-01-02")+`')`); err != nil {
		t.Fatalf("create %s: %v", oldName, err)
	}
	if _, err := st.DB().ExecContext(ctx,
		`INSERT INTO pf_logs (flow_run_id, level, message, ts) VALUES ($1,'INFO','old',$2)`,
		run.ID, old.AddDate(0, 0, 5)); err != nil {
		t.Fatalf("insert into %s: %v", oldName, err)
	}

	cutoff := old.AddDate(0, 2, 0) // still in the past, and past the whole of 'old'
	dropped, err := st.DropLogPartitionsOlderThan(ctx, cutoff)
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if !slices.Contains(dropped, oldName) {
		t.Fatalf("cutoff %s dropped %v, want %s among them", cutoff, dropped, oldName)
	}
	for _, name := range dropped {
		var n int
		st.DB().QueryRowContext(ctx, `SELECT count(*) FROM pg_class WHERE relname=$1`, name).Scan(&n)
		if n != 0 {
			t.Fatalf("partition %s still present after drop", name)
		}
	}

	// The old log went with its partition; the future log survives.
	logs, err := st.ListLogs(ctx, run.ID, 0, 100)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	var msgs []string
	for _, l := range logs {
		msgs = append(msgs, l.Message)
	}
	if slices.Contains(msgs, "old") || !slices.Contains(msgs, "future") {
		t.Fatalf("logs after drop = %v, want 'future' retained and 'old' gone", msgs)
	}

	// Restore current coverage (retiring p0 took this month's with it) and
	// confirm writes still work.
	if err := st.EnsureLogPartitions(ctx, 2); err != nil {
		t.Fatalf("EnsureLogPartitions after drop: %v", err)
	}
	if err := st.AppendLogs(ctx, []core.LogRecord{
		{FlowRunID: run.ID, Level: "INFO", Message: "now", Timestamp: time.Now().UTC()},
	}); err != nil {
		t.Fatalf("AppendLogs after maintenance: %v", err)
	}
}
