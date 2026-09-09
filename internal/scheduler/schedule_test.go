package scheduler

import (
	"testing"
	"time"

	"github.com/DetroittTxP/primeflow/internal/core"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v.UTC()
}

func TestCronSchedule(t *testing.T) {
	now := mustTime(t, "2026-09-07T01:00:00Z")
	d := core.Deployment{
		ScheduleKind: core.ScheduleCron,
		Schedule:     "0 2 * * *", // 02:00 daily
		Timezone:     "UTC",
		CatchUp:      true,
	}
	got, err := NextRuns(d, now, now.Add(72*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 occurrences in 72h, got %d: %v", len(got), got)
	}
	if !got[0].Equal(mustTime(t, "2026-09-07T02:00:00Z")) {
		t.Fatalf("first occurrence wrong: %s", got[0])
	}
}

func TestCronRespectsTimezone(t *testing.T) {
	now := mustTime(t, "2026-09-07T00:00:00Z") // 07:00 in Bangkok
	d := core.Deployment{
		ScheduleKind: core.ScheduleCron,
		Schedule:     "0 9 * * *", // 09:00 Bangkok == 02:00 UTC
		Timezone:     "Asia/Bangkok",
	}
	got, err := NextRuns(d, now, now.Add(48*time.Hour), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("expected an occurrence")
	}
	if got[0].UTC().Hour() != 2 {
		t.Fatalf("expected 02:00 UTC, got %s", got[0].UTC())
	}
}

func TestCatchUpOffLimitsToOne(t *testing.T) {
	now := mustTime(t, "2026-09-07T01:00:00Z")
	d := core.Deployment{
		ScheduleKind: core.ScheduleCron,
		Schedule:     "0 * * * *", // hourly
		Timezone:     "UTC",
		CatchUp:      false,
	}
	got, err := NextRuns(d, now, now.Add(24*time.Hour), 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("catchup=false should materialise exactly one run, got %d", len(got))
	}
}

func TestIntervalSchedulePhaseIsStable(t *testing.T) {
	created := mustTime(t, "2026-09-07T00:00:00Z")
	d := core.Deployment{
		ScheduleKind: core.ScheduleInterval,
		Schedule:     "15m",
		CreatedAt:    created,
		CatchUp:      true,
	}
	// Whatever "now" is, the ticks stay on the creation-time phase rather than
	// drifting to whenever the process restarted.
	now := mustTime(t, "2026-09-07T03:07:00Z")
	got, err := NextRuns(d, now, now.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("expected occurrences")
	}
	for _, ts := range got {
		if ts.Minute()%15 != 0 || ts.Second() != 0 {
			t.Fatalf("interval drifted off phase: %s", ts)
		}
	}
	if !got[0].Equal(mustTime(t, "2026-09-07T03:15:00Z")) {
		t.Fatalf("first tick wrong: %s", got[0])
	}
}

func TestScheduleErrors(t *testing.T) {
	now := time.Now().UTC()
	cases := []core.Deployment{
		{ScheduleKind: core.ScheduleCron, Schedule: "not a cron"},
		{ScheduleKind: core.ScheduleInterval, Schedule: "wat"},
		{ScheduleKind: core.ScheduleInterval, Schedule: "100ms"}, // too short
		{ScheduleKind: core.ScheduleCron, Schedule: "0 2 * * *", Timezone: "Mars/Olympus"},
	}
	for i, d := range cases {
		if _, err := NextRuns(d, now, now.Add(time.Hour), 5); err == nil {
			t.Errorf("case %d: expected an error for schedule %q", i, d.Schedule)
		}
	}
}

func TestNoScheduleReturnsNothing(t *testing.T) {
	now := time.Now().UTC()
	got, err := NextRuns(core.Deployment{}, now, now.Add(time.Hour), 5)
	if err != nil || len(got) != 0 {
		t.Fatalf("expected no occurrences, got %v (err %v)", got, err)
	}
}
