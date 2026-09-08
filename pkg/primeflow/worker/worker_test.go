package worker

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestRemoteNeedsBothVariables(t *testing.T) {
	for _, tc := range []struct {
		name string
		o    Options
		want bool
	}{
		{"neither", Options{}, false},
		{"url only", Options{APIURL: "https://x"}, false},
		{"token only", Options{WorkerToken: "pmx_x"}, false},
		{"both", Options{APIURL: "https://x", WorkerToken: "pmx_x"}, true},
	} {
		if got := tc.o.Remote(); got != tc.want {
			t.Errorf("%s: Remote() = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestApplyEnvDefaults(t *testing.T) {
	// Clear anything the ambient environment might set, so the defaults are
	// what is under test rather than the developer's shell.
	for _, k := range []string{
		"PRIMEFLOW_API_URL", "PRIMEFLOW_WORKER_TOKEN", "PRIMEFLOW_DATABASE_URL",
		"DATABASE_URL", "PRIMEFLOW_QUEUES", "PRIMEFLOW_CONCURRENCY",
		"PRIMEFLOW_LEASE", "PRIMEFLOW_POLL", "PRIMEFLOW_MAX_SUBFLOW_DEPTH",
		"PRIMEFLOW_METRICS_ADDR", "PRIMEFLOW_PUSH_ADDR",
	} {
		t.Setenv(k, "")
	}

	var o Options
	o.applyEnv()

	if len(o.Queues) != 1 || o.Queues[0] != "default" {
		t.Errorf("Queues = %v, want [default]", o.Queues)
	}
	if o.Concurrency != 4 {
		t.Errorf("Concurrency = %d, want 4", o.Concurrency)
	}
	if o.LeaseDuration != time.Minute {
		t.Errorf("LeaseDuration = %s, want 1m", o.LeaseDuration)
	}
	if o.MaxSubflowDepth != 8 {
		t.Errorf("MaxSubflowDepth = %d, want 8", o.MaxSubflowDepth)
	}
	if o.MetricsAddr != ":9090" {
		t.Errorf("MetricsAddr = %q, want :9090", o.MetricsAddr)
	}
	if o.Registry == nil || o.Logger == nil {
		t.Error("applyEnv left Registry or Logger nil")
	}
	// Beside the database the poll is the mechanism; over a WAN the wake-up
	// stream is, and the poll is only a backstop.
	if o.PollInterval != 2*time.Second {
		t.Errorf("local PollInterval = %s, want 2s", o.PollInterval)
	}

	remote := Options{APIURL: "https://x", WorkerToken: "pmx_x"}
	remote.applyEnv()
	if remote.PollInterval != 15*time.Second {
		t.Errorf("remote PollInterval = %s, want 15s", remote.PollInterval)
	}
}

func TestQueuesAreSplitAndTrimmed(t *testing.T) {
	t.Setenv("PRIMEFLOW_QUEUES", " site-a , site-b ,, site-c ")
	var o Options
	o.applyEnv()
	want := []string{"site-a", "site-b", "site-c"}
	if len(o.Queues) != len(want) {
		t.Fatalf("Queues = %v, want %v", o.Queues, want)
	}
	for i := range want {
		if o.Queues[i] != want[i] {
			t.Fatalf("Queues = %v, want %v", o.Queues, want)
		}
	}
}

func TestOpenWithoutAnyRouteExplainsBothOptions(t *testing.T) {
	for _, k := range []string{
		"PRIMEFLOW_API_URL", "PRIMEFLOW_WORKER_TOKEN",
		"PRIMEFLOW_DATABASE_URL", "DATABASE_URL",
	} {
		t.Setenv(k, "")
	}
	_, err := Open(context.Background(), Options{})
	if err == nil {
		t.Fatal("Open with no route succeeded")
	}
	for _, want := range []string{"PRIMEFLOW_DATABASE_URL", "PRIMEFLOW_API_URL", "PRIMEFLOW_WORKER_TOKEN"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

// TestPackageDoesNotLinkTheServer is the guard the split exists for. One import
// of pkg/primeflow — or of anything that reaches the API server — puts go-oidc,
// go-jose, the cron parser and the embedded time-zone database back into every
// site worker's binary, and nothing else in the build would complain.
func TestPackageDoesNotLinkTheServer(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Skipf("go tool unavailable: %v", err)
	}
	banned := []string{
		"internal/server",
		"internal/scheduler",
		"internal/automations",
		"internal/oidcauth",
		"internal/apiauth",
		"internal/authn",
		"internal/ratelimit",
		"pkg/primeflow", // the parent package, which links all of the above
	}
	for _, line := range strings.Split(string(out), "\n") {
		pkg := strings.TrimSpace(line)
		if !strings.HasPrefix(pkg, "github.com/primex/primeflow/") {
			continue
		}
		suffix := strings.TrimPrefix(pkg, "github.com/primex/primeflow/")
		for _, b := range banned {
			if suffix == b {
				t.Errorf("this package must not link %s — see the package doc", pkg)
			}
		}
	}
}
