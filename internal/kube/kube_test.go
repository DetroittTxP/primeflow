package kube

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DetroittTxP/primeflow/internal/core"
)

var testRef = RunRef{ID: "0f8b1e2c-1111-2222-3333-444455556666", FlowName: "Provision VM", WorkQueue: "vcd", Attempt: 2}

func testTemplate() Template {
	return Template{Image: "registry.example.com/primeflow:latest", RunAsUser: 65532, TTL: time.Minute}
}

// dig walks a rendered object, failing the test if the path is missing.
func dig(t *testing.T, obj map[string]any, path ...string) any {
	t.Helper()
	var cur any = obj
	for i, key := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("%s is not an object", strings.Join(path[:i], "."))
		}
		cur, ok = m[key]
		if !ok {
			t.Fatalf("%s is missing", strings.Join(path[:i+1], "."))
		}
	}
	return cur
}

// The two settings that keep a run from executing twice. Kubernetes retrying
// the pod would re-enter a flow under a lease already counted as one attempt,
// which is PrimeFlow's job and only PrimeFlow's.
func TestJobNeverRetriesItsOwnPod(t *testing.T) {
	job := testTemplate().Job("pf-x", testRef, nil)
	if got := dig(t, job, "spec", "backoffLimit"); got != 0 {
		t.Errorf("backoffLimit = %v, want 0", got)
	}
	if got := dig(t, job, "spec", "template", "spec", "restartPolicy"); got != "Never" {
		t.Errorf("restartPolicy = %v, want Never", got)
	}
}

func TestJobCarriesTheRunIdentity(t *testing.T) {
	tmpl := testTemplate()
	tmpl.Env = map[string]string{"PRIMEFLOW_QUEUES": "ignored-by-a-child", "EXTRA": "kept"}
	injected := map[string]string{"PRIMEFLOW_RUN_ID": testRef.ID, "PRIMEFLOW_QUEUES": "wins"}

	job := tmpl.Job("pf-x", testRef, injected)
	env := map[string]string{}
	for _, e := range dig(t, job, "spec", "template", "spec", "containers").([]any)[0].(map[string]any)["env"].([]any) {
		kv := e.(map[string]any)
		env[kv["name"].(string)] = kv["value"].(string)
	}
	if env["PRIMEFLOW_RUN_ID"] != testRef.ID {
		t.Errorf("run id = %q, want %q", env["PRIMEFLOW_RUN_ID"], testRef.ID)
	}
	if env["EXTRA"] != "kept" {
		t.Error("the template's own environment was dropped")
	}
	// The launcher names the run; a template must not be able to rename it.
	if env["PRIMEFLOW_QUEUES"] != "wins" {
		t.Errorf("injected variable = %q, want it to win over the template", env["PRIMEFLOW_QUEUES"])
	}

	labels := dig(t, job, "metadata", "labels").(map[string]any)
	if labels["primeflow.io/run-id"] != testRef.ID {
		t.Errorf("run-id label = %v", labels["primeflow.io/run-id"])
	}
	if labels["primeflow.io/flow"] != "provision-vm" {
		t.Errorf("flow label = %v, want the sanitised flow name", labels["primeflow.io/flow"])
	}
}

// runAsNonRoot without a uid is rejected by admission for an image whose USER
// is a name, and the run then looks stuck rather than broken.
func TestJobRunsAsANonRootUID(t *testing.T) {
	sec := dig(t, testTemplate().Job("pf-x", testRef, nil), "spec", "template", "spec", "securityContext").(map[string]any)
	if sec["runAsNonRoot"] != true {
		t.Error("pod does not require a non-root user")
	}
	if sec["runAsUser"] != int64(65532) {
		t.Errorf("runAsUser = %v, want 65532", sec["runAsUser"])
	}
}

func TestPodSpecPatchOverridesTheRenderedSpec(t *testing.T) {
	tmpl := testTemplate()
	tmpl.PodSpecPatch = map[string]any{
		"tolerations":                   []any{map[string]any{"key": "flows"}},
		"terminationGracePeriodSeconds": float64(600),
	}
	spec := dig(t, tmpl.Job("pf-x", testRef, nil), "spec", "template", "spec").(map[string]any)
	if spec["tolerations"] == nil {
		t.Error("patch did not reach the pod spec")
	}
	if spec["terminationGracePeriodSeconds"] != float64(600) {
		t.Error("patch value was not applied")
	}
	if spec["restartPolicy"] != "Never" {
		t.Error("patch clobbered the rendered spec")
	}
}

func TestJobNameIsAValidUniqueDNSLabel(t *testing.T) {
	seen := map[string]bool{}
	for range 50 {
		name := JobName(RunRef{ID: testRef.ID, FlowName: "Provision VM — very long name/with punctuation"})
		if len(name) > 63 {
			t.Fatalf("name %q is %d chars", name, len(name))
		}
		for _, r := range name {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-') {
				t.Fatalf("name %q holds %q", name, r)
			}
		}
		if seen[name] {
			t.Fatalf("name %q was generated twice; a retry would collide", name)
		}
		seen[name] = true
	}
}

func TestTemplateFromEnvNeedsAnImage(t *testing.T) {
	t.Setenv("PRIMEFLOW_KUBE_IMAGE", "")
	if _, err := TemplateFromEnv(); err == nil {
		t.Fatal("a template with no image was accepted")
	}
	t.Setenv("PRIMEFLOW_KUBE_IMAGE", "img:1")
	t.Setenv("PRIMEFLOW_KUBE_TTL", "nonsense")
	if _, err := TemplateFromEnv(); err == nil {
		t.Fatal("an unparseable duration was accepted")
	}
	t.Setenv("PRIMEFLOW_KUBE_TTL", "30s")
	t.Setenv("PRIMEFLOW_KUBE_ENV_FROM_SECRET", " worker-env , extra ")
	t.Setenv("PRIMEFLOW_KUBE_NODE_SELECTOR", "disk=ssd")
	tmpl, err := TemplateFromEnv()
	if err != nil {
		t.Fatalf("TemplateFromEnv: %v", err)
	}
	if len(tmpl.EnvFromSecrets) != 2 || tmpl.EnvFromSecrets[0] != "worker-env" {
		t.Errorf("EnvFromSecrets = %v", tmpl.EnvFromSecrets)
	}
	if tmpl.NodeSelector["disk"] != "ssd" {
		t.Errorf("NodeSelector = %v", tmpl.NodeSelector)
	}
	if tmpl.TTL != 30*time.Second {
		t.Errorf("TTL = %s", tmpl.TTL)
	}
}

// ---------------------------------------------------------------- launcher --

// fakeAPI is as much of the Job API as the launcher uses.
type fakeAPI struct {
	mu       sync.Mutex
	status   map[string]any // status returned by GET
	created  map[string]any
	deleted  []string
	getCalls int
	jobs     []string          // what LIST returns
	labels   map[string]string // job name -> run id label
}

func newFakeAPI() (*fakeAPI, *httptest.Server) {
	f := &fakeAPI{status: map[string]any{"active": 1}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /apis/batch/v1/namespaces/test/jobs", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.created = body
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "pf-x"}})
	})
	mux.HandleFunc("GET /apis/batch/v1/namespaces/test/jobs/{name}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		st, n := f.status, f.getCalls
		f.getCalls = n + 1
		f.mu.Unlock()
		if st == nil {
			http.Error(w, `{"reason":"NotFound"}`, http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": st})
	})
	mux.HandleFunc("GET /apis/batch/v1/namespaces/test/jobs", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		items := make([]any, 0, len(f.jobs))
		for _, name := range f.jobs {
			items = append(items, map[string]any{"metadata": map[string]any{
				"name":   name,
				"labels": map[string]any{"primeflow.io/run-id": f.labels[name]},
			}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	})
	mux.HandleFunc("DELETE /apis/batch/v1/namespaces/test/jobs/{name}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.deleted = append(f.deleted, r.PathValue("name"))
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	})
	return f, httptest.NewServer(mux)
}

func (f *fakeAPI) setStatus(s map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = s
}

func (f *fakeAPI) deletes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

type fakeRuns struct {
	mu  sync.Mutex
	run core.FlowRun
}

func (f *fakeRuns) GetFlowRun(context.Context, string) (*core.FlowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.run
	return &r, nil
}

func (f *fakeRuns) setState(s core.StateType) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.run.State = s
}

func testLauncher(t *testing.T, f *fakeAPI, srv *httptest.Server, runs RunReader, deadline time.Duration) *Launcher {
	t.Helper()
	c, err := New(Config{APIServer: srv.URL, Namespace: "test", Token: "t"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return NewLauncher(c, testTemplate(), runs, "worker-1",
		func(runID string) map[string]string { return map[string]string{"PRIMEFLOW_RUN_ID": runID} },
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		LauncherOptions{Poll: 5 * time.Millisecond, StartDeadline: deadline})
}

func testRun() *core.FlowRun {
	return &core.FlowRun{ID: testRef.ID, FlowName: "add-numbers", WorkQueue: "default", State: core.StatePending}
}

func TestLaunchBlocksUntilTheJobSucceeds(t *testing.T) {
	f, srv := newFakeAPI()
	defer srv.Close()
	runs := &fakeRuns{run: *testRun()}
	runs.setState(core.StateRunning)
	l := testLauncher(t, f, srv, runs, time.Minute)

	go func() {
		time.Sleep(30 * time.Millisecond)
		f.setStatus(map[string]any{"succeeded": 1,
			"conditions": []any{map[string]any{"type": "Complete", "status": "True"}}})
	}()
	if err := l.Launch(context.Background(), testRun()); err != nil {
		t.Fatalf("Launch of a job that completed = %v, want nil", err)
	}
	if f.created == nil {
		t.Error("no job was created")
	}
}

func TestLaunchReportsAFailedJob(t *testing.T) {
	f, srv := newFakeAPI()
	defer srv.Close()
	runs := &fakeRuns{run: *testRun()}
	runs.setState(core.StateRunning)
	f.setStatus(map[string]any{"failed": 1, "conditions": []any{
		map[string]any{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded", "message": "pod died"}}})

	err := testLauncher(t, f, srv, runs, time.Minute).Launch(context.Background(), testRun())
	if err == nil {
		t.Fatal("a failed job returned nil; the run would look settled")
	}
	if !strings.Contains(err.Error(), "BackoffLimitExceeded") {
		t.Errorf("error %q does not say why", err)
	}
}

// A pod that cannot pull its image leaves a Job that is neither running nor
// failed. Without a deadline the run would stay leased and renewed forever.
func TestLaunchGivesUpOnAPodThatNeverStarts(t *testing.T) {
	f, srv := newFakeAPI()
	defer srv.Close()
	// Still PENDING, but with a started_at — which is what a leased run looks
	// like, because the lease statement stamps it. Reading that as "the pod is
	// up" is what let an unpullable image hold a lease indefinitely.
	stamped := testRun()
	now := time.Now().UTC()
	stamped.StartedAt = &now
	runs := &fakeRuns{run: *stamped}

	err := testLauncher(t, f, srv, runs, 20*time.Millisecond).Launch(context.Background(), testRun())
	if err == nil {
		t.Fatal("Launch waited forever on a pod that never started")
	}
	if !strings.Contains(err.Error(), "did not start") {
		t.Errorf("error %q does not name the cause", err)
	}
	if len(f.deletes()) == 0 {
		t.Error("the job that never started was left behind")
	}
}

// A launcher that is replaced leaves its Jobs behind. Those whose runs have
// since finished are litter — including the pathological one, a Job retrying an
// image pull that will never succeed for a run the janitor has already failed.
func TestSweepDeletesJobsOfFinishedRunsOnly(t *testing.T) {
	f, srv := newFakeAPI()
	defer srv.Close()
	f.jobs = []string{"pf-done-1", "pf-live-2"}
	f.labels = map[string]string{"pf-done-1": "run-done", "pf-live-2": "run-live"}

	c, err := New(Config{APIServer: srv.URL, Namespace: "test", Token: "t"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runs := &statesByID{state: map[string]core.StateType{
		"run-done": core.StateFailed,
		"run-live": core.StateRunning,
	}}
	if n := SweepFinished(context.Background(), c, runs, slog.New(slog.NewTextHandler(io.Discard, nil))); n != 1 {
		t.Errorf("swept %d jobs, want 1", n)
	}
	deleted := f.deletes()
	if len(deleted) != 1 || deleted[0] != "pf-done-1" {
		t.Errorf("deleted %v, want only the finished run's job", deleted)
	}
}

// statesByID answers for several runs, which the sweep needs and one run's
// worth of fake does not.
type statesByID struct{ state map[string]core.StateType }

func (s *statesByID) GetFlowRun(_ context.Context, id string) (*core.FlowRun, error) {
	return &core.FlowRun{ID: id, State: s.state[id]}, nil
}

func TestCancelDeletesTheJob(t *testing.T) {
	f, srv := newFakeAPI()
	defer srv.Close()
	runs := &fakeRuns{run: *testRun()}
	runs.setState(core.StateRunning)
	l := testLauncher(t, f, srv, runs, time.Minute)

	done := make(chan error, 1)
	go func() { done <- l.Launch(context.Background(), testRun()) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		l.mu.Lock()
		tracked := l.jobs[testRef.ID] != ""
		l.mu.Unlock()
		if tracked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job was never tracked")
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !l.Cancel(testRef.ID) {
		t.Fatal("Cancel of a running job returned false")
	}
	if len(f.deletes()) == 0 {
		t.Fatal("Cancel did not delete the job")
	}
	// The pod settles the run itself; here the Job simply goes away.
	runs.setState(core.StateCancelled)
	f.setStatus(nil)
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Launch after a cancellation = %v, want nil (the run was settled)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Launch did not return after its job was deleted")
	}
	if l.Cancel(testRef.ID) {
		t.Error("Cancel of a finished run returned true")
	}
}
