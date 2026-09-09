package gitsync

import (
	"strings"
	"testing"

	"github.com/DetroittTxP/primeflow/internal/core"
)

func baseSpec() core.WorkerSpec {
	return core.WorkerSpec{
		Name: "primex-worker-3", Image: "primex/primeflow:v1.2.3",
		Queues: []string{"default", "vcd"}, Concurrency: 6, Replicas: 2,
		Delivery: "git", Namespace: "primeflow",
	}
}

func TestRepoPath(t *testing.T) {
	s := baseSpec()
	if got := RepoPath(s, core.GitConnection{}); got != "workers/primex-worker-3" {
		t.Fatalf("default path = %q", got)
	}
	if got := RepoPath(s, core.GitConnection{BasePath: "clusters/prod/workers/"}); got != "clusters/prod/workers/primex-worker-3" {
		t.Fatalf("base_path path = %q", got)
	}
	s.RepoPath = "/apps/w3/"
	if got := RepoPath(s, core.GitConnection{BasePath: "ignored"}); got != "apps/w3" {
		t.Fatalf("explicit path = %q", got)
	}
}

func TestRenderGitBundle(t *testing.T) {
	files, err := Render(baseSpec(), core.GitConnection{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"workers/primex-worker-3/secret.yaml",
		"workers/primex-worker-3/deployment.yaml",
		"workers/primex-worker-3/kustomization.yaml",
	}
	for _, p := range want {
		if _, ok := files[p]; !ok {
			t.Fatalf("missing %s (got %v)", p, keys(files))
		}
	}
	if len(files) != 3 {
		t.Fatalf("git delivery should render 3 files, got %v", keys(files))
	}
	sec := string(files["workers/primex-worker-3/secret.yaml"])
	if !strings.Contains(sec, `PRIMEFLOW_QUEUES: "default,vcd"`) ||
		!strings.Contains(sec, `PRIMEFLOW_CONCURRENCY: "6"`) ||
		!strings.Contains(sec, `PRIMEFLOW_WORKER_NAME: "primex-worker-3"`) {
		t.Fatalf("secret.yaml missing derived env:\n%s", sec)
	}
	if !strings.Contains(sec, "REPLACE_ME") {
		t.Fatalf("secret.yaml should keep the placeholder DB password:\n%s", sec)
	}
	dep := string(files["workers/primex-worker-3/deployment.yaml"])
	if !strings.Contains(dep, "replicas: 2") ||
		!strings.Contains(dep, "image: primex/primeflow:v1.2.3") ||
		!strings.Contains(dep, "/usr/local/bin/primex-worker") {
		t.Fatalf("deployment.yaml wrong:\n%s", dep)
	}
}

// A GitOps-delivered worker runs the same distroless image as the shipped
// manifests, whose USER is a name: runAsNonRoot without a numeric uid is
// rejected by kubelet with CreateContainerConfigError and never starts.
func TestRenderedDeploymentRunsAsANonRootUID(t *testing.T) {
	files, _ := Render(baseSpec(), core.GitConnection{})
	dep := string(files["workers/primex-worker-3/deployment.yaml"])
	for _, want := range []string{
		"runAsNonRoot: true",
		"runAsUser: 65532",
		"runAsGroup: 65532",
		"readOnlyRootFilesystem: true",
	} {
		if !strings.Contains(dep, want) {
			t.Errorf("deployment.yaml missing %q:\n%s", want, dep)
		}
	}
}

func TestRenderEnvOverride(t *testing.T) {
	s := baseSpec()
	s.Env = map[string]string{"PRIMEFLOW_CONCURRENCY": "12", "EXTRA_FLAG": "yes"}
	files, _ := Render(s, core.GitConnection{})
	sec := string(files["workers/primex-worker-3/secret.yaml"])
	if !strings.Contains(sec, `PRIMEFLOW_CONCURRENCY: "12"`) {
		t.Fatalf("override not applied:\n%s", sec)
	}
	if strings.Contains(sec, `PRIMEFLOW_CONCURRENCY: "6"`) {
		t.Fatalf("stale default left in:\n%s", sec)
	}
	if !strings.Contains(sec, `EXTRA_FLAG: "yes"`) {
		t.Fatalf("extra env not appended:\n%s", sec)
	}
}

func TestRenderArgoAndFlux(t *testing.T) {
	s := baseSpec()
	s.Delivery = "argocd"
	s.AutoSync = true
	s.ArgoCD = &core.WorkerSpecArgoCD{Project: "platform", Revision: "main"}
	files, _ := Render(s, core.GitConnection{RepoURL: "https://github.com/acme/gitops.git"})
	app, ok := files["workers/primex-worker-3/application.yaml"]
	if !ok {
		t.Fatalf("argocd delivery missing application.yaml: %v", keys(files))
	}
	if !strings.Contains(string(app), "project: platform") ||
		!strings.Contains(string(app), "repoURL: https://github.com/acme/gitops.git") ||
		!strings.Contains(string(app), "automated:") {
		t.Fatalf("application.yaml wrong:\n%s", app)
	}

	s.Delivery = "flux"
	s.AutoSync = false
	files, _ = Render(s, core.GitConnection{})
	fk, ok := files["workers/primex-worker-3/flux-kustomization.yaml"]
	if !ok {
		t.Fatalf("flux delivery missing flux-kustomization.yaml: %v", keys(files))
	}
	if !strings.Contains(string(fk), "suspend: true") {
		t.Fatalf("auto_sync off should suspend the Flux Kustomization:\n%s", fk)
	}
}

func TestTreeHashStable(t *testing.T) {
	a, _ := Render(baseSpec(), core.GitConnection{})
	b, _ := Render(baseSpec(), core.GitConnection{})
	if TreeHash(a) != TreeHash(b) {
		t.Fatal("TreeHash not deterministic for identical input")
	}
	s := baseSpec()
	s.Concurrency = 7
	c, _ := Render(s, core.GitConnection{})
	if TreeHash(a) == TreeHash(c) {
		t.Fatal("TreeHash unchanged after a manifest change")
	}
	// order independence: rebuild the map in a different insertion order
	reordered := map[string][]byte{}
	ks := keys(a)
	for i := len(ks) - 1; i >= 0; i-- {
		reordered[ks[i]] = a[ks[i]]
	}
	if TreeHash(a) != TreeHash(reordered) {
		t.Fatal("TreeHash depends on map iteration order")
	}
}

func keys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
