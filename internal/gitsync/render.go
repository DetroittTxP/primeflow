// Package gitsync renders worker specs to Kubernetes/GitOps manifests and
// pushes them to the configured Git repository. Rendering lives here (not in
// the browser) so the console preview, the "Sync now" button and the auto-sync
// reconciler all emit the same bytes.
package gitsync

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/DetroittTxP/primeflow/internal/core"
)

// RepoPath is where a spec's files live in the repo: its explicit RepoPath, or
// "<git base_path>/<name>" (base_path defaults to "workers").
func RepoPath(spec core.WorkerSpec, gc core.GitConnection) string {
	p := strings.Trim(strings.TrimSpace(spec.RepoPath), "/")
	if p != "" {
		return p
	}
	base := strings.Trim(strings.TrimSpace(gc.BasePath), "/")
	if base == "" {
		base = "workers"
	}
	return base + "/" + spec.Name
}

// envPairs is the worker's environment: fixed defaults (with placeholder
// secrets — the server never writes real credentials into git), then the
// spec's overrides applied in a deterministic order.
func envPairs(spec core.WorkerSpec) [][2]string {
	queues := strings.Join(spec.Queues, ",")
	if queues == "" {
		queues = "default"
	}
	out := [][2]string{
		{"PRIMEFLOW_DATABASE_URL", "postgres://primeflow:REPLACE_ME@POSTGRES_HOST:5432/primeflow?sslmode=disable"},
		{"PRIMEFLOW_NATS_URL", "nats://NATS_HOST:4222"},
		{"PRIMEFLOW_QUEUES", queues},
		{"PRIMEFLOW_CONCURRENCY", strconv.Itoa(spec.Concurrency)},
		{"PRIMEFLOW_WORKER_NAME", spec.Name},
	}
	idx := map[string]int{}
	for i, kv := range out {
		idx[kv[0]] = i
	}
	extra := make([]string, 0, len(spec.Env))
	for k := range spec.Env {
		extra = append(extra, k)
	}
	sort.Strings(extra)
	for _, k := range extra {
		if i, ok := idx[k]; ok {
			out[i][1] = spec.Env[k]
			continue
		}
		out = append(out, [2]string{k, spec.Env[k]})
	}
	return out
}

func yamlQuote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
}

func secretYAML(spec core.WorkerSpec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: v1\nkind: Secret\nmetadata:\n  name: %s-env\n  namespace: %s\ntype: Opaque\nstringData:\n",
		spec.Name, spec.Namespace)
	for _, kv := range envPairs(spec) {
		fmt.Fprintf(&b, "  %s: %s\n", kv[0], yamlQuote(kv[1]))
	}
	return b.String()
}

func deploymentYAML(spec core.WorkerSpec) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
  labels:
    app.kubernetes.io/name: %[1]s
    app.kubernetes.io/part-of: primeflow
    app.kubernetes.io/managed-by: primeflow
spec:
  replicas: %[3]d
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
      containers:
        - name: worker
          image: %[4]s
          command: ["/usr/local/bin/primex-worker"]
          envFrom:
            - secretRef:
                name: %[1]s-env
`, spec.Name, spec.Namespace, spec.Replicas, spec.Image)
}

func kustomizationYAML() string {
	return "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - secret.yaml\n  - deployment.yaml\n"
}

func argoApplicationYAML(spec core.WorkerSpec, gc core.GitConnection, path string) string {
	a := spec.ArgoCD
	if a == nil {
		a = &core.WorkerSpecArgoCD{}
	}
	project, server, rev := a.Project, a.DestServer, a.Revision
	if project == "" {
		project = "default"
	}
	if server == "" {
		server = "https://kubernetes.default.svc"
	}
	if rev == "" {
		rev = "HEAD"
	}
	sync := "  syncPolicy:\n    syncOptions: [CreateNamespace=true]\n"
	if spec.AutoSync {
		sync = "  syncPolicy:\n    automated:\n      prune: true\n      selfHeal: true\n    syncOptions: [CreateNamespace=true]\n"
	}
	return fmt.Sprintf(`apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: %[1]s
  namespace: argocd
spec:
  project: %[2]s
  source:
    repoURL: %[3]s
    path: %[4]s
    targetRevision: %[5]s
  destination:
    server: %[6]s
    namespace: %[7]s
%[8]s`, spec.Name, project, gc.RepoURL, path, rev, server, spec.Namespace, sync)
}

func fluxKustomizationYAML(spec core.WorkerSpec, path string) string {
	suspend := ""
	if !spec.AutoSync {
		suspend = "  suspend: true   # auto-sync OFF: flux resume / flux reconcile to apply\n"
	}
	return fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: %[1]s
  namespace: flux-system
spec:
  interval: 5m
  path: ./%[2]s
  prune: true
%[3]s  sourceRef:
    kind: GitRepository
    name: gitops
  targetNamespace: %[4]s
`, spec.Name, path, suspend, spec.Namespace)
}

// Render turns a spec into repo-relative path -> file bytes. delivery "git"
// emits the plain kustomize bundle; "argocd" / "flux" add their controller
// object. "script" (or anything unknown) still renders the bundle so the
// console can preview it, but is not pushed by the server.
func Render(spec core.WorkerSpec, gc core.GitConnection) (map[string][]byte, error) {
	if strings.TrimSpace(spec.Name) == "" {
		return nil, fmt.Errorf("worker spec has no name")
	}
	path := RepoPath(spec, gc)
	files := map[string][]byte{
		path + "/secret.yaml":        []byte(secretYAML(spec)),
		path + "/deployment.yaml":    []byte(deploymentYAML(spec)),
		path + "/kustomization.yaml": []byte(kustomizationYAML()),
	}
	switch spec.Delivery {
	case "argocd":
		files[path+"/application.yaml"] = []byte(argoApplicationYAML(spec, gc, path))
	case "flux":
		files[path+"/flux-kustomization.yaml"] = []byte(fluxKustomizationYAML(spec, path))
	}
	return files, nil
}

// TreeHash is a stable digest of a rendered file set: order-independent, so the
// reconciler can tell "same manifests" from "changed manifests" cheaply.
func TreeHash(files map[string][]byte) string {
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		h.Write([]byte(p))
		h.Write([]byte{0})
		h.Write(files[p])
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
