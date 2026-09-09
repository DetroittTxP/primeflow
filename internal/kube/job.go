package kube

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Template is the pod a run gets. It is read from the launcher's environment,
// one launcher per work queue — the shape the deployment docs already
// recommend — so "which image runs this lane's flows, with what limits and what
// service account" is answered by the launcher's own manifest rather than by a
// row in the database.
//
// PodSpecPatch is the escape hatch for everything not modelled: its keys are
// merged over the rendered pod spec, so tolerations, affinity, volumes or an
// init container need no change here.
type Template struct {
	Image            string
	Command          []string
	Args             []string
	ServiceAccount   string
	ImagePullSecrets []string
	EnvFromSecrets   []string
	EnvFromConfigMap []string
	Env              map[string]string
	NodeSelector     map[string]string
	Labels           map[string]string
	Annotations      map[string]string

	CPURequest    string
	CPULimit      string
	MemoryRequest string
	MemoryLimit   string

	// RunAsUser is set alongside runAsNonRoot because a distroless image whose
	// USER is a name rather than a uid fails admission with runAsNonRoot alone —
	// the pod is rejected before it starts, and the run looks stuck rather than
	// broken. 65532 is the nonroot uid the shipped image uses. Zero omits it.
	RunAsUser int64

	// TTL is ttlSecondsAfterFinished: how long a settled Job's record survives
	// for an operator to look at. The run's own history is in PrimeFlow, so
	// this only needs to outlast a `kubectl describe`.
	TTL time.Duration
	// ActiveDeadline bounds a pod that neither finishes nor is cancelled. Zero
	// leaves it to the run's own timeout, which the engine enforces.
	ActiveDeadline time.Duration

	PodSpecPatch map[string]any

	// Namespace and StartDeadline belong to the launcher rather than to the
	// pod, but they are read here so that every PRIMEFLOW_KUBE_* variable is
	// read in one function. Namespace empty means the launcher's own.
	Namespace     string
	StartDeadline time.Duration
}

// TemplateFromEnv reads the launcher's configuration.
func TemplateFromEnv() (Template, error) {
	t := Template{
		Image:            os.Getenv("PRIMEFLOW_KUBE_IMAGE"),
		Command:          splitList(os.Getenv("PRIMEFLOW_KUBE_COMMAND")),
		Args:             splitList(os.Getenv("PRIMEFLOW_KUBE_ARGS")),
		ServiceAccount:   os.Getenv("PRIMEFLOW_KUBE_SERVICE_ACCOUNT"),
		ImagePullSecrets: splitList(os.Getenv("PRIMEFLOW_KUBE_IMAGE_PULL_SECRETS")),
		EnvFromSecrets:   splitList(os.Getenv("PRIMEFLOW_KUBE_ENV_FROM_SECRET")),
		EnvFromConfigMap: splitList(os.Getenv("PRIMEFLOW_KUBE_ENV_FROM_CONFIGMAP")),
		Env:              splitPairs(os.Getenv("PRIMEFLOW_KUBE_ENV")),
		NodeSelector:     splitPairs(os.Getenv("PRIMEFLOW_KUBE_NODE_SELECTOR")),
		Labels:           splitPairs(os.Getenv("PRIMEFLOW_KUBE_LABELS")),
		Annotations:      splitPairs(os.Getenv("PRIMEFLOW_KUBE_ANNOTATIONS")),
		CPURequest:       os.Getenv("PRIMEFLOW_KUBE_CPU_REQUEST"),
		CPULimit:         os.Getenv("PRIMEFLOW_KUBE_CPU_LIMIT"),
		MemoryRequest:    os.Getenv("PRIMEFLOW_KUBE_MEMORY_REQUEST"),
		MemoryLimit:      os.Getenv("PRIMEFLOW_KUBE_MEMORY_LIMIT"),
		RunAsUser:        65532,
		TTL:              10 * time.Minute,
		Namespace:        os.Getenv("PRIMEFLOW_KUBE_NAMESPACE"),
	}
	if t.Image == "" {
		return Template{}, fmt.Errorf("PRIMEFLOW_KUBE_IMAGE is required to run flows in Jobs")
	}
	if v := os.Getenv("PRIMEFLOW_KUBE_RUN_AS_USER"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return Template{}, fmt.Errorf("PRIMEFLOW_KUBE_RUN_AS_USER: %w", err)
		}
		t.RunAsUser = n
	}
	for _, d := range []struct {
		key string
		dst *time.Duration
	}{
		{"PRIMEFLOW_KUBE_TTL", &t.TTL},
		{"PRIMEFLOW_KUBE_ACTIVE_DEADLINE", &t.ActiveDeadline},
		{"PRIMEFLOW_KUBE_START_DEADLINE", &t.StartDeadline},
	} {
		if v := os.Getenv(d.key); v != "" {
			parsed, err := time.ParseDuration(v)
			if err != nil {
				return Template{}, fmt.Errorf("%s: %w", d.key, err)
			}
			*d.dst = parsed
		}
	}
	if path := os.Getenv("PRIMEFLOW_KUBE_POD_SPEC_PATCH"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return Template{}, fmt.Errorf("PRIMEFLOW_KUBE_POD_SPEC_PATCH: %w", err)
		}
		if err := json.Unmarshal(raw, &t.PodSpecPatch); err != nil {
			return Template{}, fmt.Errorf("PRIMEFLOW_KUBE_POD_SPEC_PATCH is not a JSON object: %w", err)
		}
	}
	return t, nil
}

// RunRef is what a Job needs to know about the run it exists for.
type RunRef struct {
	ID        string
	FlowName  string
	WorkQueue string
	Attempt   int
}

// Job renders the object to POST. env is what the container must be told on top
// of the template's own: which run, under whose lease, and that it is a child.
func (t Template) Job(name string, run RunRef, env map[string]string) map[string]any {
	labels := map[string]any{
		"app.kubernetes.io/name":       "primeflow-run",
		"app.kubernetes.io/managed-by": "primeflow",
		"primeflow.io/run-id":          run.ID,
		"primeflow.io/flow":            labelValue(run.FlowName),
		"primeflow.io/queue":           labelValue(run.WorkQueue),
	}
	for k, v := range t.Labels {
		labels[k] = v
	}

	container := map[string]any{
		"name":  "run",
		"image": t.Image,
		"env":   envList(env, t.Env),
		"securityContext": map[string]any{
			"allowPrivilegeEscalation": false,
			"readOnlyRootFilesystem":   true,
			"capabilities":             map[string]any{"drop": []any{"ALL"}},
		},
	}
	if len(t.Command) > 0 {
		container["command"] = strList(t.Command)
	}
	if len(t.Args) > 0 {
		container["args"] = strList(t.Args)
	}
	if from := envFrom(t.EnvFromSecrets, t.EnvFromConfigMap); len(from) > 0 {
		container["envFrom"] = from
	}
	if res := resources(t); len(res) > 0 {
		container["resources"] = res
	}

	security := map[string]any{
		"runAsNonRoot":   true,
		"seccompProfile": map[string]any{"type": "RuntimeDefault"},
	}
	if t.RunAsUser > 0 {
		security["runAsUser"] = t.RunAsUser
	}

	podSpec := map[string]any{
		// Never: PrimeFlow owns retries. A Job that restarted the pod itself
		// would run the flow again under a lease already counted as one
		// attempt, which is the one way to get a run executed twice.
		"restartPolicy":   "Never",
		"containers":      []any{container},
		"securityContext": security,
	}
	if t.ServiceAccount != "" {
		podSpec["serviceAccountName"] = t.ServiceAccount
	}
	if len(t.ImagePullSecrets) > 0 {
		refs := make([]any, 0, len(t.ImagePullSecrets))
		for _, s := range t.ImagePullSecrets {
			refs = append(refs, map[string]any{"name": s})
		}
		podSpec["imagePullSecrets"] = refs
	}
	if len(t.NodeSelector) > 0 {
		podSpec["nodeSelector"] = anyMap(t.NodeSelector)
	}
	if t.ActiveDeadline > 0 {
		podSpec["activeDeadlineSeconds"] = int64(t.ActiveDeadline.Seconds())
	}
	for k, v := range t.PodSpecPatch {
		podSpec[k] = v
	}

	spec := map[string]any{
		// For the same reason as restartPolicy: one pod, one attempt.
		"backoffLimit": 0,
		"parallelism":  1,
		"completions":  1,
		"template": map[string]any{
			"metadata": map[string]any{"labels": labels},
			"spec":     podSpec,
		},
	}
	if t.TTL > 0 {
		spec["ttlSecondsAfterFinished"] = int64(t.TTL.Seconds())
	}

	meta := map[string]any{"name": name, "labels": labels}
	if len(t.Annotations) > 0 {
		meta["annotations"] = anyMap(t.Annotations)
	}
	return map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata":   meta,
		"spec":       spec,
	}
}

// JobName is unique per launch, never per run: a run that crashes and is leased
// again is a new attempt with a new pod, and a name that collided with the
// previous one would either be rejected or adopt a pod nobody is watching.
func JobName(run RunRef) string {
	return fmt.Sprintf("pf-%s-%s-%s", trim(labelValue(run.FlowName), 24), trim(run.ID, 8), suffix())
}

func suffix() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano()%100000, 36)
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

// labelValue makes a flow or queue name safe as a DNS label and as a label
// value: lowercase alphanumerics and dashes, starting and ending with one.
func labelValue(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.' || r == ' ':
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "flow"
	}
	return trim(out, 63)
}

func trim(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.Trim(s[:n], "-")
}

func envList(injected, extra map[string]string) []any {
	merged := make(map[string]string, len(injected)+len(extra))
	for k, v := range extra {
		merged[k] = v
	}
	// What the launcher injects wins: it is the run's identity.
	for k, v := range injected {
		merged[k] = v
	}
	out := make([]any, 0, len(merged))
	for _, k := range sorted(merged) {
		out = append(out, map[string]any{"name": k, "value": merged[k]})
	}
	return out
}

func envFrom(secrets, configMaps []string) []any {
	out := make([]any, 0, len(secrets)+len(configMaps))
	for _, s := range secrets {
		out = append(out, map[string]any{"secretRef": map[string]any{"name": s}})
	}
	for _, c := range configMaps {
		out = append(out, map[string]any{"configMapRef": map[string]any{"name": c}})
	}
	return out
}

func resources(t Template) map[string]any {
	req, lim := map[string]any{}, map[string]any{}
	for _, p := range []struct {
		key, val string
		dst      map[string]any
	}{
		{"cpu", t.CPURequest, req}, {"memory", t.MemoryRequest, req},
		{"cpu", t.CPULimit, lim}, {"memory", t.MemoryLimit, lim},
	} {
		if p.val != "" {
			p.dst[p.key] = p.val
		}
	}
	out := map[string]any{}
	if len(req) > 0 {
		out["requests"] = req
	}
	if len(lim) > 0 {
		out["limits"] = lim
	}
	return out
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func splitPairs(s string) map[string]string {
	out := map[string]string{}
	for _, p := range splitList(s) {
		k, v, ok := strings.Cut(p, "=")
		if ok {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func anyMap(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func strList(ss []string) []any {
	out := make([]any, 0, len(ss))
	for _, s := range ss {
		out = append(out, s)
	}
	return out
}

func sorted(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
