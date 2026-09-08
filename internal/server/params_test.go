package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/pkg/sdk"
)

type addParams struct {
	A     int      `json:"a"`
	B     int      `json:"b"`
	Note  string   `json:"note,omitempty"`
	Hosts []string `json:"hosts,omitempty"`
}

// registerFlow publishes a flow the way a booting worker does, schema and all.
func registerFlow(t *testing.T, a *api, name string, schemaOf any) {
	t.Helper()
	raw, err := json.Marshal(sdk.BuildParamsSchema(schemaOf))
	if err != nil {
		t.Fatalf("build schema: %v", err)
	}
	if err := a.store.UpsertFlow(context.Background(), &core.Flow{
		ID: uuid.NewString(), Name: name, Version: "1", ParamsSchema: raw,
	}); err != nil {
		t.Fatalf("upsert flow: %v", err)
	}
}

// Parameters the flow's decoder would refuse must cost an HTTP 400, not a
// dispatch: before this, the run was queued, leased and only then FAILED with
// "cannot unmarshal string into Go struct field".
func TestCreateRunRejectsParametersTheFlowCannotDecode(t *testing.T) {
	a := newAPI(t)
	registerFlow(t, a, "add-demo", addParams{})

	rec := a.call(http.MethodPost, "/api/v1/runs", map[string]any{
		"flow_name":  "add-demo",
		"parameters": map[string]any{"a": "not-a-number", "b": 3},
	}, testWorker)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "a must be integer") {
		t.Fatalf("error should name the field and the type: %s", rec.Body.String())
	}

	// And nothing was enqueued.
	var list struct {
		Total int `json:"total"`
	}
	a.ok(http.MethodGet, "/api/v1/runs?flow=add-demo", nil, &list)
	if list.Total != 0 {
		t.Fatalf("rejected run must not reach the queue, found %d", list.Total)
	}
}

// The validator only rejects what the decoder would. Everything the engine
// tolerates today has to keep working, or a fix for a bad parameter becomes an
// outage for every flow that leans on a default.
func TestCreateRunAcceptsWhatTheEngineTolerates(t *testing.T) {
	a := newAPI(t)
	registerFlow(t, a, "add-demo", addParams{})

	for _, tc := range []struct {
		name   string
		flow   string
		params map[string]any
	}{
		{"well-formed parameters", "add-demo", map[string]any{"a": 1, "b": 2}},
		{"a missing field the schema calls required", "add-demo", map[string]any{"a": 1}},
		{"the key an automation injects", "add-demo", map[string]any{
			"a": 1, "b": 2, "_event": map[string]any{"run_id": "x"}}},
		{"no parameters at all", "add-demo", nil},
		{"a flow no worker has registered yet", "not-published-yet",
			map[string]any{"anything": []int{1, 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"flow_name": tc.flow}
			if tc.params != nil {
				body["parameters"] = tc.params
			}
			rec := a.call(http.MethodPost, "/api/v1/runs", body, testWorker)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("want 202, got %d %s", rec.Code, rec.Body.String())
			}
		})
	}
}

// Triggering a deployment goes through the same gate, which is the path the
// console's quick-run form and every webhook actually take.
func TestTriggerDeploymentValidatesParameters(t *testing.T) {
	a := newAPI(t)
	registerFlow(t, a, "add-demo", addParams{})

	var dep core.Deployment
	a.ok(http.MethodPost, "/api/v1/deployments", map[string]any{
		"name": "add-demo-standard", "flow_name": "add-demo", "work_queue": "default",
	}, &dep)

	rec := a.call(http.MethodPost, "/api/v1/deployments/"+dep.ID+"/run", map[string]any{
		"parameters": map[string]any{"hosts": "web-01"},
	}, testWorker)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "hosts must be array") {
		t.Fatalf("error should name the field and the type: %s", rec.Body.String())
	}

	rec = a.call(http.MethodPost, "/api/v1/deployments/"+dep.ID+"/run", map[string]any{
		"parameters": map[string]any{"a": 1, "b": 2, "hosts": []string{"web-01"}},
	}, testWorker)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202, got %d %s", rec.Code, rec.Body.String())
	}
}
