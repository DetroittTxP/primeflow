package sdk

import "testing"

type demoParams struct {
	OrgName  string   `json:"org_name"`
	CPU      int      `json:"cpu,omitempty"`
	Ratio    float64  `json:"ratio"`
	Dry      bool     `json:"dry_run"`
	Note     *string  `json:"note,omitempty"`
	Hosts    []string `json:"hosts"`
	Internal string   `json:"-"`
	unexp    string
}

func TestBuildParamsSchema(t *testing.T) {
	doc := BuildParamsSchema(demoParams{OrgName: "acme", CPU: 4})
	by := map[string]ParamField{}
	for _, f := range doc.Fields {
		by[f.Name] = f
	}

	if _, ok := by["-"]; ok {
		t.Fatal(`field tagged json:"-" must be skipped`)
	}
	if _, ok := by["unexp"]; ok {
		t.Fatal("unexported field must be skipped")
	}
	if by["org_name"].Type != "string" || !by["org_name"].Required {
		t.Fatalf("org_name: %+v", by["org_name"])
	}
	if by["cpu"].Type != "integer" || by["cpu"].Required {
		t.Fatalf("cpu is omitempty so not required: %+v", by["cpu"])
	}
	if by["ratio"].Type != "number" {
		t.Fatalf("ratio type: %s", by["ratio"].Type)
	}
	if by["dry_run"].Type != "boolean" {
		t.Fatalf("dry_run type: %s", by["dry_run"].Type)
	}
	if by["note"].Required {
		t.Fatal("pointer field must not be required")
	}
	if by["hosts"].Type != "array" {
		t.Fatalf("hosts type: %s", by["hosts"].Type)
	}
	if by["org_name"].Example != "acme" || by["cpu"].Example.(int) != 4 {
		t.Fatalf("example values not surfaced: %+v %+v", by["org_name"], by["cpu"])
	}
}

func TestBuildParamsSchemaNonStruct(t *testing.T) {
	if len(BuildParamsSchema(nil).Fields) != 0 || len(BuildParamsSchema("x").Fields) != 0 {
		t.Fatal("nil / non-struct should yield an empty schema")
	}
}
