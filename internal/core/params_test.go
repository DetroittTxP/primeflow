package core_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/DetroittTxP/primeflow/internal/core"
)

// The wire shape a worker publishes for a flow like:
//
//	type AddParams struct {
//	    A     int      `json:"a"`
//	    B     float64  `json:"b"`
//	    Name  string   `json:"name"`
//	    Dry   bool     `json:"dry"`
//	    Hosts []string `json:"hosts"`
//	    When  time.Time `json:"when"`
//	}
const demoSchema = `{"fields":[
	{"name":"a","type":"integer","required":true},
	{"name":"b","type":"number","required":true},
	{"name":"name","type":"string","required":true},
	{"name":"dry","type":"boolean","required":false},
	{"name":"hosts","type":"array","required":false},
	{"name":"when","type":"object","required":false}
]}`

func TestValidateParamsRejectsWhatTheDecoderWould(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params string
		want   string // substring of the problem, empty means accept
	}{
		{"string for an integer", `{"a":"not-a-number"}`, "a must be integer, got string"},
		{"float for an integer", `{"a":1.5}`, "a must be integer, got number"},
		{"exponent for an integer", `{"a":1e3}`, "a must be integer, got number"},
		{"number for a string", `{"name":42}`, "name must be string, got integer"},
		{"string for a boolean", `{"dry":"yes"}`, "dry must be boolean, got string"},
		{"string for an array", `{"hosts":"web-01"}`, "hosts must be array, got string"},
		{"object for an array", `{"hosts":{"0":"web-01"}}`, "hosts must be array, got object"},
		{"parameters that are not an object", `[1,2,3]`, "parameters must be a JSON object"},

		{"the happy path", `{"a":1,"b":2.5,"name":"acme","dry":true,"hosts":["web-01"]}`, ""},
		{"an integer for a number field", `{"b":2}`, ""},
		{"a missing field, even a required one", `{"b":1}`, ""},
		{"an unknown key, as an automation injects", `{"a":1,"_event":{"run_id":"x"}}`, ""},
		{"null for any type", `{"a":null,"name":null,"hosts":null}`, ""},
		{"a string for an object field, as a time.Time travels", `{"when":"2026-09-08T00:00:00Z"}`, ""},
		{"no parameters at all", ``, ""},
		{"null parameters", `null`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := core.ValidateParams(json.RawMessage(demoSchema), json.RawMessage(tc.params))
			if tc.want == "" {
				if err != nil {
					t.Fatalf("want accepted, got %v", err)
				}
				return
			}
			var ip core.ErrInvalidParams
			if !errors.As(err, &ip) {
				t.Fatalf("want ErrInvalidParams, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// A flow that published no schema, or a schema this build cannot read, must not
// have its runs rejected: the caller cannot fix either.
func TestValidateParamsWithoutAUsableSchema(t *testing.T) {
	for _, schema := range []string{``, `null`, `{}`, `{"fields":[]}`, `not json at all`} {
		if err := core.ValidateParams(json.RawMessage(schema),
			json.RawMessage(`{"a":"anything","b":[1,2]}`)); err != nil {
			t.Fatalf("schema %q: %v", schema, err)
		}
	}
}

// Every problem is reported at once, so a caller fixing a form sees the whole
// list rather than one field per round trip.
func TestValidateParamsReportsEveryProblem(t *testing.T) {
	err := core.ValidateParams(json.RawMessage(demoSchema),
		json.RawMessage(`{"a":"x","name":1,"hosts":"web-01"}`))
	var ip core.ErrInvalidParams
	if !errors.As(err, &ip) {
		t.Fatalf("want ErrInvalidParams, got %v", err)
	}
	if len(ip.Problems) != 3 {
		t.Fatalf("want 3 problems, got %d: %v", len(ip.Problems), ip.Problems)
	}
}
