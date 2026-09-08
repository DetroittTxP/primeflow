package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// ErrInvalidParams reports parameters that the flow's own decoder would refuse.
// It is a client error: the run is rejected while the caller is still on the
// phone rather than dispatched to a worker that will burn a slot to discover
// the same thing.
type ErrInvalidParams struct {
	Problems []string
}

func (e ErrInvalidParams) Error() string {
	return "invalid parameters: " + strings.Join(e.Problems, "; ")
}

// paramsSchema mirrors the wire shape a worker publishes for a flow (see
// sdk.ParamsSchemaDoc). The JSON is the contract, not the Go type, so this
// package decodes it itself instead of importing the SDK.
type paramsSchema struct {
	Fields []paramField `json:"fields"`
}

type paramField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ValidateParams checks params against the schema a flow published and returns
// ErrInvalidParams naming every field that would not decode.
//
// It rejects exactly what the worker's json.Unmarshal would reject and nothing
// more, so no run that works today starts failing at enqueue:
//
//   - unknown keys pass. An automation with pass_event injects `_event`, and a
//     flow may accept more than it declares.
//   - a missing field passes, even one the schema marks required. That flag is
//     reflection-derived from "not a pointer, no omitempty", which says nothing
//     about whether the flow needs a value: collect-metering declares `orgs`
//     required and defaults it to every org when absent.
//   - null passes for every type, because Go decodes null into any field as a
//     no-op.
//   - object-typed fields are not type-checked. That is the schema builder's
//     catch-all bucket, and a time.Time lands in it but travels as a string.
//
// What is left is the mistake worth catching: a string where the flow wants an
// integer, an object where it wants a list.
func ValidateParams(schema, params json.RawMessage) error {
	if len(schema) == 0 || len(params) == 0 {
		return nil
	}
	var doc paramsSchema
	if err := json.Unmarshal(schema, &doc); err != nil || len(doc.Fields) == 0 {
		// An unreadable schema is the worker's problem, not the caller's.
		return nil
	}
	var byName map[string]json.RawMessage
	if err := json.Unmarshal(params, &byName); err != nil {
		if isJSONNull(params) {
			return nil
		}
		// Nothing but an object decodes into a parameter struct.
		return ErrInvalidParams{Problems: []string{
			fmt.Sprintf("parameters must be a JSON object, got %s", jsonTypeOf(params))}}
	}
	var problems []string
	for _, f := range doc.Fields {
		raw, ok := byName[f.Name]
		if !ok || isJSONNull(raw) {
			continue
		}
		if got := jsonTypeOf(raw); !acceptsJSONType(f.Type, got) {
			problems = append(problems, fmt.Sprintf("%s must be %s, got %s", f.Name, f.Type, got))
		}
	}
	if len(problems) > 0 {
		return ErrInvalidParams{Problems: problems}
	}
	return nil
}

// jsonTypeOf names a value's JSON type the way the schema names it, so the two
// compare directly. Numbers split into integer and number because the flow's
// decoder splits them too: strconv rejects "1.5" and "1e3" for an int field.
func jsonTypeOf(raw json.RawMessage) string {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return "invalid"
	}
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case []any:
		return "array"
	case float64:
		if bytes.ContainsAny(bytes.TrimSpace(raw), ".eE") {
			return "number"
		}
		return "integer"
	default:
		return "object"
	}
}

// acceptsJSONType reports whether a value of JSON type got decodes into a
// schema field of type want.
func acceptsJSONType(want, got string) bool {
	switch want {
	case "string", "boolean", "array", "integer":
		return got == want
	case "number":
		return got == "number" || got == "integer"
	default:
		// "object" is the catch-all the schema builder falls back to for maps,
		// structs, interfaces and anything else, so it constrains nothing.
		return true
	}
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
