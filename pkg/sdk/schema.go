package sdk

import (
	"encoding/json"
	"reflect"
	"strings"
)

// ParamsSchemaDoc is the compact, one-level description of a flow's parameter
// struct that the console's Flows page renders as a quick-run form. It is not a
// full JSON Schema — just enough to build sensible inputs.
type ParamsSchemaDoc struct {
	Fields []ParamField `json:"fields"`
}

// ParamField is one parameter.
type ParamField struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // string | integer | number | boolean | array | object
	Required bool   `json:"required"`
	Example  any    `json:"example,omitempty"`
}

// ParamsSchema attaches a parameter schema to a flow, derived by reflection from
// a zero (or example) value of the flow's parameter struct:
//
//	sdk.Flow("provision-vm", provisionVM, sdk.ParamsSchema(ProvisionParams{}))
//
// Pass a populated value to also surface example values in the form.
func ParamsSchema(example any) FlowOption {
	doc := BuildParamsSchema(example)
	raw, _ := json.Marshal(doc)
	return func(f *FlowDef) { f.ParamsSchema = raw }
}

// BuildParamsSchema reflects over v (a struct or pointer to one) and returns its
// schema. A nil or non-struct value yields an empty schema.
func BuildParamsSchema(v any) ParamsSchemaDoc {
	var doc ParamsSchemaDoc
	if v == nil {
		return doc
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			rv = reflect.New(rv.Type().Elem()).Elem()
		} else {
			rv = rv.Elem()
		}
	}
	if rv.Kind() != reflect.Struct {
		return doc
	}
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)
		if !sf.IsExported() {
			continue
		}
		tag := sf.Tag.Get("json")
		name := sf.Name
		omitempty := false
		if tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] == "-" {
				continue
			}
			if parts[0] != "" {
				name = parts[0]
			}
			for _, p := range parts[1:] {
				if p == "omitempty" {
					omitempty = true
				}
			}
		}
		ft := sf.Type
		required := true
		if ft.Kind() == reflect.Pointer {
			required = false
			ft = ft.Elem()
		}
		if omitempty {
			required = false
		}
		field := ParamField{Name: name, Type: kindToSchemaType(ft.Kind()), Required: required}
		if fv := rv.Field(i); fv.IsValid() && !fv.IsZero() {
			field.Example = fv.Interface()
		}
		doc.Fields = append(doc.Fields, field)
	}
	return doc
}

func kindToSchemaType(k reflect.Kind) string {
	switch k {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Slice, reflect.Array:
		return "array"
	default:
		return "object"
	}
}
