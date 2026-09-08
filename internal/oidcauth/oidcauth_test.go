package oidcauth

import (
	"reflect"
	"testing"
)

func TestParseRoleMap(t *testing.T) {
	got := ParseRoleMap("platform-admins=admin, ops = operator ,,bad")
	want := map[string]string{"platform-admins": "admin", "ops": "operator"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseRoleMap = %v, want %v", got, want)
	}
	if len(ParseRoleMap("")) != 0 {
		t.Fatal("empty string should yield an empty map")
	}
}

func TestMapRole(t *testing.T) {
	rm := map[string]string{"admins": "admin", "ops": "operator", "readers": "viewer"}

	cases := []struct {
		name   string
		claims map[string]any
		claim  string
		want   string
	}{
		{"no claim configured", map[string]any{"groups": []any{"admins"}}, "", "viewer"},
		{"string claim maps", map[string]any{"role": "ops"}, "role", "operator"},
		{"[]any claim, most privileged wins", map[string]any{"groups": []any{"readers", "admins", "ops"}}, "groups", "admin"},
		{"[]string claim", map[string]any{"groups": []string{"ops"}}, "groups", "operator"},
		{"unmapped groups fall back to default", map[string]any{"groups": []any{"randos"}}, "groups", "viewer"},
		{"missing claim falls back", map[string]any{}, "groups", "viewer"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := MapRole(c.claims, c.claim, rm, "viewer"); got != c.want {
				t.Fatalf("MapRole = %q, want %q", got, c.want)
			}
		})
	}
}

func TestConfigEnabled(t *testing.T) {
	if (Config{}).Enabled() {
		t.Fatal("empty config must not be enabled")
	}
	if (Config{Issuer: "https://x"}).Enabled() {
		t.Fatal("issuer alone is not enough")
	}
	if !(Config{Issuer: "https://x", ClientID: "c"}).Enabled() {
		t.Fatal("issuer + client id enables OIDC")
	}
}
