package pushsig

import "testing"

func TestSignVerify(t *testing.T) {
	body := []byte(`{"run_id":"r-123","flow_name":"provision-vm"}`)
	sig := Sign("shhh", body)

	if !Verify("shhh", sig, body) {
		t.Fatal("valid signature rejected")
	}
	if Verify("shhh", sig, []byte(`{"run_id":"other"}`)) {
		t.Fatal("signature accepted for a different body")
	}
	if Verify("different-secret", sig, body) {
		t.Fatal("signature accepted under the wrong secret")
	}
	if Verify("shhh", "sha256=not-hex", body) {
		t.Fatal("malformed signature accepted")
	}
	if Verify("shhh", "md5=deadbeef", body) {
		t.Fatal("wrong algo prefix accepted")
	}
	if Verify("shhh", "", body) {
		t.Fatal("empty signature accepted under a secret")
	}
	// An unset secret means the receiver trusts any dispatch (network-isolated).
	if !Verify("", "sha256=whatever", body) {
		t.Fatal("empty secret should accept unconditionally")
	}
}

func TestSignIsPrefixed(t *testing.T) {
	got := Sign("k", []byte("b"))
	if len(got) != len("sha256=")+64 {
		t.Fatalf("Sign returned %q; want the sha256= prefix and 64 hex characters", got)
	}
}
