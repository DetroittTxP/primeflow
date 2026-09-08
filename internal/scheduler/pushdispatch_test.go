package scheduler

import "testing"

func TestPushSignature(t *testing.T) {
	body := []byte(`{"run_id":"r-123","flow_name":"provision-vm"}`)
	sig := "sha256=" + signPush("shhh", body)

	if !VerifyPushSignature("shhh", sig, body) {
		t.Fatal("valid signature rejected")
	}
	if VerifyPushSignature("shhh", sig, []byte(`{"run_id":"other"}`)) {
		t.Fatal("signature accepted for a different body")
	}
	if VerifyPushSignature("different-secret", sig, body) {
		t.Fatal("signature accepted under the wrong secret")
	}
	if VerifyPushSignature("shhh", "sha256=not-hex", body) {
		t.Fatal("malformed signature accepted")
	}
	if VerifyPushSignature("shhh", "md5=deadbeef", body) {
		t.Fatal("wrong algo prefix accepted")
	}
	// An unset secret means the receiver trusts any dispatch (network-isolated).
	if !VerifyPushSignature("", "sha256=whatever", body) {
		t.Fatal("empty secret should accept unconditionally")
	}
}
