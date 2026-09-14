package backingendpoint

import "testing"

// QA: ATT-12; pure canonical-identity proof, not live Docker DNS or authentication proof.
// Rationale: endpoint identities must remain stable for one backing Service and differ for two same-kind instances.
func TestNewUsesStableServiceIdentity(t *testing.T) {
	first := New("svc_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	second := New("svc_01ARZ3NDEKTSV4RRFFQ69G5FAW")
	if first != "gp-svc-01arz3ndektsv4rrffq69g5fav" || second != "gp-svc-01arz3ndektsv4rrffq69g5faw" ||
		first == second {
		t.Fatalf("New() = %q/%q", first, second)
	}
}
