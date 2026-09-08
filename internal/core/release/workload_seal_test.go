package release

import (
	"strings"
	"testing"
)

func testWorkloadSeal() WorkloadSeal {
	return WorkloadSeal{
		RequestedReference: "registry.invalid/app:sha-123",
		LocalImageID:       "sha256:" + strings.Repeat("a", 64),
		ReplicaCount:       1,
	}
}

// Rationale: immutable Releases require explicit positive replica counts and
// exact local Docker ids; registry metadata cannot substitute for local bytes.
func TestWorkloadSealRequiresExactLocalImageAndCount(t *testing.T) {
	seal := WorkloadSeal{
		RequestedReference: "app:dev",
		LocalImageID:       "sha256:" + strings.Repeat("a", 64),
		ReplicaCount:       2,
	}
	if err := ValidateWorkloadSeal(seal); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []WorkloadSeal{
		{RequestedReference: seal.RequestedReference, LocalImageID: seal.LocalImageID},
		{RequestedReference: "app", LocalImageID: seal.LocalImageID, ReplicaCount: 1},
		{RequestedReference: seal.RequestedReference, LocalImageID: "app@sha256:" + strings.Repeat("a", 64), ReplicaCount: 1},
		{RequestedReference: seal.RequestedReference, LocalImageID: "sha256:" + strings.Repeat("A", 64), ReplicaCount: 1},
	} {
		if ValidateWorkloadSeal(invalid) == nil {
			t.Fatalf("accepted invalid seal %#v", invalid)
		}
	}
}
