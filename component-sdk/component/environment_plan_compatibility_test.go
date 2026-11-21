package component

import (
	"fmt"
	"testing"
)

// Retained runtime proofs use this digest. The expected identity was produced
// by the pre-Entry SDK, so adding Environment credentials must not invalidate
// already acknowledged Secret-backed plans during an in-place upgrade.
func TestEnvironmentCredentialExtensionPreservesRetainedPlanIdentity(t *testing.T) {
	plan := EnvironmentPlan{Services: []ManagedService{{
		ID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", Name: "cloudflare-tunnel",
		Image: environmentPlanTestImage("example/tunnel"), Replicas: 1,
		SecretEnvironment: []ManagedSecretEnvironment{{
			Name: "TUNNEL_TOKEN", SecretID: "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		}},
	}}}
	want := "13c7e1753ccbf49e82256e1e056f00e1e1deb7bedf555e02cd00aae806eb8e20"
	if got := fmt.Sprintf("%x", DigestEnvironmentPlan(plan)); got != want {
		t.Fatalf("retained Secret-backed plan identity changed: %s", got)
	}
	entry := CloneEnvironmentPlan(plan)
	entry.Services[0].SecretEnvironment[0] = ManagedSecretEnvironment{
		Name: "TUNNEL_TOKEN", EntryID: "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV",
	}
	if DigestEnvironmentPlan(entry) == DigestEnvironmentPlan(plan) {
		t.Fatal("Environment Entry selection reused a different credential's proof")
	}
}
