package component

import "testing"

// Rationale: replay validation must reject a changed service contract even
// when the managed file bytes are unchanged.
func TestDigestEnvironmentPlanIncludesServiceBehavior(t *testing.T) {
	base := EnvironmentPlan{
		Services: []ManagedService{{
			ID: "svc_resolver", Name: "resolver", Image: "example/resolver:1",
			Command: []string{"--config", "/etc/resolver/config"}, Restart: "unless-stopped",
			Replicas: 1, Mounts: []ManagedMount{{Source: "config", Target: "/etc/resolver/config", ReadOnly: true}},
		}},
		Files: []ManagedFile{{Path: "config", Content: []byte("same bytes\n")}},
	}
	changed := CloneEnvironmentPlan(base)
	changed.Services[0].Image = "example/resolver:2"
	if base.Files[0].Path != changed.Files[0].Path || string(base.Files[0].Content) != string(changed.Files[0].Content) {
		t.Fatal("replay fixture must retain identical file metadata and bytes")
	}
	if DigestEnvironmentPlan(base) == DigestEnvironmentPlan(changed) {
		t.Fatal("DigestEnvironmentPlan() ignored changed service behavior")
	}
}
