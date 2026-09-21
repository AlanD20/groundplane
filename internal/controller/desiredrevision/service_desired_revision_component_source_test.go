package desiredrevision

import (
	"reflect"
	"testing"

	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
)

func TestCloneEnvironmentDesiredProjectionPreservesManagedComponentRuntimeSources(t *testing.T) {
	// Rationale: desired-only edits must preserve immutable teardown authority
	// for every previously rendered managed Component source.
	t.Parallel()
	sources := []testenvironmentprojection.ManagedComponentRuntimeSource{
		{
			ComponentKind: "caddy", ComponentID: "cmp-caddy", ServiceID: "svc-caddy",
			ComposeName: "caddy", RevisionID: "task-caddy", ArtifactID: "cfg-caddy",
			ArtifactSHA256: "sha-caddy",
		},
		{
			ComponentKind: "cloudflare-tunnel", ComponentID: "cmp-tunnel", ServiceID: "svc-tunnel",
			ComposeName: "cloudflare-tunnel", RevisionID: "task-tunnel", ArtifactID: "cfg-tunnel",
			ArtifactSHA256: "sha-tunnel",
		},
	}
	current := testenvironmentprojection.EnvironmentComposeProjection{ManagedComponentRuntimeSources: sources}

	result := CloneProjection(current)
	if !reflect.DeepEqual(result.ManagedComponentRuntimeSources, sources) {
		t.Fatalf("cloned managed Component sources = %#v, want %#v", result.ManagedComponentRuntimeSources, sources)
	}
	result.ManagedComponentRuntimeSources[0].ArtifactID = "changed"
	if current.ManagedComponentRuntimeSources[0].ArtifactID != "cfg-caddy" {
		t.Fatal("cloned managed Component sources alias the input slice")
	}
}
