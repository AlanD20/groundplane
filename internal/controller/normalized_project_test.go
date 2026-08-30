package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestLoadNormalizedEnvironmentProjectDoesNotResolveEnvFiles(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "service.env")
	if err := os.WriteFile(path, []byte("EXPANDED=${APP_NAME}\n"), 0o600); err != nil {
		t.Fatalf("write env file: %v", err)
	}
	canonical := []byte("services:\n  api:\n    image: example/api:1\n    env_file:\n      - " + path + "\n")
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId:    "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		OwnerKind:     agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:       "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ProjectName:   "groundplane-test",
		CanonicalYaml: canonical,
	})
	if err != nil {
		t.Fatalf("marshal Compose artifact: %v", err)
	}
	project, err := loadNormalizedEnvironmentProject(context.Background(), etcd.EnvironmentComposeProjection{
		EnvironmentID:     "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ComposeArtifact:   artifact,
		NormalizedCompose: canonical,
	})
	if err != nil {
		t.Fatalf("loadNormalizedEnvironmentProject() error = %v", err)
	}
	service := project.Services["api"]
	if _, resolved := service.Environment["EXPANDED"]; resolved {
		t.Fatalf("loadNormalizedEnvironmentProject() resolved env_file contents: %#v", service.Environment)
	}
}

// Rationale: execution metadata can be stale independently of the immutable
// authored stream, so it must never decide which Services direct mutations own.
func TestNormalizedEnvironmentArtifactUsesNormalizedComposeServiceIdentity(t *testing.T) {
	t.Parallel()
	const (
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		apiID         = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		generatedID   = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	)
	canonical := []byte("services:\n  api:\n    image: example/api:1\n")
	runtime, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId:  "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:     environmentID,
		ProjectName: "groundplane-test",
		Services: []*agentpb.ComposeService{
			{ServiceId: apiID, ComposeName: "api", OwnerComponentId: "cmp_stale"},
			{ServiceId: generatedID, ComposeName: "router"},
		},
	})
	if err != nil {
		t.Fatalf("marshal runtime artifact: %v", err)
	}
	artifact, err := NormalizedEnvironmentArtifact(etcd.EnvironmentComposeProjection{
		EnvironmentID: environmentID, ComposeArtifact: runtime, NormalizedCompose: canonical,
		Services: []etcd.EnvironmentComposeIdentity{
			{ID: apiID, Name: "api"},
			{ID: generatedID, Name: "router"},
		},
	})
	if err != nil {
		t.Fatalf("NormalizedEnvironmentArtifact() error = %v", err)
	}
	if len(artifact.Services) != 1 || artifact.Services[0].GetServiceId() != apiID ||
		artifact.Services[0].GetComposeName() != "api" {
		t.Fatalf("NormalizedEnvironmentArtifact() Services = %#v", artifact.Services)
	}
}
