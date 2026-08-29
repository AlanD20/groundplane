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
		EnvironmentID:   "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ComposeArtifact: artifact,
	})
	if err != nil {
		t.Fatalf("loadNormalizedEnvironmentProject() error = %v", err)
	}
	service := project.Services["api"]
	if _, resolved := service.Environment["EXPANDED"]; resolved {
		t.Fatalf("loadNormalizedEnvironmentProject() resolved env_file contents: %#v", service.Environment)
	}
}
