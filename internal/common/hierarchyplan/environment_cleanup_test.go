package hierarchyplan

import (
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestEnvironmentCleanupRemovesComposeProjectBeforeDirectory(t *testing.T) {
	yaml := []byte("services:\n  app:\n    image: example.invalid/app:1\n")
	digest := sha256.Sum256(yaml)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:          "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ProjectName:         "gp-env_01arz3ndektsv4rrffq69g5fav",
		CanonicalYaml:       yaml,
		YamlSha256:          digest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := EnvironmentCleanup(
		"plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", 1, artifact.OwnerId,
		"step_01ARZ3NDEKTSV4RRFFQ69G5FAV", "step_01ARZ3NDEKTSV4RRFFQ69G5FAW", 300,
		artifact.AuthorizedVolumeDir, encoded,
	)
	if err != nil {
		t.Fatalf("EnvironmentCleanup() error = %v", err)
	}
	if len(plan.Artifacts) != 1 || len(plan.Steps) != 2 ||
		!plan.Steps[0].GetComposeRemove().GetWholeProject() ||
		plan.Steps[1].GetEnvironmentDirectoryRemove().GetEnvironmentId() != artifact.OwnerId {
		t.Fatalf("EnvironmentCleanup() plan = %#v", plan)
	}
}
