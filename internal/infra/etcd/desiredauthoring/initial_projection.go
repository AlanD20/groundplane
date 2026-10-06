package desiredauthoring

import (
	"crypto/sha256"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func EmptyProjection(environment hierarchy.EnvironmentRecord, revisionID string) (environmentprojection.EnvironmentComposeProjection, error) {
	yaml := []byte("services: {}\nnetworks: {}\n")
	digest := sha256.Sum256(yaml)
	suffix := strings.TrimPrefix(revisionID, "task_")
	artifact := &agentpb.ComposeArtifact{ArtifactId: "cfg_" + suffix,
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: environment.ID,
		ProjectName: "gp-" + strings.ToLower(environment.ID), AuthorizedVolumeDir: environment.VolumeDir,
		CanonicalYaml: yaml, YamlSha256: digest[:]}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return environmentprojection.EnvironmentComposeProjection{}, err
	}
	value := environmentprojection.EnvironmentComposeProjection{EnvironmentID: environment.ID, RevisionID: revisionID,
		RenderGeneration: 1, ComposeArtifact: encoded, NormalizedCompose: yaml}
	return value, environmentprojection.ValidateEnvironmentComposeProjection(value)
}
