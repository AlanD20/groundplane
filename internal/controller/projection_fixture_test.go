package controller

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func normalizedProjectionArtifactFixture(
	t *testing.T,
	artifactID string,
	environmentID string,
	volumeDirectory string,
	canonicalYAML []byte,
	services []*agentpb.ComposeService,
	volumes []*agentpb.ComposeVolume,
) []byte {
	t.Helper()
	digest := sha256.Sum256(canonicalYAML)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, ProjectName: "gp-" + strings.ToLower(environmentID),
		CanonicalYaml: canonicalYAML, YamlSha256: digest[:], AuthorizedVolumeDir: volumeDirectory,
		Services: services, Volumes: volumes,
	})
	if err != nil {
		t.Fatalf("marshal normalized projection artifact: %v", err)
	}
	return encoded
}
