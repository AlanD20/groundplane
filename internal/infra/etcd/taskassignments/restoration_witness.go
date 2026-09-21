package taskassignments

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// openRestorationWitness opens only the captured canonical applied artifact;
// it never reads or substitutes a newer Environment projection.
func OpenRestorationWitness(environmentID string, encoded []byte) (*agentpb.ComposeArtifact, error) {
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(encoded, artifact) != nil || executionplan.RejectUnknown(artifact) != nil {
		return nil, CorruptTaskAssignment()
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, encoded) ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || artifact.GetOwnerId() != environmentID {
		return nil, CorruptTaskAssignment()
	}
	return artifact, nil
}
