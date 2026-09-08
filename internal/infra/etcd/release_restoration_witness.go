package etcd

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// openRestorationWitness opens only the captured canonical applied artifact;
// it never reads or substitutes a newer Environment projection.
func openRestorationWitness(environmentID string, encoded []byte) (*agentpb.ComposeArtifact, error) {
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(encoded, artifact) != nil || executionplan.RejectUnknown(artifact) != nil {
		return nil, corruptTaskAssignment()
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, encoded) ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || artifact.GetOwnerId() != environmentID {
		return nil, corruptTaskAssignment()
	}
	return artifact, nil
}

func validateRestorationMemberWitness(authority ReleaseRestorationAuthority) error {
	if len(authority.NativePredecessors) != 0 {
		return validateNativeRestorationMemberWitness(authority)
	}
	var artifact *agentpb.ComposeArtifact
	if authority.AppliedPredecessor != nil {
		var err error
		artifact, err = openRestorationWitness(authority.EnvironmentID, authority.AppliedPredecessor.ComposeArtifact)
		if err != nil {
			return err
		}
	}
	identities := make([]executionplan.CandidateServiceIdentity, len(authority.Candidates))
	for index, member := range authority.Candidates {
		identities[index] = executionplan.CandidateServiceIdentity{
			ServiceID: member.ServiceID,
			ReleaseID: member.ReleaseID,
		}
	}
	selected, err := executionplan.SelectBlueprintRestorationMembers(identities, artifact)
	if err != nil || len(selected) != len(authority.Candidates) {
		return corruptTaskAssignment()
	}
	for index, member := range selected {
		candidate := authority.Candidates[index]
		if candidate.ServiceID != member.ServiceID || candidate.ReleaseID != member.ReleaseID {
			return corruptTaskAssignment()
		}
		switch member.Target {
		case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE:
			if candidate.Target != ReleaseRestorationCandidateAbsence {
				return corruptTaskAssignment()
			}
		case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR:
			if candidate.Target != ReleaseRestorationServingPredecessor {
				return corruptTaskAssignment()
			}
		default:
			return corruptTaskAssignment()
		}
	}
	return nil
}
