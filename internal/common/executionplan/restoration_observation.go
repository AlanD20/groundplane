package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RestorationObservation is read-only authority for the captured predecessor.
// It is not an ExecutionPlan and cannot authorize a helper mutation.
type RestorationObservation struct{ artifact *agentpb.ComposeArtifact }

func (observation *RestorationObservation) Artifact() *agentpb.ComposeArtifact {
	if observation == nil {
		return nil
	}
	return proto.CloneOf(observation.artifact)
}

// NewRestorationObservation preserves historical labels and binds observation to
// a declared serving recovery step of the original, fully validated plan.
func NewRestorationObservation(
	plan *agentpb.ExecutionPlan,
	authority *agentpb.ReleaseRestorationAuthority,
	stepID string,
) (*RestorationObservation, error) {
	owned, err := Validate(plan)
	if err != nil {
		return nil, err
	}
	if authority == nil || RejectUnknown(authority) != nil ||
		!bytes.Equal(
			authority.GetPlanHash(),
			owned.GetPlanHash(),
		) || len(authority.GetAuthoritySha256()) != sha256.Size ||
		ids.Validate(
			ids.KindTask,
			authority.GetTaskId(),
		) != nil || ids.Validate(ids.KindOperation, authority.GetOperationId()) != nil {
		return nil, invalidRestorationObservation()
	}
	witness := authority.GetAppliedPredecessor()
	encoded := witness.GetComposeArtifact()
	if witness.GetKeyRevision() <= 0 || witness.GetRenderGeneration() == 0 ||
		ids.Validate(ids.KindTask, witness.GetRevisionId()) != nil ||
		len(encoded) == 0 ||
		len(encoded) > MaximumPlanBytes {
		return nil, invalidRestorationObservation()
	}
	digest := sha256.Sum256(encoded)
	artifact := &agentpb.ComposeArtifact{}
	if !bytes.Equal(digest[:], witness.GetComposeArtifactSha256()) || proto.Unmarshal(encoded, artifact) != nil ||
		RejectUnknown(artifact) != nil {
		return nil, invalidRestorationObservation()
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, encoded) ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != authority.GetEnvironmentId() ||
		ids.Validate(ids.KindEnvironment, artifact.GetOwnerId()) != nil {
		return nil, invalidRestorationObservation()
	}
	members := owned.GetCandidateReleaseProcedure().GetMembers()
	if len(members) == 0 || len(members) != len(authority.GetCandidates()) {
		return nil, invalidRestorationObservation()
	}
	identities := make([]CandidateServiceIdentity, len(members))
	selected := false
	for index, member := range members {
		candidate := authority.GetCandidates()[index]
		if candidate.GetServiceId() != member.GetServiceId() ||
			candidate.GetReleaseId() != member.GetCandidateReleaseId() ||
			authority.GetCandidateArtifactId() != member.GetCandidateArtifactId() {
			return nil, invalidRestorationObservation()
		}
		identities[index] = CandidateServiceIdentity{
			ServiceID: candidate.GetServiceId(),
			ReleaseID: candidate.GetReleaseId(),
		}
		serving := member.GetServingPredecessor()
		if stepID != "" && serving != nil &&
			(stepID == serving.GetProbeStepId() || stepID == serving.GetCompensateStepId()) &&
			candidate.GetTarget() == agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR {
			selected = true
		}
	}
	selections, err := SelectBlueprintRestorationMembers(identities, artifact)
	if err != nil || !selected {
		return nil, invalidRestorationObservation()
	}
	for index, selection := range selections {
		if selection.Target != authority.GetCandidates()[index].GetTarget() {
			return nil, invalidRestorationObservation()
		}
	}
	for _, candidate := range owned.GetArtifacts() {
		if candidate.GetArtifactId() == authority.GetCandidateArtifactId() &&
			candidate.GetOwnerId() == artifact.GetOwnerId() &&
			candidate.GetProjectName() == artifact.GetProjectName() {
			return &RestorationObservation{artifact: artifact}, nil
		}
	}
	return nil, invalidRestorationObservation()
}

func invalidRestorationObservation() error {
	return errs.New(
		errs.KindValidationFailed,
		"restoration observation is not bound to the selected historical witness",
	)
}
