package executionplan

import (
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
	if err := ValidateNativeRestorationAuthority(owned, authority); err != nil {
		return nil, err
	}
	for index, member := range owned.GetCandidateReleaseProcedure().GetMembers() {
		serving := member.GetServingPredecessor()
		if stepID == "" || serving == nil ||
			(stepID != serving.GetProbeStepId() && stepID != serving.GetCompensateStepId()) ||
			authority.GetCandidates()[index].GetTarget() != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR {
			continue
		}
		artifact, err := openNativePredecessorArtifact(
			authority.GetEnvironmentId(),
			authority.GetNativePredecessors()[index].GetCurrentArtifact(),
		)
		if err != nil {
			return nil, err
		}
		return &RestorationObservation{artifact: artifact}, nil
	}
	return nil, invalidRestorationObservation()
}

func invalidRestorationObservation() error {
	return errs.New(
		errs.KindValidationFailed,
		"restoration observation is not bound to the selected historical witness",
	)
}
