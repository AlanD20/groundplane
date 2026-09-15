package executionplan

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RestorationObservation is read-only authority for the captured predecessor.
// It is not an ExecutionPlan and cannot authorize a helper mutation.
type RestorationObservation struct {
	artifact           *agentpb.ComposeArtifact
	candidateWorkloads []*agentpb.ComposeService
}

// CandidateWorkloads are separately sealed, non-serving evidence. They never
// replace the captured predecessor or authorize a mutation.
func (observation *RestorationObservation) CandidateWorkloads() []*agentpb.ComposeService {
	if observation == nil {
		return nil
	}
	result := make([]*agentpb.ComposeService, len(observation.candidateWorkloads))
	for i, service := range observation.candidateWorkloads {
		result[i] = proto.CloneOf(service)
	}
	return result
}

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
		return &RestorationObservation{artifact: artifact,
			candidateWorkloads: RestorationCandidateWorkloads(owned, member.GetServiceId())}, nil
	}
	return nil, invalidRestorationObservation()
}

// NewPlanRestorationObservation binds ordinary Release observation to a sealed
// recovery pair and one of that pair's declared runtime artifacts.
func NewPlanRestorationObservation(
	plan *agentpb.ExecutionPlan,
	stepID, artifactID string,
) (*RestorationObservation, error) {
	owned, err := Validate(plan)
	if err != nil {
		return nil, err
	}
	for _, member := range owned.GetCandidateReleaseProcedure().GetMembers() {
		serving := member.GetServingPredecessor()
		if serving == nil || stepID == "" ||
			(stepID != serving.GetProbeStepId() && stepID != serving.GetCompensateStepId()) ||
			(artifactID != serving.GetPriorArtifactId() && artifactID != member.GetCandidateArtifactId()) {
			continue
		}
		for _, artifact := range owned.GetArtifacts() {
			if artifact.GetArtifactId() == artifactID {
				return &RestorationObservation{artifact: artifact,
					candidateWorkloads: RestorationCandidateWorkloads(owned, member.GetServiceId())}, nil
			}
		}
	}
	return nil, invalidRestorationObservation()
}

// RestorationCandidateWorkloads selects only the current member's candidate
// workload identities from an admitted plan, never its proxy or other members.
func RestorationCandidateWorkloads(plan *agentpb.ExecutionPlan, serviceID string) []*agentpb.ComposeService {
	for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
		if member.GetServiceId() != serviceID {
			continue
		}
		for _, artifact := range plan.GetArtifacts() {
			if artifact.GetArtifactId() != member.GetCandidateArtifactId() {
				continue
			}
			var result []*agentpb.ComposeService
			for _, service := range artifact.GetServices() {
				if service.GetServiceId() != serviceID ||
					(service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT &&
						service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON) {
					continue
				}
				for _, label := range service.GetExpectedLabels() {
					if label.GetKey() == "com.groundplane.release-id" &&
						label.GetValue() == member.GetCandidateReleaseId() {
						result = append(result, proto.CloneOf(service))
					}
				}
			}
			return result
		}
	}
	return nil
}

func invalidRestorationObservation() error {
	return errs.New(
		errs.KindValidationFailed,
		"restoration observation is not bound to the selected historical witness",
	)
}
