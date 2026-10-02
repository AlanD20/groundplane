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
		if retained := authority.GetNativePredecessors()[index].GetRetainedPriorArtifact(); len(retained) != 0 {
			prior, err := openNativePredecessorArtifact(authority.GetEnvironmentId(), retained)
			if err != nil {
				return nil, err
			}
			artifact = completeRestorationObservationArtifact(artifact, prior)
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
		artifact, _, err := PlanRestorationExpectedArtifact(owned, member.GetServiceId(), artifactID)
		if err != nil {
			return nil, err
		}
		return &RestorationObservation{artifact: artifact,
			candidateWorkloads: RestorationCandidateWorkloads(owned, member.GetServiceId())}, nil
	}
	return nil, invalidRestorationObservation()
}

// PlanRestorationExpectedArtifact selects read-only workload proof metadata from
// an already admitted plan. A selected predecessor includes its declared retained
// prior; a candidate remains unchanged. The boolean identifies the predecessor
// selection. This helper does not validate a plan or authorize any mutation.
func PlanRestorationExpectedArtifact(
	plan *agentpb.ExecutionPlan,
	serviceID, artifactID string,
) (*agentpb.ComposeArtifact, bool, error) {
	var selected *agentpb.ComposeArtifact
	for _, artifact := range plan.GetArtifacts() {
		if artifact.GetArtifactId() == artifactID {
			selected = proto.CloneOf(artifact)
		}
	}
	if selected == nil {
		return nil, false, invalidRestorationObservation()
	}
	for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
		serving := member.GetServingPredecessor()
		if member.GetServiceId() != serviceID || serving == nil || serving.GetPriorArtifactId() != artifactID {
			continue
		}
		if serving.GetRetainedPriorArtifactId() == "" {
			return selected, true, nil
		}
		for _, retained := range plan.GetArtifacts() {
			if retained.GetArtifactId() == serving.GetRetainedPriorArtifactId() {
				return completeRestorationObservationArtifact(selected, retained), true, nil
			}
		}
		return nil, true, invalidRestorationObservation()
	}
	return selected, false, nil
}

// The combined descriptor is read-only observation authority. Mutations still
// execute each captured artifact's own canonical YAML and historical labels.
func completeRestorationObservationArtifact(current, retained *agentpb.ComposeArtifact) *agentpb.ComposeArtifact {
	complete := proto.CloneOf(current)
	for _, service := range retained.GetServices() {
		complete.Services = append(complete.Services, proto.CloneOf(service))
	}
	for _, volume := range retained.GetVolumes() {
		found := false
		for _, existing := range complete.GetVolumes() {
			found = found || existing.GetDockerName() == volume.GetDockerName()
		}
		if !found {
			complete.Volumes = append(complete.Volumes, proto.CloneOf(volume))
		}
	}
	for _, network := range retained.GetNetworks() {
		found := false
		for _, existing := range complete.GetNetworks() {
			found = found || existing.GetDockerName() == network.GetDockerName()
		}
		if !found {
			complete.Networks = append(complete.Networks, proto.CloneOf(network))
		}
	}
	return complete
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
