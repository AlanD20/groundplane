package executionplan

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BlueprintRuntimeInput selects a member from the one exact executed artifact.
// Only a distinct retained slot needs separate bytes; duplicating the current
// artifact per member would exhaust the bounded publication marker.
type BlueprintRuntimeInput struct {
	ServiceID             string `json:"service_id"`
	ReleaseID             string `json:"release_id"`
	Target                string `json:"target"`
	RetainedPriorArtifact []byte `json:"retained_prior_artifact,omitempty"`
}

// PrepareBlueprintRuntimeInputs binds every member to the sealed executed
// artifact. Blueprint applies the complete candidate, including its proxy;
// ordinary Release proxy-switch preparation remains separate.
func PrepareBlueprintRuntimeInputs(plan *agentpb.ExecutionPlan, artifactID string) ([]BlueprintRuntimeInput, error) {
	sealed, err := Validate(plan)
	if err != nil {
		return nil, err
	}
	if sealed.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		sealed.GetCandidateReleaseProcedure() == nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint runtime requires a sealed candidate procedure")
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(sealed.Artifacts))
	for _, artifact := range sealed.Artifacts {
		artifacts[artifact.ArtifactId] = artifact
	}
	var result []BlueprintRuntimeInput
	for _, member := range sealed.CandidateReleaseProcedure.Members {
		if member.CandidateArtifactId != artifactID {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint runtime differs from the executed artifact")
		}
		artifact := artifacts[member.CandidateArtifactId]
		target, err := blueprintRuntimeTarget(artifact, member)
		if err != nil {
			return nil, err
		}
		retained, err := prepareCandidateRetainedPrior(member, target, artifacts)
		if err != nil {
			return nil, err
		}
		input := BlueprintRuntimeInput{ServiceID: member.ServiceId, ReleaseID: member.CandidateReleaseId,
			Target: target, RetainedPriorArtifact: retained}
		if _, err := projectBlueprintRuntime(artifact, input); err != nil {
			return nil, err
		}
		result = append(result, input)
	}
	return result, nil
}

// OpenBlueprintRuntime reconstructs the prepared member solely from retained
// publication bytes, never from desired state or original Release input.
func OpenBlueprintRuntime(encoded []byte, input BlueprintRuntimeInput) (CandidateRuntime, error) {
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(encoded, artifact) != nil || RejectUnknown(artifact) != nil {
		return CandidateRuntime{}, errs.New(errs.KindValidationFailed, "Blueprint executed artifact is invalid")
	}
	return projectBlueprintRuntime(artifact, input)
}

func projectBlueprintRuntime(artifact *agentpb.ComposeArtifact, input BlueprintRuntimeInput) (CandidateRuntime, error) {
	current, generation, digest, err := projectCandidateRuntimeArtifact(
		artifact, input.ServiceID, input.ReleaseID, input.Target, true, nil,
	)
	if err != nil {
		return CandidateRuntime{}, err
	}
	encoded, err := marshalCandidateRuntimeArtifact(current)
	if err != nil {
		return CandidateRuntime{}, err
	}
	if err := ValidateNativePredecessorWitness(current.OwnerId, input.ServiceID, encoded, input.RetainedPriorArtifact); err != nil {
		return CandidateRuntime{}, err
	}
	return CandidateRuntime{ServiceID: input.ServiceID, ReleaseID: input.ReleaseID, Target: input.Target,
		ProxyGeneration: generation, ProxyConfigSHA256: digest, CurrentArtifact: encoded,
		RetainedPriorArtifact: append([]byte(nil), input.RetainedPriorArtifact...)}, nil
}

func blueprintRuntimeTarget(artifact *agentpb.ComposeArtifact, member *agentpb.CandidateReleaseMember) (string, error) {
	target := ""
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != member.GetServiceId() ||
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY ||
			expectedReleaseLabel(service) != member.GetCandidateReleaseId() {
			continue
		}
		if target != "" || validateNativePredecessorWorkload(service) != nil {
			return "", errs.New(errs.KindValidationFailed, "Blueprint runtime candidate workload is ambiguous")
		}
		target = attachRuntimeTarget(service)
	}
	if target == "" {
		return "", errs.New(errs.KindValidationFailed, "Blueprint runtime candidate workload is absent")
	}
	return target, nil
}
