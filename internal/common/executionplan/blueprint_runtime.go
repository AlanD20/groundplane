package executionplan

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BlueprintRuntimeInput selects a member from the one exact executed artifact.
// Recreate reuses that artifact; blue-green retains a compact post-switch member
// projection and its prior slot, never a complete Environment copy per member.
type BlueprintRuntimeInput struct {
	ServiceID             string `json:"service_id"`
	ReleaseID             string `json:"release_id"`
	Target                string `json:"target"`
	RetainedPriorArtifact []byte `json:"retained_prior_artifact,omitempty"`
	ActivatedArtifact     []byte `json:"activated_artifact,omitempty"`
}

// PrepareBlueprintRuntimeInputs binds every member to the sealed executed
// artifact and the same activation steps used by ordinary Releases. A switched
// proxy needs a compact member projection, not another complete Environment artifact.
func PrepareBlueprintRuntimeInputs(plan *agentpb.ExecutionPlan, artifactID string) ([]BlueprintRuntimeInput, error) {
	sealed, err := Validate(plan)
	if err != nil {
		return nil, err
	}
	if sealed.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		sealed.GetCandidateReleaseProcedure() == nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint runtime requires a sealed candidate procedure")
	}
	runtimes, err := PrepareCandidateRuntimes(sealed)
	if err != nil {
		return nil, err
	}
	var result []BlueprintRuntimeInput
	for index, member := range sealed.CandidateReleaseProcedure.Members {
		if member.CandidateArtifactId != artifactID {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint runtime differs from the executed artifact")
		}
		runtime := runtimes[index]
		input := BlueprintRuntimeInput{ServiceID: member.ServiceId, ReleaseID: member.CandidateReleaseId,
			Target: runtime.Target, RetainedPriorArtifact: runtime.RetainedPriorArtifact}
		if runtime.Target != "singleton" {
			input.ActivatedArtifact = runtime.CurrentArtifact
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
	if input.Target == "blue" || input.Target == "green" {
		activated := &agentpb.ComposeArtifact{}
		if len(input.ActivatedArtifact) == 0 || proto.Unmarshal(input.ActivatedArtifact, activated) != nil ||
			RejectUnknown(activated) != nil || activated.GetArtifactId() != artifact.GetArtifactId() ||
			activated.GetOwnerId() != artifact.GetOwnerId() || activated.GetProjectName() != artifact.GetProjectName() {
			return CandidateRuntime{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint switched runtime artifact is invalid",
			)
		}
		artifact = activated
	} else if input.Target != "singleton" || len(input.ActivatedArtifact) != 0 {
		return CandidateRuntime{}, errs.New(errs.KindValidationFailed, "Blueprint runtime activation target is invalid")
	}
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
