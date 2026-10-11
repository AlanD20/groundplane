package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/envoyrouter"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Runtime health is insufficient for a discovery router: the retained container
// can be healthy while still serving the failed Routes. Bind configuration proof
// to the exact forward action and its immutable before/after sources.
func validateComponentRecoveryConfiguration(
	plan *agentpb.ExecutionPlan,
	procedure *agentpb.ComponentRecoveryProcedure,
) error {
	var expected *agentpb.ComponentApply
	var expectedStep *agentpb.ExecutionStep
	expectedStepID := ""
	for _, step := range plan.GetSteps() {
		action := step.GetComponentApply()
		if action == nil || action.GetContainerConfigTransition() == nil ||
			len(action.GetContainerConfigTransition().GetPreviousSource()) == 0 {
			continue
		}
		if expected != nil || action.GetComponentId() != procedure.GetComponentId() ||
			step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD {
			return invalidComponentRecovery("discovery configuration authority is ambiguous")
		}
		expected = action
		expectedStep = step
		expectedStepID = step.GetStepId()
	}
	if expected == nil {
		if procedure.GetConfigurationActionStepId() != "" {
			return invalidComponentRecovery("configuration proof has no captured forward action")
		}
		if retained := procedure.GetRetainedConfiguration(); retained != nil {
			return validateRetainedComponentConfiguration(plan, procedure, retained)
		}
		if procedure.GetConfigurationSource() != nil {
			return invalidComponentRecovery("configuration proof has an empty source")
		}
		return nil
	}
	if procedure.GetPredecessor() == nil || procedure.GetConfigurationActionStepId() != expectedStepID ||
		validateComponentAction(expected) != nil {
		return invalidComponentRecovery("configuration proof differs from the captured predecessor")
	}
	for _, binding := range procedure.GetConfigurationRestoration().GetFiles() {
		var forwardFile *agentpb.MaterializeFile
		for _, step := range plan.GetSteps() {
			if step.GetStepId() == binding.GetForwardStepId() {
				forwardFile = step.GetMaterializeFile()
			}
		}
		if forwardFile == nil ||
			forwardFile.GetMaterializationId() != expectedStep.GetComponentApply().GetArtifactId() {
			continue
		}
		for _, step := range plan.GetSteps() {
			if step.GetStepId() != binding.GetCompensateStepId() {
				continue
			}
			file := step.GetMaterializeFile()
			transition := expected.GetContainerConfigTransition()
			if file.GetOutputKind() != agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE ||
				file.GetLength() != uint64(len(transition.GetPreviousSource())) ||
				!bytes.Equal(file.GetSha256(), transition.GetPreviousSourceSha256()) {
				return invalidComponentRecovery("previous discovery source differs from file restoration")
			}
			return nil
		}
	}
	return invalidComponentRecovery("previous discovery source has no bound file restoration")
}

func validateRetainedComponentConfiguration(
	plan *agentpb.ExecutionPlan, procedure *agentpb.ComponentRecoveryProcedure,
	retained *agentpb.ComponentRetainedConfiguration,
) error {
	if procedure.GetPredecessor() == nil || retained.GetGeneration() != plan.GetRenderGeneration() ||
		retained.GetFileSource() == nil {
		return invalidComponentRecovery("retained configuration has no predecessor or file source")
	}
	if unchanged := retained.GetUnchangedFile(); unchanged != nil {
		if err := validateUnchangedComponentConfiguration(plan, procedure, unchanged); err != nil {
			return err
		}
	} else {
		file := retainedComponentConfigurationFile(plan)
		if file == nil || file.GetEnvironmentId() != plan.GetTargetId() ||
			file.GetArtifactId() != procedure.GetCandidateArtifactId() ||
			file.GetOutputKind() != agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE ||
			file.GetServiceId() != "" || file.GetServiceName() != "" ||
			file.GetLength() != uint64(len(retained.GetSource())) {
			return invalidComponentRecovery("retained configuration differs from its captured file")
		}
		digest := sha256.Sum256(retained.GetSource())
		if !bytes.Equal(digest[:], file.GetSha256()) {
			return invalidComponentRecovery("retained configuration digest is invalid")
		}
	}
	if validateComponentAction(ComponentConfigurationAction(plan)) != nil {
		return invalidComponentRecovery("retained configuration recipe is invalid")
	}
	_, err := envoyrouter.Read(retained.GetSource())
	return err
}

func retainedComponentConfigurationFile(plan *agentpb.ExecutionPlan) *agentpb.MaterializeFile {
	stepID := plan.GetComponentRecoveryProcedure().GetRetainedConfiguration().GetMaterializeStepId()
	if stepID == "" || ConfigurationFilePair(plan, stepID).GetCompensateStepId() != stepID {
		return nil
	}
	for _, step := range plan.GetSteps() {
		if step.GetStepId() == stepID &&
			step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE {
			return step.GetMaterializeFile()
		}
	}
	return nil
}

// This is a compiled recipe reference, not permission to execute an activation.
// Updates reuse their captured forward action; other transitions derive the
// reference from a closed retained source. Complete plan validation precedes use.
func ComponentConfigurationAction(plan *agentpb.ExecutionPlan) *agentpb.ComponentApply {
	procedure := plan.GetComponentRecoveryProcedure()
	if retained := procedure.GetRetainedConfiguration(); retained != nil {
		if unchanged := retained.GetUnchangedFile(); unchanged != nil {
			return &agentpb.ComponentApply{
				ComponentId: procedure.GetComponentId(), DefinitionDigest: retained.GetDefinitionDigest(),
				CatalogDigest: retained.GetCatalogDigest(), ActionId: retained.GetActionId(),
				ArtifactId: unchanged.GetMaterializationId(), ArtifactDigest: unchanged.GetSha256(),
				Generation: retained.GetGeneration(),
			}
		}
		file := retainedComponentConfigurationFile(plan)
		if file == nil {
			return nil
		}
		return &agentpb.ComponentApply{
			ComponentId: procedure.GetComponentId(), DefinitionDigest: retained.GetDefinitionDigest(),
			CatalogDigest: retained.GetCatalogDigest(), ActionId: retained.GetActionId(),
			ArtifactId: file.GetMaterializationId(), ArtifactDigest: file.GetSha256(),
			Generation: retained.GetGeneration(),
		}
	}
	stepID := plan.GetComponentRecoveryProcedure().GetConfigurationActionStepId()
	if stepID == "" {
		return nil
	}
	for _, step := range plan.GetSteps() {
		if step.GetStepId() == stepID {
			return step.GetComponentApply()
		}
	}
	return nil
}

func ComponentConfigurationProofRequired(plan *agentpb.ExecutionPlan) bool {
	return plan.GetComponentRecoveryProcedure().GetConfigurationSource() != nil
}
