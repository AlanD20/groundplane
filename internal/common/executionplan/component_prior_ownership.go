package executionplan

import (
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validPriorComponentOwnership(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
	serviceID string,
	labels map[string]string,
	planID string,
	generationValue string,
) bool {
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY ||
		validateID(ids.KindPlan, planID) != nil {
		return false
	}
	generation, err := strconv.ParseUint(generationValue, 10, 64)
	if err != nil || generation == 0 || strconv.FormatUint(generation, 10) != generationValue {
		return false
	}
	if len(plan.GetSteps()) == 2 && plan.GetSteps()[0].GetComponentApply() != nil &&
		plan.GetSteps()[1].GetComponentApply() != nil {
		return true
	}
	return validPriorComponentDisableOwnership(plan, artifact, serviceID, labels)
}

func validPriorComponentDisableOwnership(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
	serviceID string,
	labels map[string]string,
) bool {
	if plan.GetComponentLifecycleMode() != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE ||
		artifact == nil || artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM ||
		len(plan.GetArtifacts()) != 1 || len(artifact.GetServices()) != 1 || len(plan.GetSteps()) != 2 ||
		labels[labelServiceID] != serviceID {
		return false
	}
	restoreStep := plan.GetSteps()[0]
	removeStep := plan.GetSteps()[1]
	restore := restoreStep.GetHostResolutionRestore()
	remove := removeStep.GetComposeRemove()
	rollback := plan.GetComponentRollbackObservation()
	return restore != nil && remove != nil && rollback != nil &&
		rollback.GetComponentId() == plan.GetTargetId() && rollback.GetGeneration() == plan.GetRenderGeneration() &&
		restore.GetComponentId() == plan.GetTargetId() && restore.GetGeneration() == plan.GetRenderGeneration() &&
		restoreStep.GetPrerequisiteStepId() == "" &&
		removeStep.GetPrerequisiteStepId() == restoreStep.GetStepId() &&
		!remove.GetWholeProject() && remove.GetArtifactId() == artifact.GetArtifactId() &&
		len(remove.GetServiceIds()) == 1 && remove.GetServiceIds()[0] == serviceID
}
