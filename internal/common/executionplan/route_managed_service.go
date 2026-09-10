package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Route file reconciliation may start a missing router but must not force a
// replacement. This closed materialize/up/activate chain grants no native up.
func routeManagedServiceSelection(
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
	artifacts map[string]*agentpb.ComposeArtifact,
) bool {
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE &&
		plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_REMOVE ||
		ids.Validate(ids.KindRoute, plan.GetTargetId()) != nil ||
		len(plan.GetSteps()) != 3 ||
		plan.Steps[1] != step {
		return false
	}
	apply := step.GetComposeApply()
	materialization, activation := plan.Steps[0].GetMaterializeFile(), plan.Steps[2].GetComponentApply()
	if materialization == nil || activation == nil || apply == nil || apply.ForceRecreate || !apply.NoDependencies ||
		!blueprintManagedServiceSelection(agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, apply, artifacts) ||
		step.GetPrerequisiteStepId() != plan.Steps[0].GetStepId() || plan.Steps[2].GetPrerequisiteStepId() != step.GetStepId() ||
		activation.ArtifactId != materialization.MaterializationId {
		return false
	}
	for _, service := range artifacts[apply.ArtifactId].Services {
		if service.ServiceId == apply.ServiceIds[0] {
			return service.OwnerComponentId == activation.ComponentId
		}
	}
	return false
}
