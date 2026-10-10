package agent

import "github.com/AlanD20/groundplane/proto/agentpb"

// Every host-effect unit must have accepted running evidence before it can
// mutate. A disconnect after that barrier is a settlement obligation, not
// permission to replay the unit's forward work.
func blueprintUnitMutationStep(plan *agentpb.ExecutionPlan, step *agentpb.ExecutionStep) bool {
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
		return false
	}
	return step.GetMaterializeFile() != nil || step.GetComposeApply() != nil ||
		step.GetComposeRemove() != nil || step.GetComponentApply() != nil ||
		step.GetAdapterProcedure() != nil || step.GetBackingHookProcedure() != nil ||
		step.GetManagedVolumeDirectoriesEnsure() != nil || step.GetManagedVolumeDirectoryRemove() != nil ||
		step.GetEnvironmentDirectoryCreate() != nil || step.GetEnvironmentDirectoryRemove() != nil ||
		step.GetHostResolutionApply() != nil || step.GetHostResolutionRestore() != nil
}
