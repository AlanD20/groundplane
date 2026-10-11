package agent

import "github.com/AlanD20/groundplane/proto/agentpb"

// Every host-effect unit must have accepted running evidence before it can
// mutate. A disconnect after that barrier is a settlement obligation, not
// permission to replay the unit's forward work.
func blueprintUnitMutationStep(plan *agentpb.ExecutionPlan, step *agentpb.ExecutionStep) bool {
	managedNetworkRemoval := plan.GetManagedNetworkRecoveryProcedure() != nil && step.GetManagedNetworkRemove() != nil
	attachMutation := (plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_ATTACH ||
		plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_DETACH) &&
		(step.GetAdapterProcedure() != nil || step.GetBackingHookProcedure() != nil || step.GetComposeApply() != nil)
	networkPreparation := plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_RECONCILE &&
		step.GetManagedNetworkEnsure() != nil
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY && !managedNetworkRemoval &&
		!attachMutation &&
		!networkPreparation {
		return false
	}
	return step.GetMaterializeFile() != nil || step.GetComposeApply() != nil ||
		step.GetComposeRemove() != nil || step.GetComposeWorkloadRemove() != nil || step.GetComponentApply() != nil ||
		step.GetAdapterProcedure() != nil || step.GetBackingHookProcedure() != nil ||
		step.GetManagedNetworkEnsure() != nil || step.GetManagedVolumeEnsure() != nil ||
		step.GetManagedVolumeDirectoriesEnsure() != nil || step.GetManagedVolumeDirectoryRemove() != nil ||
		step.GetEnvironmentDirectoryCreate() != nil || step.GetEnvironmentDirectoryRemove() != nil ||
		step.GetHostResolutionApply() != nil || step.GetHostResolutionRestore() != nil || managedNetworkRemoval
}
