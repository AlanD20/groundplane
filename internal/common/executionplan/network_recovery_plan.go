package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateNetworkRecoveryPlan(plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetManagedNetworkRecoveryProcedure()
	if procedure == nil {
		return nil
	}
	invalid := func() error {
		return errs.New(errs.KindValidationFailed, "network recovery exceeds its captured bridge operation")
	}
	if ids.Validate(ids.KindEnvironment, procedure.GetEnvironmentId()) != nil ||
		plan.GetCandidateReleaseProcedure() != nil || plan.GetComponentRecoveryProcedure() != nil ||
		plan.GetServiceAbsenceRecoveryProcedure() != nil || plan.GetManagedComponentProcedure() != nil ||
		plan.GetServiceLifecycleProcedure() != nil || plan.GetEntryMutationProcedure() != nil ||
		len(
			plan.GetScriptRunnerSnapshots(),
		) != 0 || len(plan.GetScriptRunnerProjections()) != 0 || len(plan.GetScriptBodyArtifacts()) != 0 {
		return invalid()
	}
	if removal := procedure.GetRemoval(); removal != nil {
		if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_REMOVE || len(plan.GetSteps()) != 1 ||
			len(plan.GetArtifacts()) != 0 {
			return invalid()
		}
		step := plan.GetSteps()[0]
		remove := step.GetManagedNetworkRemove()
		if remove == nil || removal.GetStepId() != step.GetStepId() ||
			removal.GetNetworkId() != remove.GetNetworkId() ||
			removal.GetDockerName() != remove.GetDockerName() ||
			procedure.GetEnvironmentId() != remove.GetEnvironmentId() {
			return invalid()
		}
		return nil
	}
	preparation := procedure.GetPreparation()
	if preparation == nil || plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE ||
		plan.GetTargetId() != procedure.GetEnvironmentId() || len(plan.GetArtifacts()) != 1 ||
		len(
			preparation.GetMembers(),
		) == 0 || len(preparation.GetMembers()) > 256 || len(plan.GetSteps()) != len(preparation.GetMembers())*2 {
		return invalid()
	}
	artifact := plan.GetArtifacts()[0]
	if artifact.GetArtifactId() != preparation.GetArtifactId() ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != procedure.GetEnvironmentId() ||
		len(artifact.GetServices()) != 0 ||
		len(artifact.GetVolumes()) != 0 ||
		len(artifact.GetNetworks()) != len(preparation.GetMembers()) {
		return invalid()
	}
	previousEnsure, previousProbe := "", ""
	for index, member := range preparation.GetMembers() {
		ensureStep, probeStep := plan.GetSteps()[index], plan.GetSteps()[len(preparation.GetMembers())+index]
		ensure, probe := ensureStep.GetManagedNetworkEnsure(), probeStep.GetManagedNetworkPreparationProbe()
		if member == nil || member.GetNetworkId() != artifact.GetNetworks()[index].GetNetworkId() || ensure == nil ||
			probe == nil ||
			ensureStep.GetStepId() != member.GetEnsureStepId() ||
			probeStep.GetStepId() != member.GetProbeStepId() ||
			ensureStep.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD ||
			probeStep.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE ||
			ensureStep.GetPrerequisiteStepId() != previousEnsure ||
			probeStep.GetPrerequisiteStepId() != previousProbe ||
			ensure.GetArtifactId() != preparation.GetArtifactId() ||
			probe.GetArtifactId() != preparation.GetArtifactId() ||
			ensure.GetNetworkId() != member.GetNetworkId() ||
			probe.GetNetworkId() != member.GetNetworkId() {
			return invalid()
		}
		previousEnsure, previousProbe = member.GetEnsureStepId(), member.GetProbeStepId()
	}
	return nil
}
