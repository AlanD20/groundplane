package executionplan

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ServiceAbsenceRecoveryStepIDs returns the closed probe/compensation cursor.
func ServiceAbsenceRecoveryStepIDs(procedure *agentpb.ServiceAbsenceRecoveryProcedure) []string {
	if procedure == nil {
		return nil
	}
	return []string{procedure.GetProbeStepId(), procedure.GetCompensateStepId()}
}

func validateServiceAbsenceRecoveryPlan(
	plan *agentpb.ExecutionPlan,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	procedure := plan.GetServiceAbsenceRecoveryProcedure()
	if procedure == nil {
		for _, step := range plan.GetSteps() {
			if step.GetServiceAbsenceProbe() != nil {
				return invalidServiceAbsenceRecovery("unbound absence probe")
			}
		}
		return nil
	}
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		ids.Validate(ids.KindService, plan.GetTargetId()) != nil ||
		procedure.GetServiceId() != plan.GetTargetId() ||
		ids.Validate(ids.KindConfig, procedure.GetArtifactId()) != nil ||
		len(procedure.GetForwardStepIds()) != 1 ||
		plan.GetCandidateReleaseProcedure() != nil ||
		plan.GetManagedComponentProcedure() != nil {
		return invalidServiceAbsenceRecovery("procedure scope is invalid")
	}
	artifact := artifacts[procedure.GetArtifactId()]
	if artifact == nil || artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetProjectName() == "" || len(artifact.GetServices()) == 0 || len(artifact.GetServices()) > 8 {
		return invalidServiceAbsenceRecovery("artifact is invalid")
	}
	previous := ""
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != procedure.GetServiceId() || service.GetComposeName() <= previous {
			return invalidServiceAbsenceRecovery("physical identity set is invalid")
		}
		previous = service.GetComposeName()
	}
	steps := make(map[string]*agentpb.ExecutionStep, len(plan.GetSteps()))
	for _, step := range plan.GetSteps() {
		if steps[step.GetStepId()] != nil {
			return invalidServiceAbsenceRecovery("step identity is duplicated")
		}
		steps[step.GetStepId()] = step
	}
	forwardID := procedure.GetForwardStepIds()[0]
	forward, probe, compensate := steps[forwardID], steps[procedure.GetProbeStepId()], steps[procedure.GetCompensateStepId()]
	if forwardID == procedure.GetProbeStepId() || forwardID == procedure.GetCompensateStepId() ||
		procedure.GetProbeStepId() == procedure.GetCompensateStepId() || len(steps) != 3 ||
		forward == nil || forward.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD ||
		!serviceAbsenceRemoveStep(forward, procedure) ||
		probe == nil || probe.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE ||
		probe.GetPrerequisiteStepId() != "" || probe.GetServiceAbsenceProbe().GetArtifactId() != procedure.GetArtifactId() ||
		probe.GetServiceAbsenceProbe().GetServiceId() != procedure.GetServiceId() ||
		compensate == nil || compensate.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE ||
		compensate.GetPrerequisiteStepId() != forwardID || !serviceAbsenceRemoveStep(compensate, procedure) {
		return invalidServiceAbsenceRecovery("step authority is invalid")
	}
	return nil
}

func validateServiceAbsenceLifecyclePlan(plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetServiceLifecycleProcedure()
	recovery := plan.GetServiceAbsenceRecoveryProcedure()
	if procedure == nil || recovery == nil || len(procedure.GetSources()) != 1 {
		return invalidServiceAbsenceRecovery("lifecycle source is invalid")
	}
	source := procedure.GetSources()[0]
	artifact := planArtifact(plan, recovery.GetArtifactId())
	if source == nil || artifact == nil || source.GetArtifactId() != recovery.GetArtifactId() ||
		source.GetServiceId() != recovery.GetServiceId() || source.GetStepId() != recovery.GetForwardStepIds()[0] ||
		ids.Validate(ids.KindPlan, source.GetSourcePlanId()) != nil || source.GetSourceRenderGeneration() == 0 ||
		len(source.GetComposeNames()) != len(artifact.GetServices()) || !slices.IsSorted(source.GetComposeNames()) {
		return invalidServiceAbsenceRecovery("lifecycle source exceeds captured artifact")
	}
	for index, service := range artifact.GetServices() {
		if service.GetServiceId() != recovery.GetServiceId() ||
			service.GetComposeName() != source.GetComposeNames()[index] || !serviceLifecycleLabelsMatch(service, source) {
			return invalidServiceAbsenceRecovery("lifecycle ownership is invalid")
		}
	}
	return nil
}

func serviceAbsenceRemoveStep(step *agentpb.ExecutionStep, procedure *agentpb.ServiceAbsenceRecoveryProcedure) bool {
	remove := step.GetComposeRemove()
	return remove != nil && remove.GetArtifactId() == procedure.GetArtifactId() && !remove.GetWholeProject() &&
		slices.Equal(remove.GetServiceIds(), []string{procedure.GetServiceId()})
}

func planArtifact(plan *agentpb.ExecutionPlan, artifactID string) *agentpb.ComposeArtifact {
	for _, artifact := range plan.GetArtifacts() {
		if artifact.GetArtifactId() == artifactID {
			return artifact
		}
	}
	return nil
}

func invalidServiceAbsenceRecovery(reason string) error {
	return errs.New(errs.KindValidationFailed, "Service absence recovery: "+reason)
}
