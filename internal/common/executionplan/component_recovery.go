package executionplan

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ComponentRecoveryStepIDs returns the recovery-only sequence for one
// validated authored Component procedure. Files precede runtime restoration in
// both phases so a restarted predecessor consumes only restored inputs.
func ComponentRecoveryStepIDs(procedure *agentpb.ComponentRecoveryProcedure) []string {
	if procedure == nil {
		return nil
	}
	files := procedure.GetConfigurationRestoration().GetFiles()
	result := make([]string, 0, len(files)*2+2)
	for _, file := range files {
		result = append(result, file.GetProbeStepId())
	}
	probe, compensate := componentRuntimeRecoveryStepIDs(procedure)
	result = append(result, probe)
	for _, file := range files {
		result = append(result, file.GetCompensateStepId())
	}
	return append(result, compensate)
}

func componentRuntimeRecoveryStepIDs(procedure *agentpb.ComponentRecoveryProcedure) (string, string) {
	if prior := procedure.GetPredecessor(); prior != nil {
		return prior.GetProbeStepId(), prior.GetCompensateStepId()
	}
	absence := procedure.GetCandidateAbsence()
	return absence.GetProbeStepId(), absence.GetCompensateStepId()
}

func validateComponentRecoveryPlan(
	plan *agentpb.ExecutionPlan,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	procedure := plan.GetComponentRecoveryProcedure()
	if procedure == nil {
		for _, step := range plan.GetSteps() {
			if step.GetComponentRestorationProbe() != nil || step.GetComponentRestorationCompensate() != nil {
				return invalidComponentRecovery("unbound runtime restoration step")
			}
		}
		return nil
	}
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		ids.Validate(ids.KindEnvironment, plan.GetTargetId()) != nil ||
		ids.Validate(ids.KindComponent, procedure.GetComponentId()) != nil ||
		ids.Validate(ids.KindConfig, procedure.GetCandidateArtifactId()) != nil ||
		plan.GetCandidateReleaseProcedure() != nil || plan.GetServiceLifecycleProcedure() != nil ||
		plan.GetEntryMutationProcedure() != nil || plan.GetServiceAbsenceRecoveryProcedure() != nil ||
		len(procedure.GetForwardStepIds()) == 0 {
		return invalidComponentRecovery("procedure scope is invalid")
	}
	candidate := artifacts[procedure.GetCandidateArtifactId()]
	if candidate == nil || candidate.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		candidate.GetOwnerId() != plan.GetTargetId() {
		return invalidComponentRecovery("candidate artifact is invalid")
	}
	steps := make(map[string][]*agentpb.ExecutionStep, len(plan.GetSteps()))
	for _, step := range plan.GetSteps() {
		steps[step.GetStepId()] = append(steps[step.GetStepId()], step)
	}
	selected := make(map[string]bool, len(procedure.GetForwardStepIds())+2)
	for _, stepID := range procedure.GetForwardStepIds() {
		bound := steps[stepID]
		if ids.Validate(ids.KindStep, stepID) != nil || selected[stepID] || len(bound) != 1 ||
			bound[0].GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD ||
			!componentForwardMutation(bound[0]) {
			return invalidComponentRecovery("forward mutation authority is invalid")
		}
		selected[stepID] = true
	}
	for _, step := range plan.GetSteps() {
		if componentForwardMutation(step) && !selected[step.GetStepId()] {
			return invalidComponentRecovery("forward mutation is not retained")
		}
	}
	if err := validateComponentConfigurationRestoration(plan, procedure, selected); err != nil {
		return err
	}
	probeID, compensateID := componentRuntimeRecoveryStepIDs(procedure)
	probe, compensate := steps[probeID], steps[compensateID]
	if probeID == "" || compensateID == "" || probeID == compensateID || selected[probeID] || selected[compensateID] ||
		len(probe) != 1 || len(compensate) != 1 ||
		probe[0].GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE ||
		compensate[0].GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE ||
		probe[0].GetPrerequisiteStepId() != "" || compensate[0].GetPrerequisiteStepId() != "" ||
		probe[0].GetComponentRestorationProbe().GetComponentId() != procedure.GetComponentId() ||
		probe[0].GetComponentRestorationProbe().GetCandidateArtifactId() != procedure.GetCandidateArtifactId() ||
		compensate[0].GetComponentRestorationCompensate().GetComponentId() != procedure.GetComponentId() ||
		compensate[0].GetComponentRestorationCompensate().
			GetCandidateArtifactId() !=
			procedure.GetCandidateArtifactId() {
		return invalidComponentRecovery("runtime restoration pair is invalid")
	}
	if err := validateComponentRecoveryTarget(plan, procedure, candidate, artifacts); err != nil {
		return err
	}
	for _, step := range plan.GetSteps() {
		if (step.GetComponentRestorationProbe() != nil && step.GetStepId() != probeID) ||
			(step.GetComponentRestorationCompensate() != nil && step.GetStepId() != compensateID) {
			return invalidComponentRecovery("runtime restoration step escapes its procedure")
		}
	}
	return nil
}

func validateComponentConfigurationRestoration(
	plan *agentpb.ExecutionPlan,
	procedure *agentpb.ComponentRecoveryProcedure,
	selected map[string]bool,
) error {
	configuration := procedure.GetConfigurationRestoration()
	if configuration == nil {
		return nil
	}
	if len(configuration.GetFiles()) == 0 || len(configuration.GetFiles()) > 512 ||
		(configuration.GetPriorSnapshotId() == "" && len(configuration.GetPriorSnapshotSha256()) != 0) ||
		(configuration.GetPriorSnapshotId() != "" && (ids.Validate(ids.KindConfig, configuration.GetPriorSnapshotId()) != nil ||
			len(configuration.GetPriorSnapshotSha256()) != 32)) {
		return invalidComponentRecovery("configuration source is invalid")
	}
	previous := ""
	for _, file := range configuration.GetFiles() {
		if file == nil || file.GetForwardStepId() <= previous || !selected[file.GetForwardStepId()] ||
			ids.Validate(ids.KindStep, file.GetProbeStepId()) != nil ||
			ids.Validate(ids.KindStep, file.GetCompensateStepId()) != nil ||
			selected[file.GetProbeStepId()] || selected[file.GetCompensateStepId()] {
			return invalidComponentRecovery("configuration bindings are invalid")
		}
		previous = file.GetForwardStepId()
		selected[file.GetProbeStepId()], selected[file.GetCompensateStepId()] = true, true
	}
	return validateConfigurationRestorationSteps(plan, configuration)
}

func validateComponentRecoveryTarget(
	plan *agentpb.ExecutionPlan,
	procedure *agentpb.ComponentRecoveryProcedure,
	candidate *agentpb.ComposeArtifact,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	ownedCandidate := componentOwnedServices(candidate, procedure.GetComponentId())
	if prior := procedure.GetPredecessor(); prior != nil {
		if procedure.GetCandidateAbsence() != nil || ids.Validate(ids.KindConfig, prior.GetPriorArtifactId()) != nil ||
			ids.Validate(ids.KindService, prior.GetServiceId()) != nil {
			return invalidComponentRecovery("predecessor target is invalid")
		}
		artifact := artifacts[prior.GetPriorArtifactId()]
		ownedPrior := componentOwnedServices(artifact, procedure.GetComponentId())
		managed := plan.GetManagedComponentProcedure()
		if artifact == nil || artifact.GetOwnerId() != plan.GetTargetId() || len(ownedPrior) != 1 ||
			ownedPrior[0].GetServiceId() != prior.GetServiceId() ||
			managed == nil || len(managed.GetServices()) != 1 ||
			managed.GetServices()[0].GetComponentId() != procedure.GetComponentId() ||
			managed.GetServices()[0].GetServiceId() != prior.GetServiceId() ||
			managed.GetServices()[0].GetSourceArtifactId() != prior.GetPriorArtifactId() ||
			(len(ownedCandidate) != 0 && (len(ownedCandidate) != 1 || ownedCandidate[0].GetServiceId() != prior.GetServiceId())) {
			return invalidComponentRecovery("predecessor artifact is invalid")
		}
		return nil
	}
	absence := procedure.GetCandidateAbsence()
	if absence == nil || absence.GetComposeProjectName() == "" ||
		ids.Validate(ids.KindService, absence.GetServiceId()) != nil ||
		len(ownedCandidate) != 1 || ownedCandidate[0].GetServiceId() != absence.GetServiceId() ||
		candidate.GetProjectName() != absence.GetComposeProjectName() || plan.GetManagedComponentProcedure() != nil {
		return invalidComponentRecovery("candidate absence target is invalid")
	}
	return nil
}

func componentOwnedServices(artifact *agentpb.ComposeArtifact, componentID string) []*agentpb.ComposeService {
	if artifact == nil {
		return nil
	}
	var result []*agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetOwnerComponentId() == componentID {
			result = append(result, service)
		}
	}
	return result
}

func componentForwardMutation(step *agentpb.ExecutionStep) bool {
	return step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD &&
		(step.GetMaterializeFile() != nil || step.GetComposeApply() != nil ||
			step.GetComposeRemove() != nil || step.GetComponentApply() != nil)
}

func ComponentRuntimeRecoveryPair(
	procedure *agentpb.ComponentRecoveryProcedure,
	stepID string,
) (probeID, compensateID string, found bool) {
	probeID, compensateID = componentRuntimeRecoveryStepIDs(procedure)
	return probeID, compensateID, slices.Contains([]string{probeID, compensateID}, stepID)
}

func invalidComponentRecovery(reason string) error {
	return errs.New(errs.KindValidationFailed, "Component recovery: "+reason)
}
