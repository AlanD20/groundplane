package executionplan

import (
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func serviceLifecycleArtifactValidationPlan(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
) (*agentpb.ExecutionPlan, bool, error) {
	procedure := plan.GetServiceLifecycleProcedure()
	if procedure == nil {
		return plan, false, nil
	}
	for _, source := range procedure.GetSources() {
		if source.GetArtifactId() != artifact.GetArtifactId() {
			continue
		}
		if ids.Validate(ids.KindPlan, source.GetSourcePlanId()) != nil || source.GetSourceRenderGeneration() == 0 {
			return nil, true, errs.New(errs.KindValidationFailed, "service lifecycle source ownership is invalid")
		}
		validation := proto.CloneOf(plan)
		validation.PlanId = source.GetSourcePlanId()
		validation.RenderGeneration = source.GetSourceRenderGeneration()
		return validation, true, nil
	}
	if blueprintServiceRemovalPriorArtifact(plan, artifact.GetArtifactId()) {
		return plan, false, nil
	}
	return nil, true, errs.New(errs.KindValidationFailed, "service lifecycle artifact source is absent")
}

func validateServiceLifecyclePlan(plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetServiceLifecycleProcedure()
	if procedure == nil {
		return nil
	}
	if plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
		return validateBlueprintServiceRemovalLifecyclePlan(plan)
	}
	if ids.Validate(ids.KindService, plan.GetTargetId()) != nil ||
		plan.GetCandidateReleaseProcedure() != nil || plan.GetManagedComponentProcedure() != nil ||
		plan.GetComponentRollbackObservation() != nil ||
		(plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_START &&
			plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_STOP &&
			plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_DESTROY &&
			plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_REMOVE) ||
		len(procedure.GetSources()) == 0 || len(procedure.GetSources()) > 3 ||
		len(procedure.GetSources()) != len(plan.GetArtifacts()) || len(procedure.GetSources()) != len(plan.GetSteps()) {
		return errs.New(errs.KindValidationFailed, "service lifecycle procedure shape is invalid")
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(plan.GetArtifacts()))
	steps := make(map[string]*agentpb.ExecutionStep, len(plan.GetSteps()))
	for _, artifact := range plan.GetArtifacts() {
		artifacts[artifact.GetArtifactId()] = artifact
	}
	for _, step := range plan.GetSteps() {
		steps[step.GetStepId()] = step
	}
	seenArtifacts := make(map[string]bool, len(artifacts))
	seenSteps := make(map[string]bool, len(steps))
	for index, source := range procedure.GetSources() {
		if source == nil || ids.Validate(ids.KindConfig, source.GetArtifactId()) != nil ||
			ids.Validate(ids.KindPlan, source.GetSourcePlanId()) != nil || source.GetSourceRenderGeneration() == 0 ||
			source.GetServiceId() != plan.GetTargetId() || ids.Validate(ids.KindStep, source.GetStepId()) != nil ||
			seenArtifacts[source.GetArtifactId()] || seenSteps[source.GetStepId()] ||
			len(source.GetComposeNames()) == 0 || !slices.IsSorted(source.GetComposeNames()) {
			return errs.New(errs.KindValidationFailed, "service lifecycle source is invalid")
		}
		artifact, step := artifacts[source.GetArtifactId()], steps[source.GetStepId()]
		if artifact == nil || step == nil ||
			artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			len(artifact.GetServices()) != len(source.GetComposeNames()) {
			return errs.New(errs.KindValidationFailed, "service lifecycle source selection is incomplete")
		}
		for serviceIndex, service := range artifact.GetServices() {
			if service == nil || service.GetServiceId() != plan.GetTargetId() ||
				service.GetComposeName() != source.GetComposeNames()[serviceIndex] ||
				!serviceLifecycleLabelsMatch(service, source) {
				return errs.New(errs.KindValidationFailed, "service lifecycle artifact exceeds its source authority")
			}
		}
		if index == 0 && step.GetPrerequisiteStepId() != "" ||
			index > 0 && step.GetPrerequisiteStepId() != procedure.GetSources()[index-1].GetStepId() ||
			!serviceLifecycleStepMatches(plan.GetOperation(), step, source) {
			return errs.New(errs.KindValidationFailed, "service lifecycle step exceeds its source authority")
		}
		seenArtifacts[source.GetArtifactId()] = true
		seenSteps[source.GetStepId()] = true
	}
	return nil
}

func validateBlueprintServiceRemovalLifecyclePlan(plan *agentpb.ExecutionPlan) error {
	invalid := func() error {
		return errs.New(errs.KindValidationFailed, "Blueprint Service removal procedure shape is invalid")
	}
	procedure := plan.GetServiceLifecycleProcedure()
	candidate := plan.GetCandidateReleaseProcedure()
	if ids.Validate(ids.KindService, plan.GetTargetId()) != nil || procedure == nil || candidate == nil ||
		candidate.GetConfigurationRestoration() != nil || len(candidate.GetMembers()) != 1 ||
		plan.GetManagedComponentProcedure() != nil || plan.GetComponentRecoveryProcedure() != nil ||
		plan.GetEntryMutationProcedure() != nil || plan.GetComponentRollbackObservation() != nil ||
		len(procedure.GetSources()) == 0 || len(procedure.GetSources()) > 3 ||
		len(plan.GetSteps()) != len(procedure.GetSources())+2 {
		return invalid()
	}
	member := candidate.GetMembers()[0]
	serving, absence := member.GetServingPredecessor(), member.GetCandidateAbsence()
	if member.GetServiceId() != plan.GetTargetId() || serving == nil || absence == nil ||
		len(member.GetForwardStepIds()) != len(procedure.GetSources()) ||
		serving.GetPriorArtifactId() == "" || serving.GetPriorReleaseId() != member.GetCandidateReleaseId() ||
		absence.GetComposeProjectName() == "" || len(absence.GetServices()) != 1 ||
		absence.GetServices()[0].GetServiceId() != member.GetServiceId() ||
		absence.GetServices()[0].GetReleaseId() != member.GetCandidateReleaseId() {
		return invalid()
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(plan.GetArtifacts()))
	for _, artifact := range plan.GetArtifacts() {
		if artifact == nil || artifact.GetArtifactId() == "" || artifacts[artifact.GetArtifactId()] != nil {
			return invalid()
		}
		artifacts[artifact.GetArtifactId()] = artifact
	}
	wantArtifacts := make(map[string]bool, len(procedure.GetSources())+2)
	seenSteps := make(map[string]bool, len(procedure.GetSources()))
	for index, source := range procedure.GetSources() {
		if source == nil || source.GetServiceId() != plan.GetTargetId() ||
			member.GetForwardStepIds()[index] != source.GetStepId() || seenSteps[source.GetStepId()] ||
			len(source.GetComposeNames()) != 1 {
			return invalid()
		}
		artifact, step := artifacts[source.GetArtifactId()], plan.GetSteps()[index]
		if artifact == nil || len(artifact.GetServices()) != 1 ||
			artifact.GetServices()[0].GetComposeName() != source.GetComposeNames()[0] ||
			artifact.GetServices()[0].GetServiceId() != plan.GetTargetId() ||
			step.GetStepId() != source.GetStepId() ||
			step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD ||
			index == 0 && step.GetPrerequisiteStepId() != "" ||
			index > 0 && step.GetPrerequisiteStepId() != procedure.GetSources()[index-1].GetStepId() ||
			!serviceLifecycleStepMatches(agentpb.PlanOperation_PLAN_OPERATION_REMOVE, step, source) {
			return invalid()
		}
		wantArtifacts[source.GetArtifactId()] = true
		seenSteps[source.GetStepId()] = true
	}
	probe, compensate := plan.GetSteps()[len(procedure.GetSources())], plan.GetSteps()[len(procedure.GetSources())+1]
	if probe.GetStepId() != serving.GetProbeStepId() || probe.GetStepId() != absence.GetProbeStepId() ||
		probe.GetPrerequisiteStepId() != "" ||
		compensate.GetStepId() != serving.GetCompensateStepId() ||
		compensate.GetStepId() != absence.GetCompensateStepId() ||
		compensate.GetPrerequisiteStepId() != procedure.GetSources()[len(procedure.GetSources())-1].GetStepId() ||
		probe.GetCandidateRestorationProbe() == nil || compensate.GetCandidateRestorationCompensate() == nil {
		return invalid()
	}
	wantArtifacts[serving.GetPriorArtifactId()] = true
	if serving.GetRetainedPriorArtifactId() != "" {
		wantArtifacts[serving.GetRetainedPriorArtifactId()] = true
	}
	if len(artifacts) != len(wantArtifacts) || artifacts[serving.GetPriorArtifactId()] == nil ||
		member.GetCandidateArtifactId() == serving.GetPriorArtifactId() ||
		artifacts[member.GetCandidateArtifactId()] == nil ||
		artifacts[member.GetCandidateArtifactId()].GetProjectName() != absence.GetComposeProjectName() {
		return invalid()
	}
	for artifactID := range artifacts {
		if !wantArtifacts[artifactID] {
			return invalid()
		}
	}
	return nil
}

func blueprintServiceRemovalPriorArtifact(plan *agentpb.ExecutionPlan, artifactID string) bool {
	if plan == nil || validateBlueprintServiceRemovalLifecyclePlan(plan) != nil {
		return false
	}
	serving := plan.GetCandidateReleaseProcedure().GetMembers()[0].GetServingPredecessor()
	return artifactID == serving.GetPriorArtifactId() ||
		artifactID != "" && artifactID == serving.GetRetainedPriorArtifactId()
}

func serviceLifecycleLabelsMatch(service *agentpb.ComposeService, source *agentpb.ServiceLifecycleSource) bool {
	planID, generation := "", ""
	for _, label := range service.GetExpectedLabels() {
		switch label.GetKey() {
		case labelPlanID:
			planID = label.GetValue()
		case labelRenderGen:
			generation = label.GetValue()
		}
	}
	return planID == source.GetSourcePlanId() &&
		generation == strconv.FormatUint(source.GetSourceRenderGeneration(), 10)
}

func serviceLifecycleStepMatches(
	operation agentpb.PlanOperation,
	step *agentpb.ExecutionStep,
	source *agentpb.ServiceLifecycleSource,
) bool {
	switch operation {
	case agentpb.PlanOperation_PLAN_OPERATION_START:
		apply := step.GetComposeApply()
		return apply != nil && apply.GetArtifactId() == source.GetArtifactId() && !apply.GetFullReconcile() &&
			!apply.GetForceRecreate() && !apply.GetNoDependencies() && slices.Equal(apply.GetServiceIds(), []string{source.GetServiceId()})
	case agentpb.PlanOperation_PLAN_OPERATION_STOP:
		stop := step.GetComposeStop()
		return stop != nil && stop.GetArtifactId() == source.GetArtifactId() && stop.GetGraceSeconds() > 0 &&
			slices.Equal(stop.GetServiceIds(), []string{source.GetServiceId()})
	case agentpb.PlanOperation_PLAN_OPERATION_DESTROY, agentpb.PlanOperation_PLAN_OPERATION_REMOVE:
		remove := step.GetComposeRemove()
		return remove != nil && remove.GetArtifactId() == source.GetArtifactId() && !remove.GetWholeProject() &&
			slices.Equal(remove.GetServiceIds(), []string{source.GetServiceId()})
	default:
		return false
	}
}
