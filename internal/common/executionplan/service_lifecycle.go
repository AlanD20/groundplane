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
	return nil, true, errs.New(errs.KindValidationFailed, "service lifecycle artifact source is absent")
}

func validateServiceLifecyclePlan(plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetServiceLifecycleProcedure()
	if procedure == nil {
		return nil
	}
	if ids.Validate(ids.KindService, plan.GetTargetId()) != nil ||
		plan.GetCandidateReleaseProcedure() != nil || plan.GetManagedComponentProcedure() != nil ||
		plan.GetComponentRollbackObservation() != nil ||
		(plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_START &&
			plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_STOP &&
			plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_DESTROY) ||
		len(procedure.GetSources()) == 0 || len(procedure.GetSources()) > 2 ||
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
	case agentpb.PlanOperation_PLAN_OPERATION_DESTROY:
		remove := step.GetComposeRemove()
		return remove != nil && remove.GetArtifactId() == source.GetArtifactId() && !remove.GetWholeProject() &&
			slices.Equal(remove.GetServiceIds(), []string{source.GetServiceId()})
	default:
		return false
	}
}
