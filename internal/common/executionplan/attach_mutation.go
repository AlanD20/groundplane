package executionplan

import (
	"sort"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validAttachMutationOwnership(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
	labels map[string]string,
) bool {
	if plan == nil || artifact == nil ||
		(plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_ATTACH &&
			plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_DETACH) ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		labels[labelComponentID] != "" || ids.Validate(ids.KindPlan, labels[labelPlanID]) != nil ||
		len(plan.GetArtifacts()) != 1 ||
		plan.GetArtifacts()[0].GetArtifactId() != artifact.GetArtifactId() {
		return false
	}
	generation, err := strconv.ParseUint(labels[labelRenderGen], 10, 64)
	if err != nil || generation == 0 || generation > plan.GetRenderGeneration() ||
		strconv.FormatUint(generation, 10) != labels[labelRenderGen] {
		return false
	}
	composeSteps := 0
	for _, step := range plan.GetSteps() {
		if step.GetComposeApply() == nil {
			continue
		}
		composeSteps++
		if step.GetComposeApply().GetArtifactId() != artifact.GetArtifactId() {
			return false
		}
		if _, selected, selectErr := AttachMutationServices(plan, step.GetStepId()); !selected || selectErr != nil {
			return false
		}
	}
	return composeSteps <= 1
}

// AttachMutationServices narrows an Attach/Detach Compose apply to currently
// running workloads. Stable proxies and inactive native slots keep their
// captured configuration without being started by the network mutation.
func AttachMutationServices(
	plan *agentpb.ExecutionPlan,
	stepID string,
) ([]string, bool, error) {
	if plan == nil ||
		(plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_ATTACH &&
			plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_DETACH) {
		return nil, false, nil
	}
	for _, step := range plan.GetSteps() {
		if step.GetStepId() != stepID {
			continue
		}
		apply := step.GetComposeApply()
		if apply == nil || apply.GetArtifactId() == "" || apply.GetFullReconcile() ||
			!apply.GetNoDependencies() {
			return nil, true, errs.New(errs.KindValidationFailed, "Attach runtime selection is invalid")
		}
		var artifact *agentpb.ComposeArtifact
		for _, candidate := range plan.GetArtifacts() {
			if candidate.GetArtifactId() == apply.GetArtifactId() {
				artifact = candidate
				break
			}
		}
		if artifact == nil {
			return nil, true, errs.New(errs.KindValidationFailed, "Attach runtime artifact is absent")
		}
		if len(apply.GetServiceIds()) == 0 {
			return nil, true, nil
		}
		selected := make(map[string]bool, len(apply.GetServiceIds()))
		for _, serviceID := range apply.GetServiceIds() {
			if serviceID == "" || selected[serviceID] {
				return nil, true, errs.New(errs.KindValidationFailed, "Attach runtime Service selection is invalid")
			}
			selected[serviceID] = true
		}
		found := make(map[string]bool, len(selected))
		var names []string
		for _, service := range artifact.GetServices() {
			if service == nil || !selected[service.GetServiceId()] || service.GetExpectedReplicas() == 0 ||
				service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				continue
			}
			found[service.GetServiceId()] = true
			names = append(names, service.GetComposeName())
		}
		if len(found) != len(selected) {
			return nil, true, errs.New(errs.KindValidationFailed, "Attach running workload is absent")
		}
		sort.Strings(names)
		return names, true, nil
	}
	return nil, true, errs.New(errs.KindValidationFailed, "Attach runtime step is absent")
}
