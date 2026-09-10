package executionplan

import (
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The authenticated Controller retains these labels only after comparing the
// complete effective Component runtime to its fenced applied source. Historical
// ownership is not native workload authority and never permits a broad up.
func validEnvironmentComponentOwnership(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
	serviceID string,
	labels map[string]string,
) bool {
	if artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindComponent, labels[labelComponentID]) != nil ||
		ids.Validate(ids.KindPlan, labels[labelPlanID]) != nil {
		return false
	}
	switch plan.GetOperation() {
	case agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK:
	default:
		return false
	}
	generation, err := strconv.ParseUint(labels[labelRenderGen], 10, 64)
	if err != nil || generation == 0 || generation > plan.GetRenderGeneration() ||
		strconv.FormatUint(generation, 10) != labels[labelRenderGen] {
		return false
	}
	matches, owners := 0, 0
	for _, service := range artifact.GetServices() {
		if service.GetOwnerComponentId() == labels[labelComponentID] {
			owners++
		}
		if service.GetServiceId() != serviceID {
			continue
		}
		if service.GetOwnerComponentId() != labels[labelComponentID] ||
			service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED || !validManagedComposeImage(service) {
			return false
		}
		matches++
	}
	if matches != 1 || owners != 1 {
		return false
	}
	for _, step := range plan.GetSteps() {
		apply := step.GetComposeApply()
		if apply.GetArtifactId() != artifact.GetArtifactId() {
			continue
		}
		if len(apply.ServiceIds) == 0 {
			return false
		}
		if slices.Contains(apply.ServiceIds, serviceID) &&
			(!apply.NoDependencies || apply.FullReconcile || apply.ForceRecreate || len(apply.ServiceIds) != 1) {
			return false
		}
	}
	return true
}
