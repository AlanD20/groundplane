package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// An ordinary no-dependencies up is allowed only for one uniquely owned
// Component Service. It never grants native candidate replacement authority.
func blueprintManagedServiceSelection(
	operation agentpb.PlanOperation,
	apply *agentpb.ComposeApply,
	artifacts map[string]*agentpb.ComposeArtifact,
) bool {
	if operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY || apply.GetFullReconcile() ||
		len(apply.GetServiceIds()) != 1 {
		return false
	}
	artifact := artifacts[apply.GetArtifactId()]
	if artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
		return false
	}
	componentID := ""
	matches := 0
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == apply.ServiceIds[0] {
			matches++
			if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED ||
				validateID(ids.KindComponent, service.GetOwnerComponentId()) != nil {
				return false
			}
			componentID = service.GetOwnerComponentId()
		}
	}
	if matches != 1 {
		return false
	}
	owners := 0
	for _, service := range artifact.GetServices() {
		if service.GetOwnerComponentId() == componentID {
			owners++
		}
	}
	return owners == 1
}
