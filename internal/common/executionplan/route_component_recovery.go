package executionplan

import (
	"bytes"
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func routeComponentRecoveryEnvironment(
	plan *agentpb.ExecutionPlan, artifacts map[string]*agentpb.ComposeArtifact,
) (string, error) {
	procedure := plan.GetComponentRecoveryProcedure()
	if procedure == nil || ids.Validate(ids.KindRoute, plan.GetTargetId()) != nil ||
		(plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE &&
			plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_REMOVE) ||
		plan.GetManagedComponentProcedure() != nil || len(procedure.GetForwardStepIds()) != 3 ||
		len(plan.GetSteps()) != 3+len(ComponentRecoveryStepIDs(procedure)) ||
		len(procedure.GetConfigurationRestoration().GetFiles()) != 1 {
		return "", invalidComponentRecovery("Route recovery exceeds its singleton operation")
	}
	candidate := artifacts[procedure.GetCandidateArtifactId()]
	if candidate == nil || ids.Validate(ids.KindEnvironment, candidate.GetOwnerId()) != nil ||
		len(artifacts) != 1 && procedure.GetPredecessor() == nil || len(artifacts) > 2 {
		return "", invalidComponentRecovery("Route recovery artifact scope is invalid")
	}
	owned := componentOwnedServices(candidate, procedure.GetComponentId())
	if len(owned) != 1 {
		return "", invalidComponentRecovery("Route recovery router is missing or ambiguous")
	}
	fileStep, applyStep, activateStep := plan.GetSteps()[0], plan.GetSteps()[1], plan.GetSteps()[2]
	file, apply, action := fileStep.GetMaterializeFile(), applyStep.GetComposeApply(), activateStep.GetComponentApply()
	if file == nil || apply == nil || action == nil ||
		file.GetArtifactId() != candidate.GetArtifactId() || file.GetEnvironmentId() != candidate.GetOwnerId() ||
		file.GetOutputKind() != agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE ||
		file.GetServiceId() != "" || file.GetServiceName() != "" || fileStep.GetPrerequisiteStepId() != "" ||
		apply.GetArtifactId() != candidate.GetArtifactId() || !apply.GetNoDependencies() ||
		apply.GetFullReconcile() || apply.GetForceRecreate() ||
		!slices.Equal(apply.GetServiceIds(), []string{owned[0].GetServiceId()}) ||
		applyStep.GetPrerequisiteStepId() != fileStep.GetStepId() ||
		activateStep.GetPrerequisiteStepId() != applyStep.GetStepId() ||
		action.GetComponentId() != procedure.GetComponentId() || action.GetArtifactId() != file.GetMaterializationId() ||
		!bytes.Equal(action.GetArtifactDigest(), file.GetSha256()) {
		return "", invalidComponentRecovery("Route forward work differs from its captured router")
	}
	for index, step := range plan.GetSteps()[:3] {
		if procedure.GetForwardStepIds()[index] != step.GetStepId() || !componentForwardMutation(step) {
			return "", invalidComponentRecovery("Route forward steps differ from their recovery binding")
		}
	}
	bound := procedure.GetConfigurationRestoration().GetFiles()[0]
	if bound.GetForwardStepId() != fileStep.GetStepId() {
		return "", invalidComponentRecovery("Route file restoration does not select its forward file")
	}
	allowed := ComponentRecoveryStepIDs(procedure)
	for _, step := range plan.GetSteps()[3:] {
		if !slices.Contains(allowed, step.GetStepId()) {
			return "", invalidComponentRecovery("Route recovery contains unrelated work")
		}
	}
	return candidate.GetOwnerId(), nil
}

// Historical artifacts can contain unrelated workloads. They are read-only
// inventory here: the closed recovery procedure can recreate only its router.
func validRouteRecoveryRetainedOwnership(
	plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact, labels map[string]string,
) bool {
	procedure := plan.GetComponentRecoveryProcedure()
	prior := procedure.GetPredecessor()
	if prior == nil || prior.GetPriorArtifactId() != artifact.GetArtifactId() ||
		ids.Validate(ids.KindPlan, labels[labelPlanID]) != nil {
		return false
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(plan.GetArtifacts()))
	for _, captured := range plan.GetArtifacts() {
		if captured == nil || artifacts[captured.GetArtifactId()] != nil {
			return false
		}
		artifacts[captured.GetArtifactId()] = captured
	}
	environmentID, err := routeComponentRecoveryEnvironment(plan, artifacts)
	if err != nil || artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != environmentID {
		return false
	}
	generation, err := strconv.ParseUint(labels[labelRenderGen], 10, 64)
	return err == nil && generation > 0 && generation < plan.GetRenderGeneration() &&
		strconv.FormatUint(generation, 10) == labels[labelRenderGen]
}
