package environmentchanges

import (
	"bytes"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Direct Route dispatch consumes the one plan sealed at admission.
// Provider input, configuration and runtime are captured separately, but cannot
// authorize rerendering or changing executable authority after publication.
func OpenRouteExecutionPlan(value []byte, provider *RouteProviderPin, environmentID, routeID string,
	renderGeneration uint64, removal bool,
) (*agentpb.ExecutionPlan, error) {
	if len(value) == 0 || len(value) > executionplan.MaximumPlanBytes || provider == nil ||
		provider.Runtime == nil || provider.Runtime.EnvironmentID != environmentID {
		return nil, errs.New(errs.KindValidationFailed, "Route has no sealed execution authority")
	}
	decoded := &agentpb.ExecutionPlan{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(value, decoded); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Route execution plan is unreadable")
	}
	plan, err := executionplan.Validate(decoded)
	if err != nil {
		return nil, err
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(plan)
	operation := agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	if removal {
		operation = agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	}
	if err != nil || !bytes.Equal(canonical, value) || plan.GetOperation() != operation ||
		plan.GetTargetId() != routeID || renderGeneration == 0 || plan.GetRenderGeneration() != renderGeneration ||
		len(plan.GetArtifacts()) < 1 || len(plan.GetSteps()) < 3 ||
		plan.GetComponentRecoveryProcedure() == nil || plan.GetCandidateReleaseProcedure() != nil ||
		plan.GetManagedComponentProcedure() != nil || plan.GetServiceLifecycleProcedure() != nil ||
		plan.GetEntryMutationProcedure() != nil || plan.GetServiceAbsenceRecoveryProcedure() != nil ||
		plan.GetManagedNetworkRecoveryProcedure() != nil {
		return nil, errs.New(errs.KindValidationFailed, "Route execution plan exceeds its captured operation")
	}
	artifact := plan.GetArtifacts()[0]
	recovery := plan.GetComponentRecoveryProcedure()
	if artifact.GetArtifactId() != recovery.GetCandidateArtifactId() ||
		recovery.GetComponentId() != provider.ComponentID {
		return nil, errs.New(errs.KindValidationFailed, "Route recovery differs from its captured provider")
	}
	prior, err := DecodeRouteProviderRuntime(provider)
	if err != nil {
		return nil, err
	}
	if prior == nil {
		if recovery.GetPredecessor() != nil || recovery.GetCandidateAbsence().GetServiceId() != provider.ServiceID ||
			len(plan.GetArtifacts()) != 1 {
			return nil, errs.New(errs.KindValidationFailed, "Route recovery invents an acknowledged predecessor")
		}
	} else if recovery.GetPredecessor().GetPriorArtifactId() != prior.GetArtifactId() ||
		recovery.GetPredecessor().GetServiceId() != provider.ServiceID || len(plan.GetArtifacts()) != 2 ||
		!proto.Equal(plan.GetArtifacts()[1], prior) {
		return nil, errs.New(errs.KindValidationFailed, "Route recovery changes its captured predecessor")
	}
	file, apply, activate := plan.GetSteps()[0], plan.GetSteps()[1], plan.GetSteps()[2]
	action := activate.GetComponentApply()
	if artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != environmentID ||
		file.GetMaterializeFile().GetArtifactId() != artifact.GetArtifactId() ||
		file.GetMaterializeFile().GetEnvironmentId() != environmentID ||
		file.GetMaterializeFile().GetDestination() != provider.Destination ||
		apply.GetComposeApply() == nil || !apply.GetComposeApply().GetNoDependencies() ||
		apply.GetComposeApply().GetArtifactId() != artifact.GetArtifactId() ||
		apply.GetComposeApply().GetFullReconcile() || len(apply.GetComposeApply().GetServiceIds()) != 1 ||
		apply.GetComposeApply().GetServiceIds()[0] != provider.ServiceID ||
		apply.GetPrerequisiteStepId() != file.GetStepId() || activate.GetPrerequisiteStepId() != apply.GetStepId() ||
		action == nil || action.GetComponentId() != provider.ComponentID || action.GetActionId() != provider.ActionID ||
		hex.EncodeToString(action.GetDefinitionDigest()) != provider.DefinitionDigest ||
		hex.EncodeToString(action.GetCatalogDigest()) != provider.CatalogDigest ||
		action.GetArtifactId() != file.GetMaterializeFile().GetMaterializationId() ||
		!bytes.Equal(action.GetArtifactDigest(), file.GetMaterializeFile().GetSha256()) {
		return nil, errs.New(errs.KindValidationFailed, "Route execution plan differs from its captured provider")
	}
	return plan, nil
}
