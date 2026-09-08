package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateManagedNetworkEnsure(
	operation agentpb.PlanOperation,
	ensure *agentpb.ManagedNetworkEnsure,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if ensure == nil || validateID(ids.KindNetwork, ensure.GetNetworkId()) != nil ||
		(operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY && operation != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE && operation != agentpb.PlanOperation_PLAN_OPERATION_DEPLOY) {
		return errs.New(errs.KindValidationFailed, "managed network ensure payload is invalid")
	}
	artifact := artifacts[ensure.GetArtifactId()]
	if artifact == nil || artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
		return errs.New(errs.KindValidationFailed, "managed network ensure artifact must be Environment owned")
	}
	for _, network := range artifact.GetNetworks() {
		if network.GetNetworkId() == ensure.GetNetworkId() {
			return nil
		}
	}
	return errs.New(errs.KindValidationFailed, "managed network ensure selection is absent from artifact")
}
