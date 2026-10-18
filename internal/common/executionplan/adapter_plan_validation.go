package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/adapterprocedure"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateAdapterProcedure(operation agentpb.PlanOperation, procedure *agentpb.AdapterProcedure) error {
	if err := adapterprocedure.Validate(procedure); err != nil {
		return err
	}
	switch procedure.Phase {
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION, agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT:
		if operation == agentpb.PlanOperation_PLAN_OPERATION_ATTACH || operation == agentpb.PlanOperation_PLAN_OPERATION_RECONCILE ||
			operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
			return nil
		}
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE, agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH:
		if operation == agentpb.PlanOperation_PLAN_OPERATION_DETACH {
			return nil
		}
	}
	return errs.New(errs.KindValidationFailed, "adapter procedure does not belong to this operation")
}
