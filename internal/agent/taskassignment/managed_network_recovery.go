package taskassignment

import (
	"bytes"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ValidateManagedNetworkRecoveryAuthority(assignment Assignment, plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetManagedNetworkRecoveryProcedure()
	if procedure == nil || len(executionplan.NetworkRecoveryForwardSteps(procedure)) == 0 ||
		len(executionplan.NetworkRecoverySettlementSteps(procedure)) == 0 {
		return errs.New(errs.KindInternal, "agent: managed network recovery procedure is invalid")
	}
	if assignment.RestorationAuthority != nil {
		return errs.New(errs.KindInternal, "agent: managed network assignment carries foreign recovery authority")
	}
	if assignment.ExecutionMode != agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
		if assignment.ReleaseRecoveryDirective != nil || len(assignment.ReleaseRecoveryRecordSHA256) != 0 {
			return errs.New(errs.KindInternal, "agent: forward managed network assignment carries recovery authority")
		}
		return nil
	}
	directive := assignment.ReleaseRecoveryDirective
	phase := agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_COMPENSATE
	if procedure.GetPreparation() != nil {
		phase = agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE
	}
	if directive == nil || len(assignment.ReleaseRecoveryRecordSHA256) != 32 ||
		!bytes.Equal(directive.GetReleaseRecoveryRecordSha256(), assignment.ReleaseRecoveryRecordSHA256) ||
		directive.GetPhase() != phase ||
		directive.GetCursor() != 0 ||
		!slices.Equal(directive.GetStepIds(), executionplan.NetworkRecoverySettlementSteps(procedure)) ||
		len(directive.GetApplicableCompensationStepIds()) != 0 {
		return errs.New(errs.KindInternal, "agent: managed network recovery authority is incomplete")
	}
	return nil
}
