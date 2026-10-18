package executionplan

import (
	"bytes"
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Detach recovery completes only the original closed network/adapter cleanup.
// The plan hash pins its artifacts, credentials and optional Custom Detach hook.
// Hook checkpoints separately gate any repeated operator-authored execution.
func AttachRemovalRecoveryStepIDs(plan *agentpb.ExecutionPlan) ([]string, error) {
	if plan == nil || plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_DETACH ||
		len(plan.GetSteps()) == 0 || len(plan.GetSteps()) > 10 ||
		plan.GetCandidateReleaseProcedure() != nil ||
		plan.GetServiceAbsenceRecoveryProcedure() != nil || plan.GetManagedNetworkRecoveryProcedure() != nil {
		return nil, errs.New(errs.KindStateConflict, "Detach recovery requires its original compiled cleanup plan")
	}
	steps := make([]string, len(plan.GetSteps()))
	network := -1
	for index, step := range plan.GetSteps() {
		if step == nil || step.GetStepId() == "" || slices.Contains(steps[:index], step.GetStepId()) {
			return nil, errs.New(errs.KindStateConflict, "Detach recovery step identity is invalid")
		}
		steps[index] = step.GetStepId()
		if hook := step.GetBackingHookProcedure(); hook != nil {
			if index != 0 || len(plan.GetSteps()) != 2 ||
				hook.GetEvent() != agentpb.BackingHookEvent_BACKING_HOOK_EVENT_DETACH ||
				hook.GetAttachId() != plan.GetTargetId() {
				return nil, errs.New(errs.KindStateConflict, "Detach recovery hook does not match its captured target")
			}
			continue
		}
		if step.GetComposeApply() != nil {
			if network >= 0 {
				return nil, errs.New(errs.KindStateConflict, "Detach recovery has more than one network update")
			}
			network = index
			continue
		}
		procedure := step.GetAdapterProcedure()
		if procedure == nil {
			return nil, errs.New(errs.KindStateConflict, "Detach recovery cannot repeat operator hooks or commands")
		}
		switch procedure.GetPhase() {
		case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE:
			if network >= 0 {
				return nil, errs.New(errs.KindStateConflict, "Detach grant revocation follows its network withdrawal")
			}
		case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH:
			if index != len(plan.GetSteps())-1 || network != index-1 {
				return nil, errs.New(errs.KindStateConflict, "Detach credential cleanup is not its final step")
			}
		default:
			return nil, errs.New(errs.KindStateConflict, "Detach recovery cannot provision backing credentials")
		}
	}
	hooked := plan.GetSteps()[0].GetBackingHookProcedure() != nil
	if network < 0 || hooked && network != len(steps)-1 || !hooked && len(steps) != 1 && network != len(steps)-2 {
		return nil, errs.New(errs.KindStateConflict, "Detach recovery network withdrawal is missing")
	}
	return steps, nil
}

func ValidateAttachRemovalRecoveryDirective(
	plan *agentpb.ExecutionPlan, directive *agentpb.ReleaseRecoveryDirective, recordDigest []byte,
) error {
	steps, err := AttachRemovalRecoveryStepIDs(plan)
	if err != nil {
		return err
	}
	if directive == nil || len(recordDigest) != 32 ||
		!bytes.Equal(directive.GetReleaseRecoveryRecordSha256(), recordDigest) ||
		!slices.Equal(directive.GetStepIds(), steps) || directive.GetCursor() > uint32(len(steps)) ||
		len(directive.GetApplicableCompensationStepIds()) != 0 ||
		directive.GetCursor() < uint32(len(steps)) &&
			directive.GetPhase() != agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_COMPENSATE ||
		directive.GetCursor() == uint32(len(steps)) &&
			directive.GetPhase() != agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN {
		return errs.New(errs.KindStateConflict, "Detach recovery cursor does not match the captured cleanup plan")
	}
	return nil
}
