package agent

import (
	"bytes"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func hasServingPredecessorAuthority(authority *agentpb.ReleaseRestorationAuthority, serviceID string) bool {
	for _, witness := range authority.GetNativePredecessors() {
		if witness.GetServiceId() == serviceID {
			return len(witness.GetCurrentArtifact()) != 0
		}
	}
	return false
}

func validateCandidateReleaseAssignmentAuthority(assignment taskassignment.Assignment, plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetCandidateReleaseProcedure()
	if procedure == nil {
		if assignment.RestorationAuthority != nil || assignment.ReleaseRecoveryDirective != nil ||
			len(assignment.ReleaseRecoveryRecordSHA256) != 0 {
			return errs.New(errs.KindInternal, "agent: non-release assignment carries restoration authority")
		}
		return nil
	}
	authority := assignment.RestorationAuthority
	if authority == nil || len(authority.GetPlanHash()) != 32 ||
		!bytes.Equal(authority.GetPlanHash(), plan.GetPlanHash()) ||
		len(authority.GetAuthoritySha256()) != 32 ||
		authority.GetTaskId() != assignment.TaskID ||
		authority.GetOperationId() != assignment.OperationID ||
		len(authority.GetCandidates()) != len(procedure.GetMembers()) {
		return errs.New(errs.KindInternal, "agent: candidate Release restoration authority is invalid")
	}
	if len(authority.GetNativePredecessors()) != len(procedure.GetMembers()) {
		return errs.New(errs.KindInternal, "agent: native predecessor authority is incomplete")
	}
	if err := executionplan.ValidateNativeRestorationAuthority(plan, authority); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	for index, member := range procedure.GetMembers() {
		candidate := authority.GetCandidates()[index]
		if candidate.GetServiceId() != member.GetServiceId() ||
			candidate.GetReleaseId() != member.GetCandidateReleaseId() ||
			authority.GetCandidateArtifactId() != member.GetCandidateArtifactId() {
			return errs.New(errs.KindInternal, "agent: candidate Release restoration members diverge")
		}
		switch candidate.GetTarget() {
		case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR:
			selected := member.GetServingPredecessor()
			if selected == nil || !hasServingPredecessorAuthority(authority, member.GetServiceId()) {
				return errs.New(errs.KindInternal, "agent: serving predecessor authority is incomplete")
			}
		case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE:
			selected := member.GetCandidateAbsence()
			if selected == nil {
				return errs.New(errs.KindInternal, "agent: candidate absence authority is incomplete")
			}
		default:
			return errs.New(errs.KindInternal, "agent: candidate Release restoration target is invalid")
		}
	}
	if assignment.ExecutionMode != agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
		return nil
	}
	return validateReleaseRecoveryDirective(procedure, assignment.ReleaseRecoveryDirective)
}

func validateReleaseRecoveryDirective(
	procedure *agentpb.CandidateReleaseProcedure,
	directive *agentpb.ReleaseRecoveryDirective,
) error {
	selectedStepIDs := executionplan.RecoveryStepIDs(procedure)
	probeCount := len(selectedStepIDs) / 2
	if directive.GetCursor() > uint32(len(directive.GetStepIds())) ||
		len(directive.GetStepIds()) != len(selectedStepIDs) {
		return errs.New(errs.KindInternal, "agent: candidate Release recovery cursor is invalid")
	}
	for index := range selectedStepIDs {
		if directive.GetStepIds()[index] != selectedStepIDs[index] {
			return errs.New(errs.KindInternal, "agent: candidate Release recovery procedure diverges")
		}
	}
	applicableIndex := 0
	for _, compensationStepID := range selectedStepIDs[probeCount:] {
		if applicableIndex < len(directive.GetApplicableCompensationStepIds()) &&
			directive.GetApplicableCompensationStepIds()[applicableIndex] == compensationStepID {
			applicableIndex++
		}
	}
	if applicableIndex != len(directive.GetApplicableCompensationStepIds()) {
		return errs.New(errs.KindInternal, "agent: recovery compensation obligation is not canonical")
	}
	switch directive.GetPhase() {
	case agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE:
		if int(directive.GetCursor()) >= probeCount {
			return errs.New(errs.KindInternal, "agent: recovery probe phase cursor is invalid")
		}
	case agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_COMPENSATE:
		if int(directive.GetCursor()) < probeCount ||
			int(directive.GetCursor()) >= len(selectedStepIDs) {
			return errs.New(errs.KindInternal, "agent: recovery compensation phase cursor is invalid")
		}
	case agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN:
		if int(directive.GetCursor()) != len(selectedStepIDs) {
			return errs.New(errs.KindInternal, "agent: recovery proven phase cursor is invalid")
		}
	default:
		return errs.New(errs.KindInternal, "agent: recovery assignment phase is invalid")
	}
	return nil
}
