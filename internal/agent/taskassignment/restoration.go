package taskassignment

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func HasServingPredecessorAuthority(authority *agentpb.ReleaseRestorationAuthority, serviceID string) bool {
	for _, witness := range authority.GetNativePredecessors() {
		if witness.GetServiceId() == serviceID {
			return len(witness.GetCurrentArtifact()) != 0
		}
	}
	return false
}

func ValidateCandidateReleaseAuthority(assignment Assignment, plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetCandidateReleaseProcedure()
	if procedure == nil {
		if plan.GetServiceAbsenceRecoveryProcedure() != nil {
			return ValidateServiceAbsenceRecoveryAuthority(assignment, plan)
		}
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
			if selected == nil || !HasServingPredecessorAuthority(authority, member.GetServiceId()) {
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

func ValidateServiceAbsenceRecoveryAuthority(assignment Assignment, plan *agentpb.ExecutionPlan) error {
	if assignment.RestorationAuthority != nil || assignment.ComponentRecoveryDirective != nil ||
		len(assignment.ComponentRecoveryRecordSHA256) != 0 {
		return errs.New(errs.KindInternal, "agent: Service absence assignment carries foreign restoration authority")
	}
	if assignment.ExecutionMode != agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
		if assignment.ReleaseRecoveryDirective != nil || len(assignment.ReleaseRecoveryRecordSHA256) != 0 {
			return errs.New(errs.KindInternal, "agent: forward Service absence assignment carries recovery authority")
		}
		return nil
	}
	directive := assignment.ReleaseRecoveryDirective
	steps := executionplan.ServiceAbsenceRecoveryStepIDs(plan.GetServiceAbsenceRecoveryProcedure())
	if directive == nil || len(assignment.ReleaseRecoveryRecordSHA256) != 32 ||
		!bytes.Equal(directive.GetReleaseRecoveryRecordSha256(), assignment.ReleaseRecoveryRecordSHA256) ||
		directive.GetCursor() > uint32(len(steps)) || len(directive.GetStepIds()) != len(steps) {
		return errs.New(errs.KindInternal, "agent: Service absence recovery cursor is invalid")
	}
	for index := range steps {
		if directive.GetStepIds()[index] != steps[index] {
			return errs.New(errs.KindInternal, "agent: Service absence recovery procedure diverges")
		}
	}
	applicable := directive.GetApplicableCompensationStepIds()
	if len(applicable) > 1 || len(applicable) == 1 && applicable[0] != steps[1] {
		return errs.New(errs.KindInternal, "agent: Service absence compensation obligation is invalid")
	}
	switch directive.GetPhase() {
	case agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE:
		if directive.GetCursor() != 0 {
			return errs.New(errs.KindInternal, "agent: Service absence probe cursor is invalid")
		}
	case agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_COMPENSATE:
		if directive.GetCursor() != 1 {
			return errs.New(errs.KindInternal, "agent: Service absence compensation cursor is invalid")
		}
	case agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN:
		if directive.GetCursor() != 2 {
			return errs.New(errs.KindInternal, "agent: Service absence proof cursor is invalid")
		}
	default:
		return errs.New(errs.KindInternal, "agent: Service absence recovery phase is invalid")
	}
	return nil
}

func ValidateComponentRecoveryAuthority(assignment Assignment, plan *agentpb.ExecutionPlan) error {
	procedure := plan.GetComponentRecoveryProcedure()
	if procedure == nil {
		if assignment.ComponentRecoveryDirective != nil || len(assignment.ComponentRecoveryRecordSHA256) != 0 {
			return errs.New(errs.KindInternal, "agent: non-Component assignment carries Component recovery authority")
		}
		return nil
	}
	if assignment.RestorationAuthority != nil || assignment.ReleaseRecoveryDirective != nil ||
		len(assignment.ReleaseRecoveryRecordSHA256) != 0 {
		return errs.New(errs.KindInternal, "agent: Component assignment carries Release restoration authority")
	}
	if assignment.ExecutionMode != agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
		return nil
	}
	directive := assignment.ComponentRecoveryDirective
	selectedStepIDs := executionplan.ComponentRecoveryStepIDs(procedure)
	probeCount := len(selectedStepIDs) / 2
	if directive == nil || directive.GetCursor() > uint32(len(directive.GetStepIds())) ||
		len(directive.GetStepIds()) != len(selectedStepIDs) {
		return errs.New(errs.KindInternal, "agent: Component recovery cursor is invalid")
	}
	for index := range selectedStepIDs {
		if directive.GetStepIds()[index] != selectedStepIDs[index] {
			return errs.New(errs.KindInternal, "agent: Component recovery procedure diverges")
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
		return errs.New(errs.KindInternal, "agent: Component recovery obligation is not canonical")
	}
	switch directive.GetPhase() {
	case agentpb.ComponentRecoveryPhase_COMPONENT_RECOVERY_PHASE_PROBE:
		if int(directive.GetCursor()) >= probeCount {
			return errs.New(errs.KindInternal, "agent: Component recovery probe cursor is invalid")
		}
	case agentpb.ComponentRecoveryPhase_COMPONENT_RECOVERY_PHASE_COMPENSATE:
		if int(directive.GetCursor()) < probeCount || int(directive.GetCursor()) >= len(selectedStepIDs) {
			return errs.New(errs.KindInternal, "agent: Component recovery compensation cursor is invalid")
		}
	case agentpb.ComponentRecoveryPhase_COMPONENT_RECOVERY_PHASE_PROVEN:
		if int(directive.GetCursor()) != len(selectedStepIDs) {
			return errs.New(errs.KindInternal, "agent: Component recovery proven cursor is invalid")
		}
	default:
		return errs.New(errs.KindInternal, "agent: Component recovery phase is invalid")
	}
	return nil
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
