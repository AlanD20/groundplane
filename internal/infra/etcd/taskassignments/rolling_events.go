package taskassignments

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// AdvanceRollingEvent is persisted in the same CAS as its Task event. The
// accepted first Running event starts the budget before any replica mutation;
// later phases, readiness updates and duplicate delivery cannot reset it.
func AdvanceRollingEvent(record RollingReleaseRecord, assignment TaskAssignmentRecord,
	member *agentpb.CandidateReleaseMember, event taskjournal.TaskEventInput, receivedAt time.Time,
) (RollingReleaseRecord, bool, error) {
	if err := ValidateRollingReleaseAssignment(record, assignment); err != nil {
		return RollingReleaseRecord{}, false, err
	}
	if event.Identity.TaskID != record.TaskID || event.Identity.AssignmentID != record.AssignmentID ||
		event.Identity.Attempt != record.ExecutionEpoch || receivedAt.Before(assignment.AssignedAt) {
		return RollingReleaseRecord{}, false, errs.New(
			errs.KindStateConflict,
			"rolling event does not match its execution identity",
		)
	}
	plan, err := executionplan.OpenRollingPlan(member)
	if err != nil {
		return RollingReleaseRecord{}, false, err
	}
	digest, err := RollingProcedureSHA256(plan)
	if err != nil || digest != record.ProcedureSHA256 {
		return RollingReleaseRecord{}, false, errs.New(
			errs.KindStateConflict,
			"rolling event procedure differs from its captured execution",
		)
	}
	ordinal, phase, err := executionplan.RollingStepPosition(member, event.Identity.StepID)
	if err != nil {
		return RollingReleaseRecord{}, false, err
	}
	if record.Failed || record.CompletionProven {
		return RollingReleaseRecord{}, false, errs.New(
			errs.KindStateConflict,
			"rolling forward execution has already failed or completed",
		)
	}
	if ordinal == 0 {
		if record.Progress.Phase != "" || record.Progress.InFlight {
			return RollingReleaseRecord{}, false, errs.New(
				errs.KindStateConflict,
				"rolling Release cannot complete before every replica change",
			)
		}
	} else if record.Progress.Change != ordinal || record.Progress.Phase != phase {
		return RollingReleaseRecord{}, false, errs.New(errs.KindStateConflict, "rolling event is out of replica or phase order")
	}
	next := CloneRollingReleaseRecord(record)
	switch event.State {
	case taskjournal.TaskEventStateRunning:
		if ordinal == 0 {
			if !receivedAt.Before(record.TaskDeadline) {
				return RollingReleaseRecord{}, false, errs.New(
					errs.KindStateConflict,
					"rolling Task deadline expired before final verification",
				)
			}
			if event.Identity.Ordinal == 1 && !next.FinalVerificationStarted {
				next.FinalVerificationStarted = true
				return next, true, ValidateRollingReleaseRecord(next)
			}
			if event.Identity.Ordinal == 1 || !next.FinalVerificationStarted {
				return RollingReleaseRecord{}, false, errs.New(
					errs.KindStateConflict,
					"rolling final verification has an invalid start sequence",
				)
			}
			return next, false, nil
		}
		if event.Identity.Ordinal != 1 {
			if !next.Progress.InFlight {
				return RollingReleaseRecord{}, false, errs.New(
					errs.KindStateConflict,
					"rolling phase progress has no accepted start",
				)
			}
			return next, false, nil
		}
		next.Progress, err = next.Progress.Begin(next.Plan, next.TaskDeadline, receivedAt)
	case taskjournal.TaskEventStateCompleted:
		if ordinal == 0 {
			if !record.FinalVerificationStarted {
				return RollingReleaseRecord{}, false, errs.New(
					errs.KindStateConflict,
					"rolling completion has no accepted final verification start",
				)
			}
			if !receivedAt.Before(record.TaskDeadline) {
				return RollingReleaseRecord{}, false, errs.New(
					errs.KindStateConflict,
					"rolling completion arrived after its Task deadline",
				)
			}
			next.CompletionProven = true
		} else {
			next.Progress, err = next.Progress.Confirm(next.Plan, next.TaskDeadline, phase, receivedAt)
		}
	case taskjournal.TaskEventStateFailed, taskjournal.TaskEventStateAborted, taskjournal.TaskEventStateTimedOut:
		next.Failed = true // Preserve the uncertain cursor for recovery; never advance it.
	default:
		return RollingReleaseRecord{}, false, errs.New(
			errs.KindStateConflict,
			"rolling event state cannot advance execution",
		)
	}
	if err != nil {
		return RollingReleaseRecord{}, false, err
	}
	return next, true, ValidateRollingReleaseRecord(next)
}
