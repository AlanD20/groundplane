package taskassignments

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RollingEventBudget also handles an exact repeated start acknowledgement. It
// can return only the still-in-flight phase's original budget, never permission
// to execute an already completed phase again.
func RollingEventBudget(record RollingReleaseRecord, member *agentpb.CandidateReleaseMember,
	event taskjournal.TaskEventInput,
) (*rollingrelease.StepBudget, error) {
	if event.State != taskjournal.TaskEventStateRunning || event.Identity.Ordinal != 1 {
		return nil, nil
	}
	if record.Failed || record.CompletionProven {
		return nil, errs.New(errs.KindStateConflict, "rolling phase is no longer authorized to execute")
	}
	ordinal, phase, err := executionplan.RollingStepPosition(member, event.Identity.StepID)
	if err != nil {
		return nil, err
	}
	if ordinal == 0 {
		if !record.FinalVerificationStarted || record.Progress.Phase != "" || record.Progress.InFlight {
			return nil, errs.New(errs.KindStateConflict, "rolling final verification has no accepted start")
		}
		return &rollingrelease.StepBudget{Deadline: record.TaskDeadline}, nil
	}
	if record.Progress.Change != ordinal || record.Progress.Phase != phase || !record.Progress.InFlight {
		return nil, errs.New(
			errs.KindStateConflict,
			"rolling start acknowledgement no longer matches the current phase",
		)
	}
	budget := &rollingrelease.StepBudget{Deadline: record.Progress.ReplicaDeadline}
	if phase == rollingrelease.DrainPredecessor {
		budget.DrainUntil = record.Progress.DrainUntil
	}
	return budget, nil
}
