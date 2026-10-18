package backingchecks

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Stop joins native Task terminal publication. It releases no unresolved helper
// authority and never turns an uncertain probe into successful access evidence.
func (record *Record) Stop(failure string, now time.Time) error {
	if record.Phase != Ready && record.Phase != Checking && record.Phase != Cleaning ||
		!validTime(now) || !validFailure(failure) || failure == "" {
		return invalidRecord()
	}
	now = record.settlementTime(now)
	record.Clear()
	record.Phase, record.Outcome, record.Failure, record.FinishedAt = Finished, Failed, failure, &now
	if record.CleanupPending {
		record.Phase = CleanupRequired
	}
	return record.Validate()
}

func (record Record) settlementTime(now time.Time) time.Time {
	if now.Before(record.CreatedAt) {
		now = record.CreatedAt
	}
	if record.StartedAt != nil && now.Before(*record.StartedAt) {
		now = *record.StartedAt
	}
	return now
}

func (record *Record) start(now, deadline time.Time) error {
	if record.Phase != Ready || !validTime(now) || !now.Before(deadline) {
		return errs.New(errs.KindStateConflict, "Backing access check cannot restart or exceed its deadline")
	}
	now = record.settlementTime(now)
	if !now.Before(deadline) {
		return errs.New(errs.KindStateConflict, "Backing access check deadline has expired")
	}
	record.Clear()
	record.Phase, record.StartedAt, record.CleanupPending = Checking, &now, true
	return nil
}

func (record *Record) observe(result *backingaccess.Result, pending bool, failure string) error {
	if record.Phase != Checking || record.ProbeRecorded || !validFailure(failure) || pending && failure == "" ||
		result == nil && failure == "" {
		return errs.New(errs.KindStateConflict, "Backing access result does not belong to a forward check")
	}
	if result != nil {
		if result.ValidateFor(record.Source.Parameters) != nil {
			return invalidRecord()
		}
		copyOf := *result
		copyOf.Stages = append([]backingaccess.Stage(nil), result.Stages...)
		record.Observation = &copyOf
		if failure == "" {
			for _, stage := range result.Stages {
				if stage.Outcome == backingaccess.Failed {
					failure = string(stage.Failure)
					break
				}
			}
			if failure == "" && !observationPassed(result) {
				failure = string(backingaccess.ProtocolFailure)
			}
		}
	}
	record.CleanupPending, record.ProbeRecorded, record.ProbeFailure = pending, true, failure
	return nil
}

func (record *Record) finish(failure string, now time.Time) error {
	if record.Phase != Checking && record.Phase != Cleaning || record.CleanupPending ||
		!validTime(now) || !validFailure(failure) {
		return invalidRecord()
	}
	now = record.settlementTime(now)
	outcome := Failed
	if record.Phase == Cleaning {
		outcome, failure = Interrupted, InterruptedFailure
	} else if record.ProbeRecorded && failure != record.ProbeFailure {
		return errs.New(errs.KindStateConflict, "Backing access settlement changed its recorded probe failure")
	} else if failure == "" {
		if !observationPassed(record.Observation) {
			return errs.New(errs.KindStateConflict, "Backing access check has no passing access result")
		}
		outcome = Passed
	}
	record.Clear()
	record.Phase, record.Outcome, record.Failure, record.FinishedAt = Finished, outcome, failure, &now
	return nil
}
