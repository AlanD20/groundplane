package agentworkjournal

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (record attemptRecord) validateReleaseRecovery() error {
	saved := record.Recovery
	if saved == nil || record.Release != nil || record.Completion != nil || record.Effect != nil ||
		saved.Report.Sequence > record.Sequence || saved.Report.Progress.UpdatedAt.After(record.UpdatedAt) {
		return invalid()
	}
	input := saved.input(record.Attempt)
	if err := saved.Report.ValidateEnvelope(input); err != nil {
		return err
	}
	maximum, pending := saved.Report.Sequence, 0
	if saved.Report.Ack == nil {
		pending++
		if maximum != record.Sequence {
			return invalid()
		}
	}
	if maximum == record.Sequence && !saved.Report.Progress.UpdatedAt.Equal(record.UpdatedAt) {
		return invalid()
	}
	script, err := record.script()
	if err != nil {
		return err
	}
	if script != nil {
		if !workexecution.SameAssignment(script.Attempt, record.Attempt) ||
			script.Report.Sequence > record.Sequence || script.Report.Sequence == saved.Report.Sequence ||
			script.Report.RecordedAt.AsTime().After(record.UpdatedAt) {
			return invalid()
		}
		if script.Report.Sequence > maximum {
			if saved.Report.Ack == nil || !saved.Report.Progress.InFlight ||
				saved.Report.Progress.Phase != releaseexecution.FailureHooks {
				return invalid()
			}
			maximum = script.Report.Sequence
		}
		if script.Report.Sequence == record.Sequence && !script.Report.RecordedAt.AsTime().Equal(record.UpdatedAt) {
			return invalid()
		}
		if script.Ack == nil {
			pending++
			if script.Report.Sequence != record.Sequence {
				return invalid()
			}
		}
	}
	if maximum != record.Sequence || pending > 1 {
		return invalid()
	}
	if failure := record.Failure; failure != nil {
		if saved.Completion != nil || saved.Report.Progress.Phase == releaseexecution.RecoveryComplete ||
			(failure.Reason != releaseexecution.ExecutionFailed && failure.Reason != releaseexecution.DeadlineExpired && failure.Reason != releaseexecution.ExecutionInterrupted) ||
			failure.Receipt != nil && failure.Receipt.Validate(record.Attempt, failure.Reason) != nil {
			return invalid()
		}
	}
	if effect := saved.Effect; effect != nil {
		if _, err := releaseexecution.DecodeRecoveryEffectEnvelope(input, effect.Boundary, effect.Raw, effect.Reference); err != nil {
			return err
		}
		if effect.Boundary.Sequence > record.Sequence ||
			!effect.Acknowledged && (pending != 0 || saved.Report.Ack == nil ||
				effect.Boundary.Progress.Sequence != saved.Report.Progress.Sequence) {
			return invalid()
		}
	}
	if receipt := saved.Completion; receipt != nil {
		if saved.Report.Ack == nil || saved.Report.Progress.Phase != releaseexecution.RecoveryComplete ||
			saved.Report.Progress.InFlight || saved.Effect == nil || !saved.Effect.Acknowledged || pending != 0 ||
			saved.Report.Sequence != record.Sequence || receipt.Outcome != workexecution.Succeeded ||
			receipt.FinishedAt.IsZero() || receipt.FinishedAt.Location() != time.UTC || receipt.FinishedAt.Before(record.UpdatedAt) {
			return invalid()
		}
	}
	return nil
}
