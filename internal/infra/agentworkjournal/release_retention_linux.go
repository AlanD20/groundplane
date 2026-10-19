package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// retireRecoveredRelease runs under the journal lock after the exact cleanup
// completion was acknowledged. Its accepted quiescence proof accounts for the
// captured forward attempt and earlier retries of this same work/input helper.
// It never removes runtime files, application data or unrelated journals.
func (journal *Journal) retireRecoveredRelease(
	ctx context.Context,
	attempt workexecution.Attempt,
	recovery RecoveryRecord,
) error {
	input := recovery.input(attempt)
	if input.ValidateIdentity() != nil || recovery.Completion == nil ||
		recovery.Completion.Outcome != workexecution.Succeeded ||
		recovery.Report.Progress.Phase != releaseexecution.RecoveryComplete ||
		recovery.Report.Ack == nil {
		return conflict()
	}
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return err
	}
	// Validate the complete candidate set before removing any of it. Keeping the
	// current completion until last makes partial retirement restartable.
	retire := []string{recovery.Forward.ID}
	if recovery.Interruption != nil {
		retire = append(retire, recovery.Interruption.Attempt.ID)
	}
	for id, committed := range attempts {
		if !committed || id == attempt.ID {
			continue
		}
		state, err := journal.readRecord(ctx, id)
		if err != nil {
			return err
		}
		if state == nil {
			return conflict()
		}
		if id == recovery.Forward.ID {
			if !workexecution.SameAssignment(state.Attempt, recovery.Forward) ||
				state.Attempt.Executor.HostID != attempt.Executor.HostID {
				return conflict()
			}
			if state.Completion != nil && (recovery.Forward.Outcome != workexecution.Succeeded ||
				state.Completion.Outcome != recovery.Forward.Outcome || !state.Completion.FinishedAt.Equal(*recovery.Forward.FinishedAt)) {
				return conflict()
			}
			if recovery.Forward.Outcome == workexecution.Succeeded && (recovery.Group == nil || state.Release == nil ||
				state.Release.Ack == nil || state.Release.Ack.Evidence != *recovery.Forward.Result) {
				return conflict()
			}
			if state.Failure != nil && state.Failure.Receipt != nil &&
				(state.Failure.Receipt.Evidence != *recovery.Forward.Result ||
					state.Failure.Receipt.Outcome != recovery.Forward.Outcome) {
				return conflict()
			}
			continue
		}
		if recovery.Interruption != nil && id == recovery.Interruption.Attempt.ID {
			previous := recovery.Interruption.Attempt
			if !workexecution.SameAssignment(state.Attempt, previous) ||
				state.Failure != nil && state.Failure.Receipt != nil &&
					state.Failure.Receipt.Evidence != *previous.Result {
				return conflict()
			}
		}
		if state.Recovery == nil || state.Attempt.WorkItemID != attempt.WorkItemID {
			continue
		}
		prior := state.Recovery
		if state.Attempt.TaskID != attempt.TaskID || state.Attempt.Executor.HostID != attempt.Executor.HostID ||
			state.Attempt.Input != attempt.Input || state.Attempt.Number >= attempt.Number ||
			!workexecution.SameAssignment(prior.Forward, recovery.Forward) || prior.Forward.Result == nil ||
			*prior.Forward.Result != *recovery.Forward.Result || !releaseexecution.EqualGroupRestoration(prior.Group, recovery.Group) ||
			prior.Completion != nil && prior.Completion.Outcome == workexecution.Succeeded {
			return conflict()
		}
		retire = append(retire, id)
	}
	for _, id := range retire {
		if err := journal.removeRecord(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
