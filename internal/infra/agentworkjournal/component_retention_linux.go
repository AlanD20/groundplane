package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// retireComponent retains every failed forward checkpoint until the exact
// predecessor-restoration receipt arrives, including between-stage failures.
func (journal *Journal) retireComponent(
	ctx context.Context,
	record attemptRecord,
	terminal workexecution.Attempt,
) (bool, error) {
	if recovery := record.ComponentRecovery; recovery != nil {
		receipt := recovery.Receipt
		if recovery.Failure != nil {
			receipt = recovery.Failure.Receipt
		}
		if !sameComponentReceipt(receipt, terminal) {
			return false, conflict()
		}
		if terminal.Outcome != workexecution.Succeeded {
			return false, nil
		}
		forward, err := journal.readRecord(ctx, recovery.Source.Forward.ID)
		if err != nil {
			return false, err
		}
		if forward != nil {
			if forward.Component == nil || forward.Component.Completion != nil ||
				forward.Component.Layout != recovery.Restoration.Layout || !workexecution.SameAssignment(forward.Attempt, recovery.Source.Forward) {
				return false, conflict()
			}
			if failure := forward.Component.Failure; failure != nil && failure.Receipt != nil &&
				!sameComponentReceipt(failure.Receipt, recovery.Source.Forward) {
				return false, conflict()
			}
			// Remove forward evidence first; the recovery receipt remains
			// replayable if retirement is interrupted here.
			if err := journal.removeRecord(ctx, recovery.Source.Forward.ID); err != nil {
				return false, err
			}
		}
	}
	if component := record.Component; component != nil {
		receipt := component.Completion
		if component.Failure != nil {
			receipt = component.Failure.Receipt
		}
		if !sameComponentReceipt(receipt, terminal) {
			return false, conflict()
		}
		if component.Failure != nil {
			failure, err := componentexecution.DecodeLayoutFailure(
				terminal,
				component.Layout,
				component.Failure.Evidence,
				*terminal.Result,
			)
			if err != nil {
				return false, err
			}
			if failure.Checkpoint != nil {
				return true, nil
			}
		}
	}
	return false, nil
}

func sameComponentReceipt(saved *workexecution.Attempt, terminal workexecution.Attempt) bool {
	return saved != nil && terminal.Validate() == nil && terminal.FinishedAt != nil && terminal.Result != nil &&
		workexecution.SameAssignment(*saved, terminal) && saved.StartedAt != nil && terminal.StartedAt != nil &&
		saved.StartedAt.Equal(*terminal.StartedAt) && saved.FinishedAt != nil && saved.Result != nil &&
		saved.Outcome == terminal.Outcome && saved.FinishedAt.Equal(*terminal.FinishedAt) && *saved.Result == *terminal.Result
}
