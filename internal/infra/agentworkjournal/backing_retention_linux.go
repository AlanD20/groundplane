package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (journal *Journal) retireRecoveredBacking(ctx context.Context, recovery BackingRecoveryRecord) error {
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return err
	}
	for id, committed := range attempts {
		if !committed || id == recovery.Receipt.ID || id == recovery.Source.Forward.ID {
			continue
		}
		state, err := journal.readRecord(ctx, id)
		if err != nil {
			return err
		}
		if state == nil || state.BackingRecovery == nil || state.Attempt.WorkItemID != recovery.Receipt.WorkItemID {
			continue
		}
		if state.Attempt.TaskID != recovery.Receipt.TaskID || state.Attempt.Input != recovery.Receipt.Input ||
			state.Attempt.Number >= recovery.Receipt.Number || state.BackingRecovery.Layout != recovery.Layout ||
			state.BackingRecovery.Source.Forward.ID != recovery.Source.Forward.ID || state.BackingRecovery.Source.Evidence != recovery.Source.Evidence ||
			state.BackingRecovery.Receipt != nil && state.BackingRecovery.Receipt.Outcome == workexecution.Succeeded {
			return conflict()
		}
		// Every cleanup retry shares one exact work/input/host helper name.
		// Joining it accounts for all older cleanup orphans, not only the
		// immediately preceding receipt retained in Controller authority.
		if err := journal.removeRecord(ctx, id); err != nil {
			return err
		}
	}
	source := recovery.Source.Forward
	state, err := journal.readRecord(ctx, source.ID)
	if err != nil || state == nil {
		return err
	}
	if !workexecution.SameAssignment(state.Attempt, source) || state.Backing == nil ||
		state.Backing.Layout != recovery.Layout ||
		state.Backing.Completion != nil ||
		state.Backing.Failure != nil && state.Backing.Failure.Receipt != nil &&
			!sameBackingReceipt(state.Backing.Failure.Receipt, source) {
		return conflict()
	}
	return journal.removeRecord(ctx, source.ID)
}
