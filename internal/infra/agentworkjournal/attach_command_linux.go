package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
)

// SaveAttachCommandResult preserves a successful return before sending Confirm.
// It does not resolve the crash window between the external effect and this
// fsynced write: missing result remains ambiguous and cannot be auto-replayed.
func (journal *Journal) SaveAttachCommandResult(ctx context.Context, input attachexecution.RemovalInput,
	result attachexecution.RemovalCommandResult,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return err
	}
	if !matchesAttachState(state, input) || !state.AttachRemoval.Acknowledged || state.AttachRemoval.Completion != nil {
		return conflict()
	}
	progress := state.AttachRemoval.Report.Progress
	if _, err := progress.Confirm(input, result, result.FinishedAt); err != nil {
		return err
	}
	if prior := state.AttachRemoval.CommandResult; prior != nil {
		if !attachexecution.SameRemovalCommandResult(*prior, result) {
			return conflict()
		}
		return nil
	}
	state.AttachRemoval.CommandResult = &result
	return journal.writeRecord(ctx, *state)
}
