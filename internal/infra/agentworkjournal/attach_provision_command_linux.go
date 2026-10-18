package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
)

// SaveAttachProvisionCommandResult preserves a successful return before sending Confirm.
// It does not resolve the crash window between the external effect and this
// fsynced write: missing result remains ambiguous and cannot be auto-replayed.
func (journal *Journal) SaveAttachProvisionCommandResult(ctx context.Context, input attachexecution.ProvisionInput,
	result attachexecution.ProvisionCommandResult,
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
	if !matchesAttachProvisionState(state, input) || !state.AttachProvision.Acknowledged || state.AttachProvision.Completion != nil {
		return conflict()
	}
	progress := state.AttachProvision.Report.Progress
	if _, err := progress.Confirm(input.Attempt, state.AttachProvision.Authority, result, result.FinishedAt); err != nil {
		return err
	}
	if prior := state.AttachProvision.CommandResult; prior != nil {
		if !attachexecution.SameProvisionCommandResult(*prior, result) {
			return conflict()
		}
		return nil
	}
	state.AttachProvision.CommandResult = &result
	return journal.writeRecord(ctx, *state)
}
