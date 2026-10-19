package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// RetireAcknowledgedScript selects only an authenticated terminal receipt saved
// by the delivery path. It never infers completion from the worker returning.
func (journal *Journal) RetireAcknowledgedScript(ctx context.Context, attempt workexecution.Attempt) error {
	if attempt.Validate() != nil {
		return invalid()
	}
	terminal, err := journal.scriptTerminal(ctx, attempt)
	if err != nil || terminal == nil {
		return err
	}
	return journal.Retire(ctx, *terminal)
}

func (journal *Journal) scriptTerminal(
	ctx context.Context,
	attempt workexecution.Attempt,
) (*workexecution.Attempt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	record, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || record == nil {
		return nil, err
	}
	if !workexecution.SameAssignment(record.Attempt, attempt) {
		return nil, conflict()
	}
	if record.ScriptFailure != nil && record.ScriptFailure.Receipt != nil {
		return record.ScriptFailure.Receipt, nil
	}
	if record.ScriptRecovery != nil && record.ScriptRecovery.Receipt != nil {
		return record.ScriptRecovery.Receipt, nil
	}
	if record.ScriptCompletion != nil {
		return record.ScriptCompletion, nil
	}
	return nil, conflict()
}
