package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) CompleteBackup(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.BackupWorkInput,
	ack *agentpb.BackupWorkCompletionAck,
) (workexecution.Attempt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := validateBackupState(state, attempt, input); err != nil {
		return workexecution.Attempt{}, err
	}
	if state.Backup.FailureReason != "" {
		return workexecution.Attempt{}, conflict()
	}
	evidence, err := state.Backup.CompletionEvidence(attempt.Input.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := executionplan.DecodeBackupWorkCompletion(attempt, evidence, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if previous := state.Backup.Completion; previous != nil {
		if previous.Outcome != terminal.Outcome || !previous.FinishedAt.Equal(*terminal.FinishedAt) ||
			*previous.Result != *terminal.Result {
			return workexecution.Attempt{}, conflict()
		}
		return *previous, nil
	}
	state.Backup.Completion = &terminal
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
