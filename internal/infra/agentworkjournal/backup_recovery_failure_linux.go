package agentworkjournal

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (journal *Journal) PrepareBackupRecoveryFailure(ctx context.Context, attempt workexecution.Attempt,
	reason executionplan.BackupFailureReason, at time.Time,
) (*BackupRecord, error) {
	if !reason.Valid() {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	if err := matchingBackupRecovery(state, attempt); err != nil {
		return nil, err
	}
	if len(state.Backup.Recovery.Report) != 0 {
		return nil, conflict()
	}
	if state.Backup.FailureReason != "" {
		if state.Backup.FailureReason != reason {
			return nil, conflict()
		}
		return state.Backup, nil
	}
	if state.Backup.Completion != nil || at.Before(state.UpdatedAt) || state.Sequence == ^uint64(0) {
		return nil, conflict()
	}
	state.Backup.FailureReason, state.Sequence, state.UpdatedAt = reason, state.Sequence+1, at.UTC()
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Backup, nil
}

func (journal *Journal) AcknowledgeBackupRecoveryFailure(ctx context.Context, attempt workexecution.Attempt,
	ack *agentpb.BackupWorkFailureAck,
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
	if err := matchingBackupRecovery(state, attempt); err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := executionplan.DecodeBackupWorkFailureAck(attempt, state.Backup.FailureReason, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(ack)
	if err != nil {
		return workexecution.Attempt{}, invalid()
	}
	if len(state.Backup.FailureAck) != 0 {
		if !bytes.Equal(raw, state.Backup.FailureAck) {
			return workexecution.Attempt{}, conflict()
		}
		return *state.Backup.Completion, nil
	}
	state.Backup.FailureAck, state.Backup.Completion = raw, &terminal
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
