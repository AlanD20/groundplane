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

// PrepareBackupFailure is called only after the executor has returned. Pending
// command receipts must be delivered first; losing their acknowledgement cannot
// turn an executed command into a different failure history.
func (journal *Journal) PrepareBackupFailure(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	attempt workexecution.Attempt, reason executionplan.BackupFailureReason, at time.Time,
) (*BackupRecord, error) {
	owned, decoded, err := executionplan.OpenBackupWorkAssignment(assignment, attempt.Executor)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(decoded, attempt) || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		!reason.Valid() || at.Before(*attempt.StartedAt) {
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
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return nil, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return nil, conflict()
		}
		state = &attemptRecord{Attempt: attempt, Sequence: 1, Backup: &BackupRecord{Sources: make([]BackupSourceReport, len(owned.Input.Steps))}}
	} else if err := validateBackupState(state, attempt, owned.Input); err != nil {
		return nil, err
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
	for _, source := range state.Backup.Sources {
		if len(source.Request) != 0 && len(source.Acknowledgement) == 0 {
			return nil, conflict()
		}
	}
	state.Backup.FailureReason = reason
	state.Sequence++
	state.UpdatedAt = at.UTC()
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Backup, nil
}

func (journal *Journal) AcknowledgeBackupFailure(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackupWorkInput, ack *agentpb.BackupWorkFailureAck,
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
		return terminal, nil
	}
	state.Backup.FailureAck, state.Backup.Completion = raw, &terminal
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
