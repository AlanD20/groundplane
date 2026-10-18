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

func (journal *Journal) PrepareBackupRecoveryCheckpoint(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	attempt workexecution.Attempt, request *agentpb.BackupWorkCheckpointRequest, at time.Time,
) (*BackupRecord, error) {
	report, err := executionplan.ValidateBackupRestoreRecoveryCheckpoint(request, assignment, attempt.Executor)
	if err != nil {
		return nil, err
	}
	if len(assignment.Sources) != 1 || report.SourceOrdinal != 0 {
		return nil, invalid()
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(report)
	if err != nil {
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
	if state.Backup.Completion != nil || state.Backup.FailureReason != "" || len(state.Backup.Recovery.Report) != 0 ||
		at.Before(state.UpdatedAt) || state.Backup.Recovery.CheckpointWrites >= ^uint64(0)-2 {
		return nil, conflict()
	}
	saved := state.Backup.Recovery.Checkpoint
	if bytes.Equal(saved.Request, raw) {
		return state.Backup, nil
	}
	sequence, fence := backupResumeFence(assignment.Sources[0])
	if len(saved.Request) != 0 {
		ack, err := saved.Ack()
		if err != nil || ack == nil {
			return nil, conflict()
		}
		sequence, fence = ack.CheckpointSequence, ack.Committed
	}
	if sequence == ^uint64(0) || report.CheckpointSequence != sequence+1 || !proto.Equal(report.PrecedingCheckpoint, fence) {
		return nil, conflict()
	}
	state.Backup.Recovery.Checkpoint = BackupSourceReport{Request: raw}
	state.Backup.Recovery.CheckpointWrites++
	state.Sequence++
	state.UpdatedAt = at.UTC()
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Backup, nil
}

func (journal *Journal) AcknowledgeBackupRecoveryCheckpoint(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	attempt workexecution.Attempt, ack *agentpb.BackupWorkCheckpointAck,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if err := matchingBackupRecovery(state, attempt); err != nil {
		return err
	}
	saved := &state.Backup.Recovery.Checkpoint
	request, err := saved.Report()
	if err != nil || request == nil {
		return conflict()
	}
	validated, err := executionplan.ValidateBackupRestoreRecoveryCheckpointAck(ack, request, assignment, attempt.Executor)
	if err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(validated)
	if err != nil {
		return invalid()
	}
	if len(saved.Acknowledgement) != 0 {
		if !bytes.Equal(saved.Acknowledgement, raw) {
			return conflict()
		}
		return nil
	}
	if state.Backup.FailureReason != "" || state.Backup.Completion != nil || len(state.Backup.Recovery.Report) != 0 {
		return conflict()
	}
	saved.Acknowledgement = raw
	return journal.writeRecord(ctx, *state)
}
