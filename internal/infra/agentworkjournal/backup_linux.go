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

func (journal *Journal) PrepareBackup(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	attempt workexecution.Attempt, request *agentpb.BackupWorkCheckpointRequest, at time.Time,
) (*BackupRecord, error) {
	owned, decoded, err := executionplan.OpenBackupWorkAssignment(assignment, attempt.Executor)
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(decoded, attempt) || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		at.Before(*attempt.StartedAt) {
		return nil, invalid()
	}
	report, err := executionplan.ValidateBackupWorkCheckpoint(request, owned.Input, attempt.Executor)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(report.Attempt, owned.Attempt) {
		return nil, conflict()
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
	if err := validateBackupState(state, attempt, owned.Input); err != nil {
		return nil, err
	}
	if state.Backup.Completion != nil || state.Backup.FailureReason != "" {
		return nil, conflict()
	}
	if at.Before(state.UpdatedAt) || state.Sequence == ^uint64(0) {
		return nil, conflict()
	}
	source := state.Backup.Sources[report.SourceOrdinal]
	if bytes.Equal(source.Request, raw) {
		return state.Backup, nil
	}
	for _, saved := range state.Backup.Sources {
		if len(saved.Request) != 0 && len(saved.Acknowledgement) == 0 {
			return nil, conflict()
		}
	}
	sequence, fence := backupResumeFence(owned.Sources[report.SourceOrdinal])
	if len(source.Request) != 0 {
		ack, err := source.Ack()
		if err != nil || ack == nil {
			return nil, conflict()
		}
		sequence, fence = ack.CheckpointSequence, ack.Committed
	}
	if sequence == ^uint64(0) || report.CheckpointSequence != sequence+1 ||
		!proto.Equal(report.PrecedingCheckpoint, fence) {
		return nil, conflict()
	}
	state.Backup.Sources[report.SourceOrdinal] = BackupSourceReport{Request: raw}
	state.Sequence++
	state.UpdatedAt = at.UTC()
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Backup, nil
}

func (journal *Journal) AcknowledgeBackup(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackupWorkInput, ack *agentpb.BackupWorkCheckpointAck,
) error {
	if ack == nil {
		return invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if err := validateBackupState(state, attempt, input); err != nil {
		return err
	}
	if uint64(ack.SourceOrdinal) >= uint64(len(state.Backup.Sources)) {
		return conflict()
	}
	source := &state.Backup.Sources[ack.SourceOrdinal]
	request, err := source.Report()
	if err != nil || request == nil {
		return conflict()
	}
	validated, err := executionplan.ValidateBackupWorkCheckpointAck(ack, request, input, attempt.Executor)
	if err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(validated)
	if err != nil {
		return invalid()
	}
	if len(source.Acknowledgement) != 0 {
		if !bytes.Equal(source.Acknowledgement, raw) {
			return conflict()
		}
		return nil
	}
	source.Acknowledgement = raw
	return journal.writeRecord(ctx, *state)
}

// LoadBackup returns the saved start identity and pending report, not a new
// execution. The caller must replay before it crosses another command boundary.
func (journal *Journal) LoadBackup(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity,
) (*workexecution.Attempt, *BackupRecord, error) {
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return nil, nil, err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return nil, nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) {
		return nil, nil, conflict()
	}
	if err := validateBackupState(state, state.Attempt, owned.Input); err != nil {
		return nil, nil, err
	}
	return &state.Attempt, state.Backup, nil
}

func validateBackupState(state *attemptRecord, attempt workexecution.Attempt, input *agentpb.BackupWorkInput) error {
	if state == nil || state.Backup == nil || !workexecution.SameAssignment(state.Attempt, attempt) ||
		state.Attempt.StartedAt == nil || attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*attempt.StartedAt) {
		return conflict()
	}
	owned, err := executionplan.OpenBackupWorkInput(attempt, input)
	if err != nil {
		return err
	}
	if len(state.Backup.Sources) != len(owned.Steps) {
		return conflict()
	}
	for _, source := range state.Backup.Sources {
		request, err := source.Report()
		if err != nil {
			return err
		}
		if request == nil {
			continue
		}
		if _, err := executionplan.ValidateBackupWorkCheckpoint(request, owned, attempt.Executor); err != nil {
			return err
		}
		ack, err := source.Ack()
		if err != nil {
			return err
		}
		if ack != nil {
			if _, err := executionplan.ValidateBackupWorkCheckpointAck(ack, request, owned, attempt.Executor); err != nil {
				return err
			}
		}
	}
	return nil
}

func backupResumeFence(source *agentpb.BackupStepResume) (uint64, *agentpb.CheckpointFence) {
	switch {
	case source.GetCapture() != nil:
		return source.GetCapture().CheckpointSequence, source.GetCapture().PrecedingCheckpoint
	case source.GetRestore() != nil:
		return source.GetRestore().CheckpointSequence, source.GetRestore().PrecedingCheckpoint
	case source.GetPrune() != nil:
		return source.GetPrune().CheckpointSequence, source.GetPrune().PrecedingCheckpoint
	default:
		return ^uint64(0), nil
	}
}
