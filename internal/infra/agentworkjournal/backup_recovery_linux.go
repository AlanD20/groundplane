package agentworkjournal

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (journal *Journal) LoadBackupRecovery(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	identity workexecution.Identity,
) (*workexecution.Attempt, *BackupRecord, error) {
	_, attempt, source, err := executionplan.OpenBackupRecoveryAssignment(assignment, identity)
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
	if err := matchingBackupRecovery(state, state.Attempt); err != nil {
		return nil, nil, err
	}
	left, err := executionplan.EncodeBackupWorkRecovery(state.Backup.Recovery.Source)
	if err != nil {
		return nil, nil, err
	}
	right, err := executionplan.EncodeBackupWorkRecovery(source)
	if err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(left, right) {
		return nil, nil, conflict()
	}
	request, err := state.Backup.Recovery.Checkpoint.Report()
	if err != nil {
		return nil, nil, err
	}
	if request != nil {
		if _, err := executionplan.ValidateBackupRestoreRecoveryCheckpoint(request, assignment, identity); err != nil {
			return nil, nil, err
		}
		ack, err := state.Backup.Recovery.Checkpoint.Ack()
		if err != nil {
			return nil, nil, err
		}
		if ack != nil {
			if _, err := executionplan.ValidateBackupRestoreRecoveryCheckpointAck(ack, request, assignment, identity); err != nil {
				return nil, nil, err
			}
		}
	}
	return &state.Attempt, state.Backup, nil
}

func (journal *Journal) PrepareBackupRecovery(ctx context.Context, attempt workexecution.Attempt,
	source executionplan.BackupWorkRecovery,
) (*BackupRecord, error) {
	if attempt.StartedAt == nil {
		return nil, invalid()
	}
	state := attemptRecord{Attempt: attempt, Sequence: 1, UpdatedAt: *attempt.StartedAt,
		Backup: &BackupRecord{Recovery: &BackupRecoveryRecord{Source: source}}}
	if err := state.validate(); err != nil {
		return nil, err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	previous, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		if err := matchingBackupRecovery(previous, attempt); err != nil {
			return nil, err
		}
		left, err := executionplan.EncodeBackupWorkRecovery(previous.Backup.Recovery.Source)
		if err != nil {
			return nil, err
		}
		right, err := executionplan.EncodeBackupWorkRecovery(source)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(left, right) {
			return nil, conflict()
		}
		return previous.Backup, nil
	}
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return nil, err
	}
	if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
		return nil, conflict()
	}
	if err := journal.writeRecord(ctx, state); err != nil {
		return nil, err
	}
	return state.Backup, nil
}

func matchingBackupRecovery(state *attemptRecord, attempt workexecution.Attempt) error {
	if state == nil || state.Backup == nil || state.Backup.Recovery == nil ||
		!workexecution.SameAssignment(state.Attempt, attempt) || state.Attempt.StartedAt == nil ||
		attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*attempt.StartedAt) {
		return conflict()
	}
	return nil
}

func (journal *Journal) SaveBackupRecoveryReport(ctx context.Context, attempt workexecution.Attempt,
	report executionplan.BackupRecoveryReport,
) ([]byte, error) {
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
	raw, _, err := executionplan.EncodeBackupRecoveryReport(attempt, state.Backup.Recovery.Source, report)
	if err != nil {
		return nil, err
	}
	if saved := state.Backup.Recovery.Report; len(saved) != 0 {
		if !bytes.Equal(saved, raw) {
			return nil, conflict()
		}
		return raw, nil
	}
	if state.Backup.Completion != nil || state.Sequence == ^uint64(0) || report.ObservedAt.Before(state.UpdatedAt) ||
		(len(state.Backup.Recovery.Checkpoint.Request) != 0 && len(state.Backup.Recovery.Checkpoint.Acknowledgement) == 0) {
		return nil, conflict()
	}
	state.Backup.Recovery.Report, state.Sequence, state.UpdatedAt = raw, state.Sequence+1, report.ObservedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return raw, nil
}

func (journal *Journal) AcknowledgeBackupRecovery(ctx context.Context, attempt workexecution.Attempt,
	ack *agentpb.BackupRecoveryAck,
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
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	report, evidence, err := executionplan.DecodeBackupRecoveryReport(attempt, state.Backup.Recovery.Source, state.Backup.Recovery.Report)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if ack == nil || executionplan.RejectUnknown(ack) != nil || !proto.Equal(identity, ack.Attempt) ||
		ack.Evidence == nil || ack.Evidence.InputId != evidence.ID || hex.EncodeToString(ack.Evidence.Sha256) != evidence.SHA256 ||
		ack.StartedAt == nil || ack.StartedAt.CheckValid() != nil || !ack.StartedAt.AsTime().Equal(*attempt.StartedAt) ||
		ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil || ack.FinishedAt.AsTime().Before(report.ObservedAt) {
		return workexecution.Attempt{}, conflict()
	}
	terminal := attempt
	finished := ack.FinishedAt.AsTime().UTC()
	terminal.FinishedAt, terminal.Result, terminal.Outcome = &finished, &evidence, workexecution.Succeeded
	if err := terminal.Validate(); err != nil {
		return workexecution.Attempt{}, err
	}
	if previous := state.Backup.Completion; previous != nil {
		if !previous.FinishedAt.Equal(finished) || *previous.Result != evidence {
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
