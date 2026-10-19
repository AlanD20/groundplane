package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackupRecoveryRecord struct {
	Source           executionplan.BackupWorkRecovery `json:"source"`
	Report           []byte                           `json:"report,omitempty"`
	Checkpoint       BackupSourceReport               `json:"checkpoint"`
	CheckpointWrites uint64                           `json:"checkpoint_writes"`
}

func (record attemptRecord) validateBackupRecovery() error {
	backup, attempt := record.Backup, record.Attempt
	recovery := backup.Recovery
	if len(backup.Sources) != 0 ||
		attempt.Validate() != nil || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		recovery.Source.ValidateFor(attempt) != nil || record.UpdatedAt.Before(*attempt.StartedAt) {
		return invalid()
	}
	if recovery.CheckpointWrites > ^uint64(0)-2 || record.validateBackupRecoveryCheckpoint() != nil {
		return invalid()
	}
	baseSequence := uint64(1) + recovery.CheckpointWrites
	if backup.FailureReason != "" {
		if !backup.FailureReason.Valid() || len(recovery.Report) != 0 || record.Sequence != baseSequence+1 {
			return invalid()
		}
		if backup.Completion == nil {
			if len(backup.FailureAck) != 0 {
				return invalid()
			}
			return nil
		}
		ack := &agentpb.BackupWorkFailureAck{}
		if proto.Unmarshal(backup.FailureAck, ack) != nil {
			return invalid()
		}
		saved, err := executionplan.DecodeBackupWorkFailureAck(attempt, backup.FailureReason, ack)
		terminal := backup.Completion
		if err != nil || terminal.Validate() != nil || !workexecution.SameAssignment(saved, *terminal) ||
			terminal.StartedAt == nil || !terminal.StartedAt.Equal(*saved.StartedAt) || terminal.FinishedAt == nil ||
			!terminal.FinishedAt.Equal(*saved.FinishedAt) || terminal.FinishedAt.Before(record.UpdatedAt) ||
			terminal.Result == nil || *terminal.Result != *saved.Result || terminal.Outcome != saved.Outcome {
			return invalid()
		}
		return nil
	}
	if len(backup.FailureAck) != 0 {
		return invalid()
	}
	if len(recovery.Report) == 0 {
		if record.Sequence != baseSequence || backup.Completion != nil {
			return invalid()
		}
		return nil
	}
	report, evidence, err := executionplan.DecodeBackupRecoveryReport(attempt, recovery.Source, recovery.Report)
	if err != nil || record.Sequence != baseSequence+1 || !record.UpdatedAt.Equal(report.ObservedAt) ||
		(len(recovery.Checkpoint.Request) != 0 && len(recovery.Checkpoint.Acknowledgement) == 0) {
		return invalid()
	}
	if terminal := backup.Completion; terminal != nil {
		if terminal.Validate() != nil || !workexecution.SameAssignment(*terminal, attempt) ||
			terminal.StartedAt == nil ||
			!terminal.StartedAt.Equal(*attempt.StartedAt) ||
			terminal.FinishedAt == nil ||
			terminal.FinishedAt.Before(record.UpdatedAt) ||
			terminal.Result == nil ||
			*terminal.Result != evidence ||
			terminal.Outcome != workexecution.Succeeded {
			return invalid()
		}
	}
	return nil
}
