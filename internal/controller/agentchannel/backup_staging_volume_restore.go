package agentchannel

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (s *Server) resolveVolumeRestoreStageDisposition(ctx context.Context, store backupStagingStore,
	entry *agentpb.BackupRecoveredStage, source etcd.BackupStagingSource,
) (*agentpb.BackupStagingDisposition, error) {
	if source.VolumeRestore == nil || source.Step.GetRestore().GetVolume() == nil {
		return nil, unresolvedBackupStage()
	}
	native := source.VolumeRestore.Record
	expected := source.Step.GetRestore().ExpectedEvidence
	if expected == nil {
		return nil, unresolvedBackupStage()
	}
	full := []*agentpb.BackupRecoveredFile{{
		Role:      agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT,
		SizeBytes: expected.SourceSizeBytes, Sha256: expected.SourceSha256}}
	if source.Step.GetRestore().Encryption.GetKind() == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
		full = append(full, &agentpb.BackupRecoveredFile{
			Role:      agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT,
			SizeBytes: expected.StoredSizeBytes, Sha256: expected.StoredSha256})
	}
	result := &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...)}
	terminal := taskjournal.IsTerminalTaskStatus(source.Task.Record.Status)
	if terminal {
		if native.State == backupruntime.BackupRestoreRecoveryRequired && native.MutationStarted {
			if len(entry.Files) != len(full) {
				return nil, unresolvedBackupStage()
			}
			for index, file := range entry.Files {
				if !proto.Equal(file, full[index]) {
					return nil, unresolvedBackupStage()
				}
			}
			digestString, err := backupruntime.BackupRestoreTerminalDomainDigest(native)
			if err != nil {
				return nil, err
			}
			digest, err := hex.DecodeString(digestString)
			if err != nil {
				return nil, err
			}
			result.Disposition = &agentpb.BackupStagingDisposition_RecoveryRequired{
				RecoveryRequired: &agentpb.BackupRecoveryRequired{
					NativeRestoreModRevision: source.VolumeRestore.Revision,
					NativeRestoreSha256:      digest, ExpectedFiles: proto.CloneOf(entry).Files}}
			return result, nil
		}
		if native.State != backupruntime.BackupRestoreCompleted &&
			native.State != backupruntime.BackupRestoreFailedSafe {
			return nil, unresolvedBackupStage()
		}
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{}}
		return result, nil
	}
	if source.Task.Record.Status != taskjournal.TaskStatusRunning || source.Assignment == nil {
		return nil, unresolvedBackupStage()
	}
	claim, err := store.GetTaskAssignment(ctx, source.Task.Record.ID)
	if err != nil {
		return nil, err
	}
	if claim.Task.Revision != source.Task.Revision || claim.Assignment.Revision != source.Assignment.Revision {
		return nil, unresolvedBackupStage()
	}
	message, err := s.taskAssignmentMessage(ctx, claim, true)
	if err != nil {
		return nil, err
	}
	var resume *agentpb.BackupRestoreResume
	for _, retained := range message.BackupResume.Steps {
		if retained.StepId == source.Step.StepId && retained.ExecutionId == source.Step.ExecutionId {
			resume = retained.GetRestore()
			break
		}
	}
	if resume == nil {
		return nil, unresolvedBackupStage()
	}
	beforeValidation := resume.CheckpointSequence == 0
	for _, file := range entry.Files {
		matched := false
		for _, representation := range full {
			if beforeValidation {
				matched = matched || file.Role == representation.Role && file.SizeBytes <= representation.SizeBytes
			} else {
				matched = matched || proto.Equal(file, representation)
			}
		}
		if !matched {
			return nil, unresolvedBackupStage()
		}
	}
	if beforeValidation || native.State == backupruntime.BackupRestoreVerified {
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{}}
		return result, nil
	}
	if len(entry.Files) != len(full) {
		return nil, unresolvedBackupStage()
	}
	for index := range full {
		if !bytes.Equal(entry.Files[index].Sha256, full[index].Sha256) {
			return nil, unresolvedBackupStage()
		}
	}
	resumeSHA, err := executionplan.BackupStagingAssignmentResumeSHA256(message.BackupAuthority, message.BackupResume)
	if err != nil {
		return nil, err
	}
	result.Disposition = &agentpb.BackupStagingDisposition_ResumePrepared{
		ResumePrepared: &agentpb.BackupResumePrepared{AssignmentResumeSha256: resumeSHA,
			RemainingGrowth:     agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_BOUNDED,
			RequiredGrowthBytes: 2 * backupvolume.RestoreHeadroom, ExpectedFiles: full}}
	return result, nil
}
