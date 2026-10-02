package agentchannel

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (s *Server) resolvePostgresRestoreStageDisposition(ctx context.Context, store backupStagingStore,
	entry *agentpb.BackupRecoveredStage, source etcd.BackupStagingSource,
) (*agentpb.BackupStagingDisposition, error) {
	restore := source.Step.GetRestore()
	if restore.GetPostgres() == nil || restore.ExpectedEvidence == nil {
		return nil, unresolvedBackupStage()
	}
	evidence := restore.ExpectedEvidence
	full := []*agentpb.BackupRecoveredFile{{
		Role:      agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT,
		SizeBytes: evidence.SourceSizeBytes, Sha256: evidence.SourceSha256}}
	if restore.Encryption.GetKind() == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
		full = append(full, &agentpb.BackupRecoveredFile{
			Role:      agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT,
			SizeBytes: evidence.StoredSizeBytes, Sha256: evidence.StoredSha256})
	}
	result := &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...)}
	if taskjournal.IsTerminalTaskStatus(source.Task.Record.Status) {
		return terminalPostgresRestoreDisposition(entry, source, full, result)
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
	for _, retained := range message.BackupResume.GetSteps() {
		if retained.StepId == source.Step.StepId && retained.ExecutionId == source.Step.ExecutionId {
			resume = retained.GetRestore()
			break
		}
	}
	if resume == nil {
		return nil, unresolvedBackupStage()
	}
	beforeValidation := resume.CheckpointSequence == 0
	cleanup := postgresRestoreCleanupAuthorized(resume, len(source.Step.ConsumerServiceIds))
	if !postgresRestoreFilesMatch(entry.Files, full, beforeValidation || cleanup) {
		return nil, unresolvedBackupStage()
	}
	if beforeValidation || cleanup {
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{}}
		return result, nil
	}
	resumeSHA, err := executionplan.BackupStagingAssignmentResumeSHA256(message.BackupAuthority, message.BackupResume)
	if err != nil {
		return nil, err
	}
	result.Disposition = &agentpb.BackupStagingDisposition_ResumePrepared{
		ResumePrepared: &agentpb.BackupResumePrepared{AssignmentResumeSha256: resumeSHA,
			RemainingGrowth: agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_NO_GROWTH, ExpectedFiles: full}}
	return result, nil
}

func postgresRestoreCleanupAuthorized(resume *agentpb.BackupRestoreResume, consumers int) bool {
	if resume.GetSourceCleanupCompleted() != nil {
		return true
	}
	if resume.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY {
		return false
	}
	if consumers == 0 {
		return resume.GetPostgresRestoreVerified() != nil
	}
	progress := resume.GetPostgresServiceProgress()
	return progress != nil && progress.ServiceCursor == 0 &&
		(progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_HEALTHY ||
			progress.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_NOT_RUNNING)
}

func postgresRestoreFilesMatch(files, expected []*agentpb.BackupRecoveredFile, partial bool) bool {
	if !partial && len(files) != len(expected) {
		return false
	}
	for _, file := range files {
		matched := false
		for _, representation := range expected {
			matched = matched || proto.Equal(file, representation) ||
				partial && file.Role == representation.Role && file.SizeBytes <= representation.SizeBytes
		}
		if !matched {
			return false
		}
	}
	return true
}

func terminalPostgresRestoreDisposition(entry *agentpb.BackupRecoveredStage, source etcd.BackupStagingSource,
	full []*agentpb.BackupRecoveredFile, result *agentpb.BackupStagingDisposition,
) (*agentpb.BackupStagingDisposition, error) {
	native, assignment := source.TerminalRestore, source.Task.Record.TerminalAssignment
	if native == nil || native.PostgresProgress == nil || source.Assignment != nil || assignment == nil ||
		assignment.AgentID != source.Index.Record.AgentID || assignment.AgentGeneration != source.Index.Record.AgentGeneration {
		return nil, unresolvedBackupStage()
	}
	safe := native.State == backupruntime.BackupRestoreFailedSafe && !native.MutationStarted
	completed := native.State == backupruntime.BackupRestoreCompleted && native.PostgresProgress.SourceCleanupCompleted
	held := native.State == backupruntime.BackupRestoreRecoveryRequired && native.MutationStarted
	if !safe && !completed && !held ||
		!postgresRestoreFilesMatch(entry.Files, full, !held || native.PostgresProgress.SourceCleanupCompleted) {
		return nil, unresolvedBackupStage()
	}
	if held {
		digest, err := backupruntime.BackupRestoreTerminalDomainDigest(*native)
		if err != nil {
			return nil, err
		}
		raw, err := hex.DecodeString(digest)
		if err != nil {
			return nil, err
		}
		result.Disposition = &agentpb.BackupStagingDisposition_RecoveryRequired{
			RecoveryRequired: &agentpb.BackupRecoveryRequired{NativeRestoreModRevision: source.Task.Revision,
				NativeRestoreSha256: raw, ExpectedFiles: proto.CloneOf(entry).Files}}
	} else {
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{}}
	}
	return result, nil
}
