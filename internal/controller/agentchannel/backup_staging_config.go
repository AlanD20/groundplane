package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (s *Server) resolveConfigPrefixDisposition(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	entry *agentpb.BackupRecoveredStage,
	source etcd.BackupStagingSource,
	message *agentpb.TaskAssignment,
) (*agentpb.BackupStagingDisposition, error) {
	capture := source.Step.GetCapture()
	config := capture.GetConfig()
	if config == nil || config.Content == nil || len(entry.Files) < 1 || len(entry.Files) > 2 ||
		entry.Files[0].Role != agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT ||
		entry.Files[0].SizeBytes > config.Content.SourceSizeBytes {
		return nil, unresolvedBackupStage()
	}
	expected, err := s.checkpoints.ResolveBackupConfigStagingPrefix(
		ctx, agentID, agentGeneration, message.BackupAuthority, source.Index.Record.StepID, entry.Files[0].SizeBytes,
	)
	if err != nil {
		return nil, err
	}
	if !proto.Equal(expected, entry.Files[0]) {
		return nil, unresolvedBackupStage()
	}
	var restart *agentpb.BackupRestartConfigEncryption
	expectedFiles := []*agentpb.BackupRecoveredFile{expected}
	if len(entry.Files) == 2 {
		// This resolver is reached only when native history has no Prepared
		// checkpoint. Put is impossible before Prepared's durable Ack.
		storedBound, err := backupconfig.AgeStoredSize(config.Content.SourceSizeBytes)
		if err != nil || capture.GetEncryption().GetKind() != agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE ||
			expected.SizeBytes != config.Content.SourceSizeBytes ||
			entry.Files[1].Role != agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT ||
			entry.Files[1].SizeBytes > storedBound {
			return nil, unresolvedBackupStage()
		}
		completed := false
		for _, retained := range message.BackupResume.Steps {
			if retained.StepId == source.Step.StepId && retained.ExecutionId == source.Step.ExecutionId &&
				retained.GetCapture().GetPhase() == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING &&
				retained.GetCapture().GetConfigProgress().GetTransfer() != nil {
				completed = true
				break
			}
		}
		if !completed {
			return nil, unresolvedBackupStage()
		}
		restart = &agentpb.BackupRestartConfigEncryption{}
		expectedFiles = append(expectedFiles, proto.CloneOf(entry.Files[1]))
	}
	remaining := config.Content.SourceSizeBytes - expected.SizeBytes
	journalGrowth, err := backupconfigtransfer.JournalGrowthUpperBound(config.Content)
	if err != nil {
		return nil, err
	}
	remaining += journalGrowth
	switch capture.GetEncryption().GetKind() {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE:
		stored, err := backupconfig.AgeStoredSize(config.Content.SourceSizeBytes)
		if err != nil {
			return nil, err
		}
		remaining += stored
	default:
		return nil, unresolvedBackupStage()
	}
	resumeSHA, err := executionplan.BackupStagingAssignmentResumeSHA256(message.BackupAuthority, message.BackupResume)
	if err != nil {
		return nil, err
	}
	resume := &agentpb.BackupResumePrepared{
		AssignmentResumeSha256:  resumeSHA,
		RemainingGrowth:         agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_NO_GROWTH,
		ExpectedFiles:           expectedFiles,
		RestartConfigEncryption: restart,
	}
	if remaining > 0 {
		resume.RemainingGrowth = agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_BOUNDED
		resume.RequiredGrowthBytes = remaining
	}
	return &agentpb.BackupStagingDisposition{
		RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...),
		Disposition:       &agentpb.BackupStagingDisposition_ResumePrepared{ResumePrepared: resume},
	}, nil
}
