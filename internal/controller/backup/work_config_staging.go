package backup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (owner *AgentWork) configCapturePrefix(ctx context.Context, identity workexecution.Identity,
	entry *agentpb.BackupRecoveredStage, source backupwork.StagingSource, run backupruntime.BackupRunRecord,
) (*agentpb.BackupStagingDisposition, []keyvalue.Condition, error) {
	step := source.Input.Steps[source.Ordinal]
	capture, retained := step.GetCapture(), source.Sources[source.Ordinal].GetCapture()
	config := capture.GetConfig()
	row := run.Sources[source.Ordinal]
	if config == nil || config.Content == nil || row.Snapshot.Config == nil || retained == nil ||
		retained.PreparedArtifact != nil ||
		entry.DatabaseExecution ||
		len(entry.Files) < 1 ||
		len(entry.Files) > 2 ||
		entry.Files[0].Role != agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT ||
		entry.Files[0].SizeBytes > config.Content.SourceSizeBytes {
		return nil, nil, invalidCaptureStaging()
	}
	assignment, guards, err := owner.stagingAssignment(ctx, identity, source)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := owner.configSnapshots.OpenCapturedConfigSnapshot(ctx, backupplanning.BackupConfigSnapshotInput{
		TaskID: run.TaskID, SnapshotID: row.Snapshot.Config.ConfigSnapshotID, EnvironmentID: run.EnvironmentID,
		SourceID: row.SourceID, ReadRevision: row.Snapshot.Config.ReadRevision,
	}, config)
	if err != nil {
		return nil, nil, err
	}
	evidence, err := snapshot.PrefixEvidence(ctx, entry.Files[0].SizeBytes)
	if err != nil {
		return nil, nil, err
	}
	expected := &agentpb.BackupRecoveredFile{
		Role:      agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT,
		SizeBytes: evidence.SizeBytes,
		Sha256:    append([]byte(nil), evidence.SHA256[:]...),
	}
	if !proto.Equal(expected, entry.Files[0]) {
		return nil, nil, invalidCaptureStaging()
	}
	digest, err := executionplan.BackupWorkStagingResumeSHA256(assignment, identity)
	if err != nil {
		return nil, nil, err
	}
	resume := &agentpb.BackupResumePrepared{AssignmentResumeSha256: digest,
		RemainingGrowth: agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_NO_GROWTH,
		ExpectedFiles:   []*agentpb.BackupRecoveredFile{expected}}
	journalGrowth, err := backupconfigtransfer.JournalGrowthUpperBound(config.Content)
	if err != nil {
		return nil, nil, err
	}
	remaining := config.Content.SourceSizeBytes - expected.SizeBytes + journalGrowth
	switch capture.Encryption.GetKind() {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
		if len(entry.Files) != 1 {
			return nil, nil, invalidCaptureStaging()
		}
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE:
		storedBound, err := backupconfig.AgeStoredSize(config.Content.SourceSizeBytes)
		if err != nil {
			return nil, nil, err
		}
		remaining += storedBound
		if len(entry.Files) == 2 {
			if expected.SizeBytes != config.Content.SourceSizeBytes ||
				entry.Files[1].Role != agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT ||
				entry.Files[1].SizeBytes > storedBound ||
				retained.Phase != agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING ||
				retained.GetConfigProgress().GetTransfer() == nil {
				return nil, nil, invalidCaptureStaging()
			}
			resume.ExpectedFiles = append(resume.ExpectedFiles, proto.CloneOf(entry.Files[1]))
			resume.RestartConfigEncryption = &agentpb.BackupRestartConfigEncryption{}
		}
	default:
		return nil, nil, invalidCaptureStaging()
	}
	if remaining > 0 {
		resume.RemainingGrowth = agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_BOUNDED
		resume.RequiredGrowthBytes = remaining
	}
	return &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...),
		Disposition: &agentpb.BackupStagingDisposition_ResumePrepared{ResumePrepared: resume}}, guards, nil
}
