package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func postgresCaptureCleanupAuthorized(step *agentpb.BackupStepAuthority, resume *agentpb.BackupTaskResume) bool {
	for _, retained := range resume.GetSteps() {
		if retained.StepId != step.StepId || retained.ExecutionId != step.ExecutionId {
			continue
		}
		capture := retained.GetCapture()
		return capture.GetSourceCleanupCompleted() != nil ||
			capture.GetPhase() == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_POINT_COMMIT ||
			capture.GetPhase() == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_SOURCE_CLEANUP
	}
	return false
}

// Before Prepared no upload can have started. DumpStart still spends the dump
// attempt: resume only its retained bytes, then let the original worker prove
// them against the original Exec and helper evidence before publishing them.
func resolvePostgresCaptureDisposition(entry *agentpb.BackupRecoveredStage,
	source etcd.BackupStagingSource, message *agentpb.TaskAssignment,
) (*agentpb.BackupStagingDisposition, error) {
	capture := source.Step.GetCapture()
	var retained *agentpb.BackupCaptureResume
	for _, candidate := range message.BackupResume.GetSteps() {
		if candidate.StepId == source.Step.StepId && candidate.ExecutionId == source.Step.ExecutionId {
			retained = candidate.GetCapture()
			break
		}
	}
	if retained == nil || retained.Phase != agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING ||
		retained.PreparedArtifact != nil {
		return nil, unresolvedBackupStage()
	}
	result := &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...)}
	if retained.DumpStart == nil {
		if retained.CheckpointSequence > 1 {
			return nil, unresolvedBackupStage()
		}
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{}}
		return result, nil
	}
	if len(entry.Files) < 1 || len(entry.Files) > 2 ||
		entry.Files[0].Role != agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT ||
		entry.Files[0].SizeBytes > capture.GetPostgres().MaxPlaintextBytes {
		return nil, unresolvedBackupStage()
	}
	resumeSHA, err := executionplan.BackupStagingAssignmentResumeSHA256(message.BackupAuthority, message.BackupResume)
	if err != nil {
		return nil, err
	}
	resume := &agentpb.BackupResumePrepared{
		AssignmentResumeSha256: resumeSHA,
		RemainingGrowth:        agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_NO_GROWTH,
		ExpectedFiles:          []*agentpb.BackupRecoveredFile{proto.CloneOf(entry.Files[0])},
	}
	switch capture.GetEncryption().GetKind() {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
		if len(entry.Files) != 1 {
			return nil, unresolvedBackupStage()
		}
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE:
		storedBound, err := backupformat.AgeStoredSize(entry.Files[0].SizeBytes)
		if err != nil {
			return nil, err
		}
		resume.RemainingGrowth = agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_BOUNDED
		resume.RequiredGrowthBytes = storedBound
		if len(entry.Files) == 2 {
			if entry.Files[1].Role != agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT ||
				entry.Files[1].SizeBytes > storedBound {
				return nil, unresolvedBackupStage()
			}
			resume.ExpectedFiles = append(resume.ExpectedFiles, proto.CloneOf(entry.Files[1]))
			resume.RestartPostgresEncryption = &agentpb.BackupRestartPostgresEncryption{}
		}
	default:
		return nil, unresolvedBackupStage()
	}
	result.Disposition = &agentpb.BackupStagingDisposition_ResumePrepared{ResumePrepared: resume}
	return result, nil
}
