package backup

import (
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Before Prepared no upload can have started. DumpStart still spends the dump
// attempt: resume only its retained bytes, then let the original worker prove
// them against the original Exec and helper evidence before publishing them.
func databaseCapturePrefix(entry *agentpb.BackupRecoveredStage, step *agentpb.BackupStepAuthority,
	retained *agentpb.BackupCaptureResume, resumeSHA []byte,
) (*agentpb.BackupStagingDisposition, error) {
	capture := step.GetCapture()
	if retained == nil || retained.Phase != agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING ||
		retained.PreparedArtifact != nil {
		return nil, invalidCaptureStaging()
	}
	result := &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...)}
	dumpStarted := retained.DumpStart != nil || retained.MysqlDumpStart != nil
	if !dumpStarted {
		if retained.CheckpointSequence > 1 {
			return nil, invalidCaptureStaging()
		}
		result.Disposition = &agentpb.BackupStagingDisposition_DiscardRecovered{
			DiscardRecovered: &agentpb.BackupDiscardRecovered{}}
		return result, nil
	}
	maximum := capture.GetPostgres().GetMaxPlaintextBytes()
	if mysql := capture.GetMysql(); mysql != nil {
		maximum = mysql.MaxPlaintextBytes
	}
	if len(entry.Files) < 1 || len(entry.Files) > 2 ||
		entry.Files[0].Role != agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT ||
		entry.Files[0].SizeBytes > maximum {
		return nil, invalidCaptureStaging()
	}
	resume := &agentpb.BackupResumePrepared{
		AssignmentResumeSha256: resumeSHA,
		RemainingGrowth:        agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_NO_GROWTH,
		ExpectedFiles:          []*agentpb.BackupRecoveredFile{proto.CloneOf(entry.Files[0])},
	}
	switch capture.GetEncryption().GetKind() {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
		if len(entry.Files) != 1 {
			return nil, invalidCaptureStaging()
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
				return nil, invalidCaptureStaging()
			}
			resume.ExpectedFiles = append(resume.ExpectedFiles, proto.CloneOf(entry.Files[1]))
			resume.RestartDatabaseEncryption = &agentpb.BackupRestartDatabaseEncryption{}
		}
	default:
		return nil, invalidCaptureStaging()
	}
	if capture.GetMysql() != nil && len(entry.Files) == 1 {
		// The daemon-owned Dump may have completed after Agent loss. Its
		// original bytes must finish the retained prefix before encryption;
		// their size is not yet known. Serialize and reserve each added write.
		resume.RemainingGrowth = agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_EXCLUSIVE_UNKNOWN
		resume.RequiredGrowthBytes = 0
	}
	result.Disposition = &agentpb.BackupStagingDisposition_ResumePrepared{ResumePrepared: resume}
	return result, nil
}
