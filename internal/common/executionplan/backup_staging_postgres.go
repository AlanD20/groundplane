package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateDatabaseStagingGuard(disposition *agentpb.BackupStagingDisposition) error {
	guard := disposition.DatabaseGuard
	if guard == nil {
		return nil
	}
	if guard.TerminalCleanup && disposition.GetDiscardRecovered() == nil ||
		guard.CompletedTask && !guard.TerminalCleanup {
		return invalidBackupStaging()
	}
	if _, err := ValidateBackupStepAuthority(guard.Step); err != nil ||
		!validBackupServiceFact(guard.DatabaseService) ||
		guard.DatabaseArtifact == nil {
		return invalidBackupStaging()
	}
	step := guard.Step
	pointID := step.GetCapture().GetPointId()
	serviceID := step.GetCapture().GetPostgres().GetDatabaseServiceId()
	if mysql := step.GetCapture().GetMysql(); mysql != nil {
		serviceID = mysql.DatabaseServiceId
	}
	if restore := step.GetRestore(); restore != nil {
		pointID, serviceID = restore.PointId, restore.GetPostgres().GetDatabaseServiceId()
		if mysql := restore.GetMysql(); mysql != nil {
			serviceID = mysql.DatabaseServiceId
		}
	}
	key, err := BackupStagingRecoveryKey(guard.TaskId, step.StepId, pointID)
	if err != nil || !bytes.Equal(key, disposition.RecoveryKeySha256) || serviceID == "" ||
		serviceID != guard.DatabaseService.ServiceId {
		return invalidBackupStaging()
	}
	digest := sha256.Sum256(guard.DatabaseArtifact.CanonicalYaml)
	if !bytes.Equal(digest[:], guard.DatabaseArtifact.YamlSha256) {
		return invalidBackupStaging()
	}
	if apply := guard.GetPostgresRecoveryApply(); apply != nil {
		restore := step.GetRestore()
		if disposition.GetRecoveryRequired() == nil || restore.GetPostgres() == nil ||
			!validBackupPostgresApply(apply) || apply.PointId != pointID || restore.ExpectedEvidence == nil ||
			apply.SourceSizeBytes != restore.ExpectedEvidence.SourceSizeBytes ||
			!bytes.Equal(apply.SourceSha256, restore.ExpectedEvidence.SourceSha256) {
			return invalidBackupStaging()
		}
	}
	if apply := guard.GetMysqlRecoveryApply(); apply != nil {
		restore := step.GetRestore()
		if disposition.GetRecoveryRequired() == nil || restore.GetMysql() == nil ||
			!validBackupMySQLApply(apply) || apply.PointId != pointID || restore.ExpectedEvidence == nil ||
			apply.SourceSizeBytes != restore.ExpectedEvidence.SourceSizeBytes ||
			!bytes.Equal(apply.SourceSha256, restore.ExpectedEvidence.SourceSha256) {
			return invalidBackupStaging()
		}
	}
	return nil
}
