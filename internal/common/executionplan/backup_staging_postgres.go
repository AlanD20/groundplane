package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validatePostgresStagingGuard(disposition *agentpb.BackupStagingDisposition) error {
	guard := disposition.PostgresGuard
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
	pointID, serviceID := step.GetCapture().GetPointId(), step.GetCapture().GetPostgres().GetDatabaseServiceId()
	if restore := step.GetRestore().GetPostgres(); restore != nil {
		pointID, serviceID = step.GetRestore().PointId, restore.DatabaseServiceId
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
	if apply := guard.RecoveryApply; apply != nil {
		restore := step.GetRestore()
		if disposition.GetRecoveryRequired() == nil || restore.GetPostgres() == nil ||
			!validBackupPostgresApply(apply) || apply.PointId != pointID || restore.ExpectedEvidence == nil ||
			apply.SourceSizeBytes != restore.ExpectedEvidence.SourceSizeBytes ||
			!bytes.Equal(apply.SourceSha256, restore.ExpectedEvidence.SourceSha256) {
			return invalidBackupStaging()
		}
	}
	return nil
}
