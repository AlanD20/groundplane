package backupruntime

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func NewBackupRunRetryRecord(source BackupRunRecord, taskID string, createdAt time.Time) (BackupRunRecord, error) {
	retry := CloneBackupRunPublicationRecord(source)
	retry.TaskID = taskID
	retry.RetryOfTaskID = source.TaskID
	retry.State = BackupRunQueued
	retry.CreatedAt = createdAt.UTC()
	retry.UpdatedAt = retry.CreatedAt
	retry.Sources = retry.Sources[:0]
	for _, sourceAttempt := range source.Sources {
		if sourceAttempt.State == BackupSourceAttemptSucceeded {
			continue
		}
		pointID := ids.New(ids.KindRecoveryPoint)
		pointCreatedAt, err := ids.Timestamp(ids.KindRecoveryPoint, pointID)
		if err != nil {
			return BackupRunRecord{}, errs.Wrap(errs.KindInternal, err)
		}
		attempt := sourceAttempt
		attempt.Ordinal = uint32(len(retry.Sources))
		attempt.Snapshot = CloneBackupRunSourceSnapshot(sourceAttempt.Snapshot)
		attempt.RecoveryPointID = pointID
		attempt.RecoveryPointCreatedAt = pointCreatedAt
		attempt.ObjectKey = retry.ConnectorPrefix + retry.EnvironmentID + "/" +
			attempt.SourceID + "/" + pointID + "/artifact.bin"
		attempt.State = BackupSourceAttemptPending
		attempt.Phase = BackupSourcePhaseCapture
		attempt.Evidence = BackupArtifactEvidence{}
		attempt.ConfigArchive = BackupConfigArchiveEvidence{}
		attempt.VolumeArchive = BackupVolumeArchiveEvidence{}
		attempt.Upload = BackupUploadOutcome{}
		attempt.Object = BackupObjectIdentity{}
		attempt.FailureCode = ""
		retry.Sources = append(retry.Sources, attempt)
	}
	if len(retry.Sources) == 0 {
		return BackupRunRecord{}, errs.New(errs.KindTaskNotRetryable, "backup run has no incomplete source attempts")
	}
	if err := ValidateBackupRunRecord(retry); err != nil {
		return BackupRunRecord{}, err
	}
	return retry, nil
}
