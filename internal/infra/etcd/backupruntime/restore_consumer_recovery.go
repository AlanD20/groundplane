package backupruntime

import (
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Consumer-only recovery never enters a database apply or verified state. The
// failed Restore remains failed while its stopped workloads are brought back.
func prepareDatabaseConsumerRecovery(current BackupRestoreRecord, input *agentpb.BackupWorkInput,
	request *agentpb.BackupWorkCheckpointRequest, at time.Time,
) (BackupRestoreRecord, error) {
	if current.DatabaseProgress.ApplyStarted || request.GetAttempt().GetTaskId() != current.TaskID ||
		ValidateRestoreWorkInput(current, input) != nil || !ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("consumer recovery authority differs from the interrupted Restore")
	}
	next := CloneBackupRestoreRecord(current)
	progress := next.DatabaseProgress
	switch {
	case request.GetDatabaseConsumerRecovery() != nil:
		if progress.ConsumersOnly || progress.ContainerID == "" || request.GetDatabaseConsumerRecovery().PointId != current.Point.ID {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("consumer recovery lacks its original database observation")
		}
		progress.ConsumersOnly = true
	case !progress.ConsumersOnly:
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("consumer recovery has not been acknowledged")
	case request.GetSourceCleanupCompleted() != nil:
		cleanup := request.GetSourceCleanupCompleted()
		if progress.RecoveryCursor != current.ServiceCount || cleanup.PointId != current.Point.ID ||
			!BackupArtifactEvidenceMatchesWire(current.Point.Evidence, cleanup.Evidence) {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("consumer recovery cleanup precedes restored workloads")
		}
		progress.SourceCleanupCompleted = true
	default:
		var value databaseServiceCheckpoint
		if report := request.GetPostgresServiceProgress(); report != nil && current.CurrentTarget.Postgres != nil {
			value = databaseServiceCheckpoint{cursor: report.ServiceCursor, serviceID: report.ServiceId, phase: report.Phase}
		} else if report := request.GetMysqlServiceProgress(); report != nil && current.CurrentTarget.MySQL != nil {
			value = databaseServiceCheckpoint{cursor: report.ServiceCursor, serviceID: report.ServiceId, phase: report.Phase}
		} else {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("consumer-only recovery cannot apply or verify a database")
		}
		reaffirm := value.phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT &&
			progress.RecoveryPhase == value.phase && progress.RecoveryServiceID == value.serviceID &&
			progress.RecoveryCursor < current.ServiceCount && value.cursor == current.ServiceCount-progress.RecoveryCursor-1
		if !reaffirm {
			if err := advanceDatabaseRestoreRecovery(&next, value); err != nil {
				return BackupRestoreRecord{}, err
			}
		}
	}
	next.UpdatedAt = at
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}
