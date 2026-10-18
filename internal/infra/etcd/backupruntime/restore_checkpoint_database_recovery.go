package backupruntime

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareDatabaseRestoreRecoveryCheckpoint retains the failed resource outcome
// while recording proof of the already-spent apply and subsequent consumer
// recovery. The ordinary source rules still validate each individual transition.
func PrepareDatabaseRestoreRecoveryCheckpoint(current BackupRestoreRecord,
	input *agentpb.BackupWorkInput, request *agentpb.BackupWorkCheckpointRequest, at time.Time,
) (BackupRestoreRecord, error) {
	if ValidateBackupRestoreRecord(current) != nil || current.State != BackupRestoreRecoveryRequired ||
		current.Point.SourceKind != BackupRuntimeSourceAttach || current.DatabaseProgress == nil ||
		current.DatabaseProgress.SourceCleanupCompleted || request == nil {
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("database recovery requires its recorded, unfinished apply")
	}
	if err := executionplan.RejectUnknown(request); err != nil {
		return BackupRestoreRecord{}, err
	}
	if !current.DatabaseProgress.ApplyStarted {
		return prepareDatabaseConsumerRecovery(current, input, request, at)
	}
	switch request.Checkpoint.(type) {
	case *agentpb.BackupWorkCheckpointRequest_PostgresRestoreVerified,
		*agentpb.BackupWorkCheckpointRequest_MysqlRestoreVerified,
		*agentpb.BackupWorkCheckpointRequest_PostgresServiceProgress,
		*agentpb.BackupWorkCheckpointRequest_MysqlServiceProgress,
		*agentpb.BackupWorkCheckpointRequest_SourceCleanupCompleted:
	default:
		return BackupRestoreRecord{}, invalidBackupRuntimeRecord("database recovery cannot start another apply")
	}
	// The source fold separately requires byte-identical restart evidence. The
	// resource record retains its cursor while the new attempt obtains its own
	// durable acknowledgement for that already-authorized restart.
	var restart *databaseServiceCheckpoint
	if value := request.GetPostgresServiceProgress(); value != nil && current.CurrentTarget.Postgres != nil {
		restart = &databaseServiceCheckpoint{cursor: value.ServiceCursor, serviceID: value.ServiceId, phase: value.Phase}
	}
	if value := request.GetMysqlServiceProgress(); value != nil && current.CurrentTarget.MySQL != nil {
		restart = &databaseServiceCheckpoint{cursor: value.ServiceCursor, serviceID: value.ServiceId, phase: value.Phase}
	}
	progress := current.DatabaseProgress
	if restart != nil && restart.phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT &&
		progress.RecoveryPhase == restart.phase && progress.RecoveryServiceID == restart.serviceID &&
		progress.RestoreVerificationSHA256 != "" && progress.RecoveryCursor < current.ServiceCount &&
		restart.cursor == current.ServiceCount-progress.RecoveryCursor-1 {
		if request.GetAttempt().GetTaskId() != current.TaskID || ValidateRestoreWorkInput(current, input) != nil ||
			!ValidBackupRuntimeInstant(at) || !at.After(current.UpdatedAt) {
			return BackupRestoreRecord{}, invalidBackupRuntimeRecord("consumer recovery intent identity changed")
		}
		next := CloneBackupRestoreRecord(current)
		next.UpdatedAt = at
		return next, nil
	}
	// Derive the transition position from durable receipts, not diagnostic
	// history or a fabricated attempt. Never persist this intermediate view.
	position := CloneBackupRestoreRecord(current)
	position.State, position.Verification = BackupRestoreRestoring, BackupVerificationPending
	if position.DatabaseProgress.RestoreVerificationSHA256 != "" &&
		position.DatabaseProgress.RecoveryCursor == position.ServiceCount {
		position.State = BackupRestoreConsumersRestored
	}
	next, err := PrepareDatabaseRestoreCheckpoint(position, input, request, at)
	if err != nil {
		return BackupRestoreRecord{}, err
	}
	next.State, next.Verification = BackupRestoreRecoveryRequired, BackupVerificationFailed
	if err := ValidateBackupRestoreRecord(next); err != nil {
		return BackupRestoreRecord{}, err
	}
	return next, nil
}
