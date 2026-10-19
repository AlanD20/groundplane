package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// AdvanceBackupRestoreRecovery advances the original source cursor under the
// new recovery attempt. Transfer evidence stays owned by the original forward
// attempt; recovery does not create a second archive or Restore generation.
func AdvanceBackupRestoreRecovery(assignment *agentpb.BackupRecoveryAssignment,
	request *agentpb.BackupWorkCheckpointRequest, authenticated workexecution.Identity,
	evidence *BackupConfigWorkEvidence,
) (*agentpb.BackupStepResume, error) {
	owned, _, source, err := OpenBackupRecoveryAssignment(assignment, authenticated)
	if err != nil {
		return nil, err
	}
	report, err := ValidateBackupRestoreRecoveryCheckpoint(request, owned, authenticated)
	if err != nil {
		return nil, err
	}
	restore := owned.Input.Steps[report.SourceOrdinal].GetRestore()
	switch {
	case restore.GetPostgres() != nil, restore.GetMysql() != nil:
		current := owned.Sources[report.SourceOrdinal].GetRestore()
		if repeatsDatabaseRestartIntent(current, report) {
			if current.CheckpointSequence == ^uint64(0) || report.CheckpointSequence != current.CheckpointSequence+1 ||
				!proto.Equal(report.PrecedingCheckpoint, current.PrecedingCheckpoint) {
				return nil, invalidBackupCheckpoint("consumer recovery intent does not follow saved progress")
			}
			next := proto.CloneOf(owned.Sources[report.SourceOrdinal])
			next.GetRestore().CheckpointSequence = report.CheckpointSequence
			next.GetRestore().PrecedingCheckpoint = nil
			return next, nil
		}
		return advanceDatabaseRestoreSource(owned.Input, owned.Sources, report)
	case restore.GetVolume() != nil:
		current := owned.Sources[report.SourceOrdinal].GetRestore()
		progress, value := current.GetVolumeProgress(), report.GetVolume().GetServiceProgress()
		if progress != nil && value != nil &&
			((current.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY &&
				progress.ServicePhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT) ||
				(current.Phase == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TARGET_PREPARATION &&
					progress.ServicePhase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT)) &&
			value.Phase == progress.ServicePhase && value.ServiceCursor == progress.ServiceCursor &&
			int(value.ServiceCursor) < len(owned.Input.Steps[report.SourceOrdinal].ConsumerServiceIds) &&
			owned.Input.Steps[report.SourceOrdinal].ConsumerServiceIds[value.ServiceCursor] == value.ServiceId {
			if current.CheckpointSequence == ^uint64(0) || report.CheckpointSequence != current.CheckpointSequence+1 ||
				!proto.Equal(report.PrecedingCheckpoint, current.PrecedingCheckpoint) {
				return nil, invalidBackupCheckpoint("Volume recovery intent does not follow saved progress")
			}
			next := proto.CloneOf(owned.Sources[report.SourceOrdinal])
			next.GetRestore().CheckpointSequence, next.GetRestore().PrecedingCheckpoint = report.CheckpointSequence, nil
			return next, nil
		}
		return advanceVolumeRestoreSource(owned.Input, owned.Sources, report)
	case restore.GetConfig() != nil:
		return advanceConfigRestoreSource(owned.Input, owned.Sources, report, evidence, source.Forward)
	default:
		return nil, invalidBackupCheckpoint("Restore recovery has no captured source")
	}
}

// Reaffirm only the exact interrupted restart. This creates an acknowledged
// receipt for the recovery attempt without rewinding the consumer state machine.
func repeatsDatabaseRestartIntent(
	current *agentpb.BackupRestoreResume,
	report *agentpb.BackupWorkCheckpointRequest,
) bool {
	progress := current.GetDatabaseProgress()
	if progress == nil ||
		(!progress.ConsumersOnly && progress.PostgresVerified == nil && progress.MysqlVerified == nil) ||
		current.Phase != agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_SERVICE_RECOVERY ||
		progress.ConsumerPhase != agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT {
		return false
	}
	if value := report.GetPostgresServiceProgress(); value != nil {
		return value.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT &&
			proto.Equal(value, current.GetPostgresServiceProgress())
	}
	if value := report.GetMysqlServiceProgress(); value != nil {
		return value.Phase == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_RESTART_INTENT &&
			proto.Equal(value, current.GetMysqlServiceProgress())
	}
	return false
}
