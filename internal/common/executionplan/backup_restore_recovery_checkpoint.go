package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateBackupRestoreRecoveryCheckpoint accepts continuation evidence, never
// a new capture or database apply grant. The persisted source transition and
// resource guards must still commit atomically before acknowledging the report.
func ValidateBackupRestoreRecoveryCheckpoint(request *agentpb.BackupWorkCheckpointRequest,
	assignment *agentpb.BackupRecoveryAssignment, authenticated workexecution.Identity,
) (*agentpb.BackupWorkCheckpointRequest, error) {
	owned, attempt, _, err := OpenBackupRecoveryAssignment(assignment, authenticated)
	if err != nil {
		return nil, err
	}
	if attempt.FinishedAt != nil ||
		owned.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE ||
		owned.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION ||
		request == nil ||
		!proto.Equal(request.Attempt, owned.Attempt) {
		return nil, invalidBackupCheckpoint("Restore continuation requires its active recovery assignment")
	}
	validated, err := validateBackupSourceCheckpoint(request, owned.Input)
	if err != nil {
		return nil, err
	}
	// Irreversible database apply permission belongs to the failed forward
	// attempt. Recovery may prove that apply completed, but cannot spend it again.
	switch validated.Checkpoint.(type) {
	case *agentpb.BackupWorkCheckpointRequest_DatabaseConsumerRecovery:
		return validated, nil
	case *agentpb.BackupWorkCheckpointRequest_Config,
		*agentpb.BackupWorkCheckpointRequest_Volume,
		*agentpb.BackupWorkCheckpointRequest_PostgresRestoreVerified,
		*agentpb.BackupWorkCheckpointRequest_PostgresServiceProgress,
		*agentpb.BackupWorkCheckpointRequest_MysqlRestoreVerified,
		*agentpb.BackupWorkCheckpointRequest_MysqlServiceProgress,
		*agentpb.BackupWorkCheckpointRequest_SourceCleanupCompleted:
		return validated, nil
	default:
		return nil, invalidBackupCheckpoint("Restore recovery cannot grant a new database apply or capture")
	}
}

func ValidateBackupRestoreRecoveryCheckpointAck(ack *agentpb.BackupWorkCheckpointAck,
	request *agentpb.BackupWorkCheckpointRequest, assignment *agentpb.BackupRecoveryAssignment,
	authenticated workexecution.Identity,
) (*agentpb.BackupWorkCheckpointAck, error) {
	validated, err := ValidateBackupRestoreRecoveryCheckpoint(request, assignment, authenticated)
	if err != nil {
		return nil, err
	}
	return validateBackupSourceCheckpointAck(ack, validated, assignment.Input)
}
