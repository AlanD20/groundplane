package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func OpenBackupRecoveryAssignment(assignment *agentpb.BackupRecoveryAssignment, identity workexecution.Identity) (
	*agentpb.BackupRecoveryAssignment, workexecution.Attempt, BackupWorkRecovery, error,
) {
	if assignment == nil || proto.Size(assignment) > MaximumPlanBytes || RejectUnknown(assignment) != nil {
		return nil, workexecution.Attempt{}, BackupWorkRecovery{}, errs.New(
			errs.KindValidationFailed,
			"Backup recovery assignment is invalid",
		)
	}
	attempt, err := workexecution.DecodeAgentAttempt(assignment.Attempt, identity)
	if err != nil {
		return nil, workexecution.Attempt{}, BackupWorkRecovery{}, err
	}
	source, err := DecodeBackupWorkRecovery(assignment.RecoverySource)
	if err != nil {
		return nil, workexecution.Attempt{}, BackupWorkRecovery{}, err
	}
	if err := source.ValidateFor(attempt); err != nil {
		return nil, workexecution.Attempt{}, BackupWorkRecovery{}, err
	}
	input, raw, reference, err := CaptureBackupWorkInput(attempt.Input.ID, assignment.Input)
	clear(raw)
	if err != nil {
		return nil, workexecution.Attempt{}, BackupWorkRecovery{}, err
	}
	if reference != attempt.Input {
		return nil, workexecution.Attempt{}, BackupWorkRecovery{}, errs.New(
			errs.KindStateConflict,
			"Backup recovery input differs from its failed operation",
		)
	}
	if input.Operation == agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE {
		if assignment.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_NO_LIVE_MUTATION &&
			assignment.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION {
			return nil, workexecution.Attempt{}, BackupWorkRecovery{}, errs.New(
				errs.KindValidationFailed,
				"Restore recovery requires the Controller's mutation disposition",
			)
		}
	} else if assignment.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_UNSPECIFIED {
		return nil, workexecution.Attempt{}, BackupWorkRecovery{}, errs.New(errs.KindValidationFailed, "Non-Restore recovery carries a Restore disposition")
	}
	if err := ValidateBackupSourceResumes(assignment.Sources, input.Steps); err != nil {
		return nil, workexecution.Attempt{}, BackupWorkRecovery{}, err
	}
	for ordinal, progress := range assignment.Sources {
		if err := validateDatabaseWorkProgress(progress.GetRestore(), input.Steps[ordinal], input.Services); err != nil {
			return nil, workexecution.Attempt{}, BackupWorkRecovery{}, err
		}
	}
	owned := proto.CloneOf(assignment)
	owned.Input = input
	return owned, attempt, source, nil
}
