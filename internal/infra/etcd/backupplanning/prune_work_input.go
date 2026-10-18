package backupplanning

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ValidateBackupPruneWorkInput(dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	evidence []PruneExecutionEvidence, input *agentpb.BackupWorkInput,
) error {
	if err := backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch); err != nil {
		return err
	}
	if err := executionplan.ValidateBackupWorkInput(input); err != nil {
		return err
	}
	if input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE ||
		input.EnvironmentId != dispatch.EnvironmentID || len(input.Steps) != len(evidence) ||
		len(evidence) != len(dispatch.RecoveryPointIDs) {
		return errs.New(errs.KindValidationFailed, "Backup prune input differs from selected objects")
	}
	scope := &agentpb.BackupPlanScope{ProjectId: input.ProjectId, Project: input.Project,
		EnvironmentId: input.EnvironmentId, Environment: input.Environment, Services: input.Services}
	for index, item := range evidence {
		if err := ValidateBackupPruneSourceAuthority(dispatch, item, scope, input.Steps[index], index); err != nil {
			return err
		}
	}
	return nil
}
