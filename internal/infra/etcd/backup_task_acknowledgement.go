package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ValidateBackupRunTaskBinding(task TaskRecord, run backupruntime.BackupRunRecord) error {
	if task.Type != taskjournal.TaskBackup || task.ID != run.TaskID || task.OperationID != run.OperationID ||
		task.Owner.EnvironmentID == "" || task.Owner.EnvironmentID != run.EnvironmentID ||
		task.Target != run.EnvironmentID || !task.CreatedAt.Equal(run.CreatedAt) ||
		task.RetryOf != "" {
		return errs.New(errs.KindInternal, "backup task and run identity differ")
	}
	return nil
}
