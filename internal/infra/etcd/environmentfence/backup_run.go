package environmentfence

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
)

func LoadBackupRunOwned(
	ctx context.Context,
	store environmentMutationFenceStore,
	run backupruntime.BackupRunRecord,
	revision int64,
) (Evidence, error) {
	return LoadOwned(
		ctx,
		store,
		run.EnvironmentID,
		revision,
		Owner{
			Kind: backupruntime.BackupOperationBackup, OperationID: run.OperationID, TaskID: run.TaskID,
		},
	)
}
