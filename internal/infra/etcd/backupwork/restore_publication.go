package backupwork

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func PrepareRestoreAuthority(task workexecution.Task, item workexecution.Item, input *agentpb.BackupWorkInput,
	restore backupruntime.BackupRestoreRecord,
) (workqueue.Change, error) {
	if err := validateRestorePublication(task, item, input, restore); err != nil {
		return workqueue.Change{}, err
	}
	return PrepareExecutionAuthority(task, item, input)
}

func validateRestorePublication(task workexecution.Task, item workexecution.Item,
	input *agentpb.BackupWorkInput, restore backupruntime.BackupRestoreRecord,
) error {
	if err := backupruntime.ValidateRestoreWorkInput(restore, input); err != nil {
		return err
	}
	if task.ID != restore.TaskID || task.TargetID != restore.EnvironmentID ||
		task.Actor != workexecution.OperatorActor || !task.UpdatedAt.Equal(restore.CreatedAt) || task.CreatedAt.After(restore.CreatedAt) ||
		restore.State != backupruntime.BackupRestoreQueued || restore.MutationStarted ||
		restore.Artifact != nil || !restore.UpdatedAt.Equal(restore.CreatedAt) {
		return conflict("Restore Task differs from its initial resource admission")
	}
	return nil
}
