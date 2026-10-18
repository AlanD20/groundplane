package backupwork

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareRestorePublication must be joined to the selected Point, destination,
// credential and Environment exclusion transaction before anything is committed.
func PrepareRestorePublication(task workexecution.Task, item workexecution.Item,
	input *agentpb.BackupWorkInput, restore backupruntime.BackupRestoreRecord,
	marker idempotency.IdempotencyMarker,
) (workqueue.Change, error) {
	if err := backupruntime.ValidateRestoreWorkInput(restore, input); err != nil {
		return workqueue.Change{}, err
	}
	if task.ID != restore.TaskID || task.TargetID != restore.EnvironmentID ||
		task.Actor != workexecution.OperatorActor || !task.CreatedAt.Equal(restore.CreatedAt) ||
		!task.Deadline.Equal(restore.CreatedAt.Add(6*time.Hour)) || restore.State != backupruntime.BackupRestoreQueued || restore.MutationStarted ||
		restore.Artifact != nil || !restore.UpdatedAt.Equal(restore.CreatedAt) {
		return workqueue.Change{}, conflict("Restore Task differs from its initial resource admission")
	}
	return PrepareExecutionPublication(task, item, input, marker)
}
