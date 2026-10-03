package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionretention"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) prepareHierarchyDeletionTaskPrune(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	retention keyvalue.KeyValue,
	revision int64,
	now time.Time,
) (bool, bool, error) {
	return hierarchydeletionretention.Prune(ctx, repository.store, hierarchydeletionretention.ParentTask{
		ID:          task.ID,
		OperationID: task.Params[taskjournal.TaskHierarchyDeletionOperationParam],
		Marker:      *task.idempotencyMarker,
	}, taskRevision, retention, revision, now)
}
