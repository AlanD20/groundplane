package releaseoperation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func groupPublicationProgress(task workqueue.TaskSnapshot, at time.Time) (workqueue.Change, error) {
	next := workexecution.CloneTask(task.Task)
	next.UpdatedAt = at
	value, err := workqueue.EncodeTask(next)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Mutations: []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: workqueue.TaskKey(task.Task.ID), Value: value,
	}}}, nil
}

// Member count is not a byte budget: every member retains a complete immutable
// render. Include the Task progress rewrite and configured physical key prefix
// when deciding whether another independently staged member fits this batch.
func (owner *GraphPublication) groupPreparationFits(ctx context.Context, task workqueue.TaskSnapshot,
	staged, next workqueue.Change, at time.Time,
) (bool, error) {
	progress, err := groupPublicationProgress(task, at)
	if err != nil {
		return false, err
	}
	defer keyvalue.ZeroMutationBytes(progress.Mutations)
	budget, err := owner.queue.Measure(ctx, staged, next, progress)
	return budget.Fits(), err
}
