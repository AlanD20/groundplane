package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (ledger *ReleaseLedger) PrepareDirectReleasePreparationRetirement(
	ctx context.Context,
	view workqueue.View,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if ctx == nil || task.CollectionStartedAt == nil || task.FinishedAt == nil || task.Progress.Unsettled() != 0 ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" ||
		task.Action != workexecution.ActionDeploy && task.Action != workexecution.ActionRollback {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Direct Release preparation retirement requires settled collecting work",
		)
	}
	_, guard, err := ledger.ReadDirectReleasePreparation(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{guard}, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: guard.Key},
	}}, nil
}
