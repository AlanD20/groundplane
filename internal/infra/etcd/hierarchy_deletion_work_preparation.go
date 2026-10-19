package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptmutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareHierarchyControllerProgress removes at most one page of inactive Script
// locators before Environment finalization. It never commits during preparation
// and never reports the removal work complete merely because a page was removed.
func (repository *HierarchyDeletionRepository) PrepareHierarchyControllerProgress(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workqueue.Change, error) {
	fence, err := workqueue.PrepareDecisionFence(request)
	if err != nil {
		return workqueue.Change{}, err
	}
	if request.Task.Task.Action != workexecution.ActionRemove ||
		request.Item.Item.Operation != workexecution.Hierarchy ||
		at.IsZero() ||
		at.Before(request.Item.Item.UpdatedAt) ||
		!at.Before(request.Item.Item.Deadline) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "removal preparation requires hierarchy work")
	}
	captured, err := repository.LoadHierarchyWork(
		ctx,
		workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if captured.Action.ActionKind != hierarchydeletion.HierarchyDeletionEnvironmentFinalize {
		return workqueue.Change{}, nil
	}
	operation := captured.Operation
	conditions, mutations, err := scriptmutations.PrepareEnvironmentDeletionScriptLocatorBatch(
		ctx,
		repository.store,
		captured.Action.TargetID,
		keyvalue.Condition{
			Key: hierarchydeletion.HierarchyDeletionTombstoneKey(
				string(operation.Tombstone.TargetKind),
				operation.Tombstone.TargetID,
			),
			ModRevision: operation.TombstoneRevision,
		},
	)
	if err != nil || len(mutations) == 0 {
		return workqueue.Change{}, err
	}
	fence.Conditions = append(fence.Conditions, captured.Conditions...)
	fence.Conditions = append(fence.Conditions, conditions...)
	fence.Mutations = mutations
	return fence, nil
}
