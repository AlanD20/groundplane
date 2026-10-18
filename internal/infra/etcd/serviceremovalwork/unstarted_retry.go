package serviceremovalwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) PrepareUnstartedRetry(ctx context.Context, view workqueue.View, at, deadline time.Time) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Stop != nil || task.CollectionStartedAt != nil || item.ForwardRevoked || item.AttemptNumber != 0 ||
		item.Phase != workexecution.Settled || item.Outcome != workexecution.Failed || item.Cleanup != workexecution.CleanupNone ||
		item.Input == nil || item.Result == nil || *item.Result != *item.Input || item.Operation != workexecution.ServiceRemoval {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "service removal Retry requires a captured action that failed before assignment")
	}
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := repository.prepareRetryFence(ctx, view, input, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	retry, err := workqueue.PrepareUnstartedRetry(view.Task, view.Item, deadline, at)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	retry.Conditions = append(retry.Conditions, capture)
	retry.Conditions = append(retry.Conditions, change.Conditions...)
	retry.Mutations = append(retry.Mutations, change.Mutations...)
	return retry, nil
}

// Reacquiring admission cannot replace a later runtime, configuration or desired
// revision. The fence and queue Retry are published in one transaction.
func (repository *Repository) prepareRetryFence(ctx context.Context, view workqueue.View, input Input, at time.Time) (workqueue.Change, error) {
	desired, err := repository.DesiredGuard(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	pins, err := repository.configurationPinGuard(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	fenceKey := deletions.TombstoneKey(string(deletions.DeletionTargetService), input.ServiceID)
	conditions := append(pins, desired,
		keyvalue.Condition{Key: fenceKey},
		keyvalue.Condition{Key: services.ServiceRuntimeKey(input.ServiceID), ModRevision: input.RuntimeRevision},
		keyvalue.Condition{Key: serviceruntimerecord.Key(input.ServiceID), ModRevision: input.RuntimeSourceRevision})
	if input.Runtime != nil {
		conditions = append(conditions, keyvalue.Condition{Key: runtimeconfiguration.HeadKey(input.Scope.EnvironmentID), ModRevision: input.ConfigurationRevision})
	}
	if view.Task.Task.Action == workexecution.ActionRemove {
		conditions = append(conditions, keyvalue.Condition{Key: blueprints.EnvironmentBlueprintHeadKey(input.Scope.EnvironmentID), ModRevision: input.HeadRevision})
	}
	tombstone, err := deletions.EncodeDeletionTombstone(deletions.DeletionTombstoneRecord{
		TargetKind: deletions.DeletionTargetService, TargetID: input.ServiceID, TargetRevision: input.ServiceRevision,
		TaskID: input.TaskID, Phase: deletions.DeletionPhaseHostEffects, CreatedAt: input.CreatedAt, UpdatedAt: at})
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: conditions, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: fenceKey, Value: tombstone}}}, nil
}
