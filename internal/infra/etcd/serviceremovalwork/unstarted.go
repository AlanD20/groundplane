package serviceremovalwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareUnstartedStop settles only a removal that never had an executor.
// No runtime proof is fabricated, and the Service definition remains unchanged.
func (repository *Repository) PrepareUnstartedStop(ctx context.Context, view workqueue.View, at time.Time) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupNone ||
		item.Input == nil || !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(item.UpdatedAt) {
		return workqueue.Change{}, conflict("Service removal cannot settle as never assigned")
	}
	var outcome workexecution.Outcome
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		outcome = workexecution.Failed
	} else {
		return workqueue.Change{}, conflict("Service removal has no stop or expired deadline")
	}
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := repository.DeletionGuard(ctx, input, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, Evidence: *item.Input, EffectsSettled: true,
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, capture, fence)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: fence.Key})
	return change, nil
}
