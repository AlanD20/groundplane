package serviceremovalwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareRetention removes only expired execution input. Desired revisions,
// applied receipts, Service state and Volume data have separate owners and are
// never removed as a side effect of collecting a Task's history.
func (repository *Repository) PrepareRetention(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.IsZero() || at.Location() != time.UTC || at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		item.Operation != workexecution.ServiceRemoval || item.TaskID != task.ID ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Result == nil ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, conflict("Service removal history requires expired, settled execution")
	}
	if item.Input == nil {
		return repository.retireUnprepared(ctx, view, final)
	}
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{capture,
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
	}}
	if !final {
		return true, change, nil
	}
	collection, err := workqueue.PrepareWorkCollection(view.Task, view.Item)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, collection.Conditions...)
	retired, guards, err := repository.retireConfigurationPins(ctx, input, view.ReadRevision, change.Conditions)
	if err != nil || !retired {
		return false, workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: InputKey(item.ID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: RecoveryKey(item.ID)},
		keyvalue.Mutation{
			Type:   keyvalue.MutationDelete,
			Key:    servicelifecyclework.ContinuationPrefix(item.ID),
			Prefix: true,
		},
	)
	return true, change, nil
}

func PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	if view.Item.Item.Operation != workexecution.ServiceRemoval {
		return workqueue.Change{}, conflict("Service removal retention received another operation")
	}
	change, err := servicelifecyclework.PrepareAttemptRetirement(view, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{
		Type: keyvalue.MutationDelete, Key: RecoveryReportPrefix(snapshot.Attempt.ID), Prefix: true,
	}, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: RecoveryFailureKey(snapshot.Attempt.ID)})
	return change, nil
}
