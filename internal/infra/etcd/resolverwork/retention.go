package resolverwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Collection removes only execution captures. Intent, applied configuration,
// live DNS answers and host baseline remain independently resource-owned.
func (repository *Repository) PrepareRetention(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil || at.IsZero() ||
		at.Location() != time.UTC ||
		at.Before(*task.CollectionStartedAt) ||
		at.Before(*task.RetainUntil) ||
		item.Operation != workexecution.Resolver ||
		item.Phase != workexecution.Settled ||
		item.ActiveAttemptID != "" ||
		item.Result == nil ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, invalid("CoreDNS history requires expired, settled execution")
	}
	stored, err := repository.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: append(append([]keyvalue.Condition(nil), stored.Guards...),
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision})}
	if final {
		collected, err := workqueue.PrepareWorkCollection(view.Task, view.Item)
		if err != nil {
			return false, workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, collected.Conditions...)
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: InputKey(item.ID)},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: CaptureKey(item.ID)},
		)
	}
	return true, change, nil
}

func PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	item, attempt := view.Item.Item, snapshot.Attempt
	if item.Operation != workexecution.Resolver || item.Placement.Executor != workexecution.Agent ||
		item.Input == nil ||
		attempt.Input != *item.Input ||
		attempt.Executor.HostID != item.Placement.HostID ||
		attempt.Executor.Executor != workexecution.Agent {
		return workqueue.Change{}, invalid("CoreDNS attempt collection differs from its captured work and host")
	}
	collected, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	failureID, err := resolverexecution.FailureID(attempt.ID)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: collected.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: ReportPrefix(attempt.ID), Prefix: true},
		{Type: keyvalue.MutationDelete, Key: FailureKey(failureID)},
		{Type: keyvalue.MutationDelete, Key: ResultKey(attempt.ID)},
		{Type: keyvalue.MutationDelete, Key: RecoveryResultKey(attempt.ID)},
	}}, nil
}
