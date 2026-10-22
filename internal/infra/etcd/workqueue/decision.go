package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareExpiryFence protects a queued timeout decision without requiring its
// failed prerequisites or an available executor. It grants no execution effects
// and never releases a resource claim held by another work item.
func PrepareExpiryFence(request Assignment, at time.Time) (Change, error) {
	item, task := request.Item.Item, request.Task.Task
	if item.Validate() != nil || task.Validate() != nil || request.Task.Revision <= 0 || request.Item.Revision <= 0 ||
		request.QueueRevision <= 0 || request.ReadRevision <= 0 || item.TaskID != task.ID || item.ActiveAttemptID != "" ||
		item.Phase != workexecution.Ready || at.Before(item.Deadline) || task.Stop != nil ||
		!task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return Change{}, invalid("queued expiry requires captured work beyond its deadline")
	}
	return Change{Conditions: []keyvalue.Condition{
		{Key: TaskKey(task.ID), ModRevision: request.Task.Revision},
		{Key: ItemKey(item.ID), ModRevision: request.Item.Revision},
		{Key: QueueKey(item), ModRevision: request.QueueRevision},
	}}, nil
}

// PrepareDecisionFence protects a Controller-only atomic effect without taking
// a worker slot. Every dependency and conflict observed by admission is compared
// again in the effect transaction, so concurrent assignment cannot slip between
// the read and the resource write.
func PrepareDecisionFence(request Assignment) (Change, error) {
	item, task := request.Item.Item, request.Task.Task
	if !request.DecisionOnly || request.ReadRevision <= 0 || request.Task.Revision <= 0 || request.Item.Revision <= 0 ||
		item.Validate() != nil || task.Validate() != nil || item.TaskID != task.ID ||
		item.Placement.Executor != workexecution.Controller || item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" || item.Phase != workexecution.Ready || task.Stop != nil ||
		!task.PublicationComplete || task.FinishedAt != nil ||
		len(request.Dependencies) != len(item.Dependencies) || len(request.Claims) != len(item.Claims) ||
		request.Executor.Revision <= 0 || request.QueueRevision <= 0 {
		return Change{}, invalid("Controller decision requires current dependency and conflict evidence")
	}
	change := Change{Conditions: []keyvalue.Condition{
		{Key: TaskKey(task.ID), ModRevision: request.Task.Revision},
		{Key: ItemKey(item.ID), ModRevision: request.Item.Revision},
		{Key: QueueKey(item), ModRevision: request.QueueRevision},
		{Key: ExecutorKey(request.Executor.Record.Identity), ModRevision: request.Executor.Revision},
	}}
	if err := assignmentDependencies(request); err != nil {
		return Change{}, err
	}
	for index, claim := range request.Claims {
		if claim.Claim != item.Claims[index] || claim.Owner != nil || claim.Revision != 0 {
			return Change{}, invalid("Controller decision resource is already claimed")
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: ClaimKey(claim.Claim)})
	}
	reservations, err := taskReservationConditions(task, item, request.Reservations)
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, reservations...)
	return change, nil
}
