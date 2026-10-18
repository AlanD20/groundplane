// Package operationretention coordinates collection of completed single-item operations.
// Operation repositories retire their captured input before execution records.
package operationretention

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Collector struct {
	queue       *workqueue.Repository
	connectors  *connectorchecks.Repository
	backings    *backingchecks.Repository
	dns         *dnschecks.Repository
	edge        *edgechecks.Repository
	runners     *runnerchecks.Repository
	images      *imageoperations.Repository
	hierarchy   *etcd.HierarchyDeletionRepository
	networks    *networkwork.InputRepository
	workOwners  map[workexecution.Operation]WorkOwner
	taskOwners  map[workexecution.Action]TaskOwner
	draftOwners []DraftOwner
}

func New(queue *workqueue.Repository, connectors *connectorchecks.Repository,
	backings *backingchecks.Repository, dns *dnschecks.Repository, edge *edgechecks.Repository,
	runners *runnerchecks.Repository,
	images *imageoperations.Repository,
	hierarchy *etcd.HierarchyDeletionRepository,
	networks *networkwork.InputRepository,
	tasks []TaskRegistration,
	registrations ...WorkRegistration,
) (*Collector, error) {
	if queue == nil || connectors == nil || backings == nil || dns == nil || edge == nil || runners == nil || images == nil || hierarchy == nil || networks == nil {
		return nil, errs.New(errs.KindInternal, "operation retention requires all operation repositories")
	}
	owners := make(map[workexecution.Operation]WorkOwner, len(registrations))
	for _, registration := range registrations {
		if !workexecution.ValidOperation(registration.Operation) || registration.Owner == nil || owners[registration.Operation] != nil {
			return nil, errs.New(errs.KindInternal, "work retention owner is invalid or duplicated")
		}
		owners[registration.Operation] = registration.Owner
	}
	taskOwners := make(map[workexecution.Action]TaskOwner, len(tasks))
	var drafts []DraftOwner
	for _, registration := range tasks {
		if registration.Owner == nil || taskOwners[registration.Action] != nil ||
			registration.Action != workexecution.ActionDeploy && registration.Action != workexecution.ActionRollback {
			return nil, errs.New(errs.KindInternal, "Task retention owner is invalid or duplicated")
		}
		taskOwners[registration.Action] = registration.Owner
		if registration.Drafts != nil {
			drafts = append(drafts, registration.Drafts)
		}
	}
	return &Collector{queue: queue, connectors: connectors, backings: backings, dns: dns, edge: edge, runners: runners, images: images, hierarchy: hierarchy, networks: networks, workOwners: owners, taskOwners: taskOwners, draftOwners: drafts}, nil
}

// Collect performs at most one eight-attempt batch. False means another
// bounded pass is needed or a concurrent write won; it never restarts the Task.
func (collector *Collector) Collect(ctx context.Context, taskID string, at time.Time) (bool, error) {
	task, revision, err := collector.queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		return false, err
	}
	if task.Task.Action == workexecution.ActionApply && task.Task.Scope.EnvironmentID != "" &&
		task.Task.TargetID == task.Task.Scope.EnvironmentID {
		return collector.collectGraph(ctx, task, revision, at)
	}
	if task.Task.Action == workexecution.ActionActivate || task.Task.Action == workexecution.ActionDeploy || task.Task.Action == workexecution.ActionRollback {
		return collector.collectGraph(ctx, task, revision, at)
	}
	if task.Task.Action == workexecution.ActionRemove && ids.Validate(ids.KindNetwork, task.Task.TargetID) == nil {
		return collector.collectZone(ctx, task, revision, at)
	}
	guard, err := collector.hierarchy.HierarchyCollectionGuard(ctx, task, revision, at)
	if err != nil {
		return false, err
	}
	if guard.Present {
		return collector.collectHierarchy(ctx, task, revision, guard, at)
	}
	if task.Task.ExpectedItems != 1 {
		return false, errs.New(errs.KindStateConflict, "Task requires its own operation retention coordinator")
	}
	page, err := collector.queue.ListWork(ctx, taskID, "", 2, revision)
	if err != nil {
		return false, err
	}
	if len(page.Items) == 0 && !page.More && task.Task.CollectionStartedAt != nil {
		change, err := workqueue.PrepareTaskCollection(task)
		if err != nil {
			return false, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: executiondiagnostics.TaskEventsKey(taskID)})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: scriptexecutions.SourceRetirementPrefix(taskID), Prefix: true})
		result, err := collector.queue.Commit(ctx, change)
		return result.Succeeded, err
	}
	if len(page.Items) != 1 || page.More {
		return false, errs.New(errs.KindInternal, "operation retention work membership is inconsistent")
	}
	item := page.Items[0]
	if collector.workOwners[item.Item.Operation] != nil {
		if task.Task.CollectionStartedAt == nil {
			change, err := workqueue.PrepareCollection(task, at)
			if err != nil {
				return false, err
			}
			_, err = collector.queue.Commit(ctx, change)
			return false, err
		}
		return collector.collectOwnedWork(ctx, taskID, item.Item.ID, at)
	}
	inputKey, err := operationInputKey(item.Item.Operation, taskID)
	if err != nil {
		return false, err
	}
	if task.Task.CollectionStartedAt == nil {
		change, err := workqueue.PrepareCollection(task, at)
		if err != nil {
			return false, err
		}
		_, err = collector.queue.Commit(ctx, change)
		return false, err
	}
	retired, err := collector.retireInput(ctx, item.Item.Operation, taskID, at)
	if err != nil || !retired {
		return false, err
	}
	view, attempts, err := collector.queue.CollectionAttempts(ctx, taskID, item.Item.ID)
	if err != nil {
		return false, err
	}
	owner := workqueue.Change{Conditions: []keyvalue.Condition{{Key: inputKey}}}
	if len(attempts.Attempts) == 0 {
		change, err := workqueue.PrepareWorkCollection(view.Task, view.Item)
		if err != nil {
			return false, err
		}
		_, err = collector.queue.Commit(ctx, change, owner)
		return false, err
	}
	changes := []workqueue.Change{owner}
	for _, attempt := range attempts.Attempts {
		change, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, attempt)
		if err != nil {
			return false, err
		}
		switch item.Item.Operation {
		case workexecution.ConnectorCheck:
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: connectorchecks.ResultKey(attempt.Attempt.ID)})
		case workexecution.BackingCheck:
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: backingchecks.ResultKey(attempt.Attempt.ID)})
		case workexecution.EdgeCheck:
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: edgechecks.ResultKey(attempt.Attempt.ID)})
		case workexecution.Image:
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: imageoperations.ProgressKey(attempt.Attempt.ID)})
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: imageoperations.ResultKey(attempt.Attempt.ID)})
			// DNS evidence is the finished operation input, already retired above.
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: executiondiagnostics.Key(attempt.Attempt.ID)})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: executiondiagnostics.LossKey(attempt.Attempt.ID)})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: executiondiagnostics.EventsKey(attempt.Attempt.ID)})
		changes = append(changes, change)
	}
	_, err = collector.queue.Commit(ctx, changes...)
	return false, err
}

func operationInputKey(operation workexecution.Operation, taskID string) (string, error) {
	switch operation {
	case workexecution.ConnectorCheck:
		return connectorchecks.Key(taskID), nil
	case workexecution.BackingCheck:
		return backingchecks.Key(taskID), nil
	case workexecution.DNSCheck:
		return dnschecks.RecordKey(taskID), nil
	case workexecution.EdgeCheck:
		return edgechecks.Key(taskID), nil
	case workexecution.RunnerCheck:
		return runnerchecks.RecordKey(taskID), nil
	case workexecution.Image:
		return imageoperations.Key(taskID), nil
	default:
		return "", errs.New(errs.KindStateConflict, "operation has no retention owner")
	}
}

func (collector *Collector) retireInput(ctx context.Context, operation workexecution.Operation, taskID string, at time.Time) (bool, error) {
	switch operation {
	case workexecution.ConnectorCheck:
		return collector.connectors.PruneInput(ctx, taskID, at)
	case workexecution.BackingCheck:
		return collector.backings.PruneInput(ctx, taskID, at)
	case workexecution.DNSCheck:
		return collector.dns.PruneInput(ctx, taskID, at)
	case workexecution.EdgeCheck:
		return collector.edge.PruneInput(ctx, taskID, at)
	case workexecution.RunnerCheck:
		return collector.runners.PruneInput(ctx, taskID, at)
	case workexecution.Image:
		return collector.images.PruneInput(ctx, taskID, at)
	default:
		return false, errs.New(errs.KindStateConflict, "operation has no retention owner")
	}
}
