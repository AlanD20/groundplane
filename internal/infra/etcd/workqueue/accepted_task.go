package workqueue

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareAcceptedTask publishes pure operator intent and replay identity before
// image/runtime capture. The caller joins encrypted typed request storage and
// stable input references. It must not join runtime claims or execution grants.
func (repository *Repository) PrepareAcceptedTask(ctx context.Context, task workexecution.Task,
	marker idempotency.IdempotencyMarker,
) (Change, error) {
	if !task.PendingPreparation() || task.ExpectedItems != 0 {
		return Change{}, invalid("accepted action requires pure intent without an execution capture")
	}
	change, err := PrepareTask(task, marker)
	if err != nil || len(task.After) == 0 {
		return change, err
	}
	_, dependencies, err := repository.PrepareTaskDependencies(
		ctx,
		task.ID,
		*task.AcceptedIntent,
		task.After,
		task.CreatedAt,
	)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return Change{}, err
	}
	joined, err := JoinChanges(change, dependencies)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		keyvalue.ClearMutationValues(dependencies.Mutations)
	}
	return joined, err
}

// PrepareAcceptedTaskPromotion publishes a typed operation owner's freshly
// captured execution graph under the ORIGINAL accepted Task identity. It never
// resubmits an API request or creates a hidden child Task. The owner joins all
// exact source comparisons, captured inputs and runtime reservations here.
func (repository *Repository) PrepareAcceptedTaskPromotion(
	ctx context.Context,
	snapshot TaskSnapshot,
	prepared workexecution.Task,
	graph, initial []workexecution.Item,
	staged bool,
	at time.Time,
) (Change, error) {
	var dependencyGuard Change
	if len(snapshot.Task.After) != 0 {
		dependencies, _, err := repository.ReadTaskDependencies(ctx, snapshot.Task.ID, 0)
		if err != nil {
			return Change{}, err
		}
		if dependencies.Revision <= 0 || dependencies.Record.ReleasedAt == nil || snapshot.Task.AcceptedIntent == nil ||
			dependencies.Record.Intent != *snapshot.Task.AcceptedIntent || !slices.Equal(dependencies.Record.After, snapshot.Task.After) {
			return Change{}, invalid("execution preparation requires its resolved Task dependencies")
		}
		decision, err := dependencies.Record.Decision()
		if err != nil || !decision.Ready {
			return Change{}, invalid("execution preparation is blocked by its predecessor Tasks")
		}
		dependencyGuard.Conditions = []keyvalue.Condition{
			{Key: TaskDependenciesKey(snapshot.Task.ID), ModRevision: dependencies.Revision},
		}
	}
	accepted := snapshot.Task
	if snapshot.Revision <= 0 || !accepted.PendingPreparation() || accepted.Stop != nil ||
		accepted.PublicationComplete || accepted.Progress.Total() != 0 || !at.Before(accepted.Deadline) ||
		at.Before(accepted.UpdatedAt) || prepared.ID != accepted.ID || prepared.Actor != accepted.Actor ||
		prepared.Scope != accepted.Scope || prepared.Action != accepted.Action || prepared.TargetID != accepted.TargetID ||
		prepared.TargetName != accepted.TargetName || !prepared.CreatedAt.Equal(accepted.CreatedAt) ||
		!prepared.Deadline.Equal(accepted.Deadline) || prepared.AcceptedIntent == nil ||
		*prepared.AcceptedIntent != *accepted.AcceptedIntent || !slices.Equal(prepared.After, accepted.After) ||
		prepared.Intent.Validate() != nil || prepared.PublicationComplete || prepared.Progress.Total() != 0 ||
		prepared.FinishedAt != nil || prepared.Stop != nil || prepared.Outcome != "" || prepared.Activation != nil {
		return Change{}, invalid("execution preparation must preserve the unstarted accepted Task and intent")
	}
	if err := workexecution.ValidateGraph(prepared, graph); err != nil {
		return Change{}, err
	}
	for _, item := range graph {
		if item.CreatedAt.After(prepared.UpdatedAt) {
			return Change{}, invalid("prepared work cannot follow its capture timestamp")
		}
	}
	if !staged {
		if len(graph) > 8 {
			return Change{}, invalid("unstaged preparation requires at most eight work items")
		}
		initial = graph
	} else {
		if len(initial) > 8 {
			return Change{}, invalid("staged preparation requires a bounded initial subset")
		}
		seen := make(map[string]bool, len(initial))
		for _, item := range initial {
			found := false
			for _, member := range graph {
				if member.ID != item.ID {
					continue
				}
				matches, err := samePublishedItem(member, item)
				if err != nil {
					return Change{}, err
				}
				found = matches
				break
			}
			if !found || seen[item.ID] {
				return Change{}, invalid("staged preparation repeats or changes a captured initial work item")
			}
			seen[item.ID] = true
		}
	}
	next, err := workexecution.CountPublication(prepared, initial, at)
	if err != nil {
		return Change{}, err
	}
	next.PublicationComplete = !staged && len(initial) == len(graph)
	change, err := taskChange(snapshot, next)
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, dependencyGuard.Conditions...)
	for _, item := range initial {
		part, err := prepareItemCreation(item)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return Change{}, err
		}
		change.Conditions = append(change.Conditions, part.Conditions...)
		change.Mutations = append(change.Mutations, part.Mutations...)
	}
	return change, nil
}
