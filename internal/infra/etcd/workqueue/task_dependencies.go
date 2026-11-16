package workqueue

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type TaskDependencySnapshot struct {
	Record   workexecution.TaskDependencies
	Revision int64
}

func TaskDependenciesKey(taskID string) string { return root + "task-dependencies/" + taskID }

func taskDependentPrefix(taskID string) string { return root + "task-dependents/" + taskID + "/" }

func taskDependentKey(parentID, childID string) string {
	return taskDependentPrefix(parentID) + childID
}

// PrepareTaskDependencies joins a new accepted-intent Task transaction. The
// caller has not captured execution inputs or claimed runtime resources yet.
// Every predecessor must already exist. Since edges are immutable and point only
// to existing Tasks, no cycle can be introduced, including by concurrent submits.
func (repository *Repository) PrepareTaskDependencies(ctx context.Context, taskID string,
	intent workexecution.Input, after []string, at time.Time,
) (TaskDependencySnapshot, Change, error) {
	dependencies := workexecution.TaskDependencies{TaskID: taskID, Intent: intent,
		After: slices.Clone(after), CreatedAt: at, UpdatedAt: at}
	if err := dependencies.Validate(); err != nil {
		return TaskDependencySnapshot{}, Change{}, err
	}
	keys := make([]string, len(after))
	for index, id := range after {
		keys[index] = TaskKey(id)
	}
	read, err := repository.read(ctx, keys, 0)
	if err != nil {
		return TaskDependencySnapshot{}, Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	change := Change{Conditions: []keyvalue.Condition{{Key: TaskKey(taskID)}, {Key: TaskDependenciesKey(taskID)}}}
	for index, value := range read.Values {
		if value == nil {
			return TaskDependencySnapshot{}, Change{}, errs.Newf(errs.KindTaskNotFound,
				"Run after Task %s was not found", after[index])
		}
		parent, err := DecodeTask(value.Value)
		if err != nil || parent.ID != after[index] {
			return TaskDependencySnapshot{}, Change{}, corrupt()
		}
		if parent.CollectionStartedAt != nil {
			return TaskDependencySnapshot{}, Change{}, errs.Newf(errs.KindStateConflict,
				"Run after Task %s is being collected; select a retained Task", parent.ID)
		}
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision},
		)
		if parent.FinishedAt != nil {
			dependencies.Facts = append(dependencies.Facts, taskCompletion(parent))
			if dependencies.UpdatedAt.Before(*parent.FinishedAt) {
				dependencies.UpdatedAt = *parent.FinishedAt
			}
		} else if failure := taskDependencyFailure(parent); failure != nil {
			if dependencies.Failure == nil {
				dependencies.Failure = failure
			}
			if dependencies.UpdatedAt.Before(failure.ObservedAt) {
				dependencies.UpdatedAt = failure.ObservedAt
			}
		} else {
			key := taskDependentKey(parent.ID, taskID)
			change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
				Key: key, Value: []byte(taskID)})
		}
	}
	decision, err := dependencies.Decision()
	if err != nil {
		return TaskDependencySnapshot{}, Change{}, err
	}
	if decision.Ready || decision.Failed != nil {
		releasedAt := dependencies.UpdatedAt
		dependencies.ReleasedAt = &releasedAt
		// A failed prerequisite releases all pending predecessor references too.
		change.Mutations = nil
	}
	value, err := encode("execution_task_dependencies", dependencies)
	if err != nil {
		return TaskDependencySnapshot{}, Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: TaskDependenciesKey(taskID), Value: value})
	return TaskDependencySnapshot{Record: dependencies}, change, nil
}

func (repository *Repository) ReadTaskDependencies(ctx context.Context, taskID string,
	revision int64,
) (TaskDependencySnapshot, int64, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || revision < 0 {
		return TaskDependencySnapshot{}, 0, invalid("Task dependency read identity or revision is invalid")
	}
	read, err := repository.read(ctx, []string{TaskDependenciesKey(taskID)}, revision)
	if err != nil {
		return TaskDependencySnapshot{}, 0, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return TaskDependencySnapshot{}, read.ReadRevision, nil
	}
	dependencies, err := recordcodec.Decode[workexecution.TaskDependencies](read.Values[0].Value,
		"execution_task_dependencies")
	if err != nil || dependencies.Validate() != nil || dependencies.TaskID != taskID {
		return TaskDependencySnapshot{}, 0, corrupt()
	}
	return TaskDependencySnapshot{Record: dependencies, Revision: read.Values[0].ModRevision}, read.ReadRevision, nil
}

func taskCompletion(task workexecution.Task) workexecution.TaskCompletion {
	return workexecution.TaskCompletion{TaskID: task.ID, Outcome: task.Outcome, FinishedAt: *task.FinishedAt}
}

func taskDependencyFailure(task workexecution.Task) *workexecution.TaskDependencyFailure {
	outcome := task.Outcome
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	}
	if outcome != workexecution.Failed && outcome != workexecution.Cancelled && outcome != workexecution.Superseded {
		return nil
	}
	return &workexecution.TaskDependencyFailure{TaskID: task.ID, Outcome: outcome, ObservedAt: task.UpdatedAt}
}

func taskDependencyChange(snapshot TaskDependencySnapshot, next workexecution.TaskDependencies) (Change, error) {
	if snapshot.Revision <= 0 || snapshot.Record.TaskID != next.TaskID || snapshot.Record.Intent != next.Intent ||
		!slices.Equal(snapshot.Record.After, next.After) || snapshot.Record.ReleasedAt != nil ||
		next.UpdatedAt.Before(snapshot.Record.UpdatedAt) || next.Validate() != nil {
		return Change{}, invalid("Task dependency transition requires its unchanged accepted intent")
	}
	for _, previous := range snapshot.Record.Facts {
		if !slices.Contains(next.Facts, previous) {
			return Change{}, invalid("settled Task dependency facts are immutable")
		}
	}
	if snapshot.Record.Failure != nil && (next.Failure == nil || *snapshot.Record.Failure != *next.Failure) {
		return Change{}, invalid("a captured predecessor failure is immutable")
	}
	value, err := encode("execution_task_dependencies", next)
	if err != nil {
		return Change{}, err
	}
	change := Change{
		Conditions: []keyvalue.Condition{{Key: TaskDependenciesKey(next.TaskID), ModRevision: snapshot.Revision}},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: TaskDependenciesKey(next.TaskID), Value: value},
		},
	}
	for _, id := range snapshot.Record.After {
		wasPending, pending := true, next.ReleasedAt == nil
		for _, fact := range snapshot.Record.Facts {
			wasPending = wasPending && fact.TaskID != id
		}
		for _, fact := range next.Facts {
			pending = pending && fact.TaskID != id
		}
		if wasPending && !pending {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
				Key: taskDependentKey(id, next.TaskID)})
		}
	}
	return change, nil
}
