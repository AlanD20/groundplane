package workqueue

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// PrepareTaskDependencyProgress copies only settled predecessor facts. The
// returned change must join the dependent's accepted-intent transition. A ready
// decision grants no execution permission and captures no runtime/image inputs;
// those remain the operation owner's next, separately guarded preparation.
func (repository *Repository) PrepareTaskDependencyProgress(ctx context.Context,
	task TaskSnapshot, at time.Time,
) (workexecution.TaskDependencyDecision, Change, error) {
	if task.Revision <= 0 || task.Task.Validate() != nil || at.Before(task.Task.UpdatedAt) {
		return workexecution.TaskDependencyDecision{}, Change{}, invalid(
			"Task dependency progress requires its current Task",
		)
	}
	snapshot, revision, err := repository.ReadTaskDependencies(ctx, task.Task.ID, 0)
	if err != nil {
		return workexecution.TaskDependencyDecision{}, Change{}, err
	}
	if snapshot.Revision == 0 {
		if len(task.Task.After) != 0 {
			return workexecution.TaskDependencyDecision{}, Change{}, corrupt()
		}
		return workexecution.TaskDependencyDecision{Ready: true}, Change{}, nil
	}
	if task.Task.AcceptedIntent == nil || snapshot.Record.Intent != *task.Task.AcceptedIntent ||
		!slices.Equal(snapshot.Record.After, task.Task.After) || at.Before(snapshot.Record.UpdatedAt) {
		return workexecution.TaskDependencyDecision{}, Change{}, corrupt()
	}
	decision, err := snapshot.Record.Decision()
	if err != nil || snapshot.Record.ReleasedAt != nil {
		return decision, Change{Conditions: []keyvalue.Condition{{Key: TaskDependenciesKey(task.Task.ID),
			ModRevision: snapshot.Revision}}}, err
	}
	if task.Task.Stop != nil || task.Task.FinishedAt != nil || !at.Before(task.Task.Deadline) {
		next := workexecution.CloneTaskDependencies(snapshot.Record)
		next.UpdatedAt, next.ReleasedAt = at, &at
		change, err := taskDependencyChange(snapshot, next)
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: TaskKey(task.Task.ID), ModRevision: task.Revision},
		)
		return decision, change, err
	}
	keys := make([]string, 0, len(decision.Waiting)*2)
	for _, id := range decision.Waiting {
		keys = append(keys, TaskKey(id), taskCompletionKey(id))
	}
	read, err := repository.read(ctx, keys, revision)
	if err != nil {
		return workexecution.TaskDependencyDecision{}, Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	next := workexecution.CloneTaskDependencies(snapshot.Record)
	next.UpdatedAt = at
	guards := []keyvalue.Condition{{Key: TaskKey(task.Task.ID), ModRevision: task.Revision}}
	for index, id := range decision.Waiting {
		parent, receipt := read.Values[index*2], read.Values[index*2+1]
		if parent != nil {
			captured, err := DecodeTask(parent.Value)
			if err != nil || captured.ID != id {
				return workexecution.TaskDependencyDecision{}, Change{}, corrupt()
			}
			if captured.FinishedAt == nil {
				if failure := taskDependencyFailure(captured); failure != nil && next.Failure == nil {
					next.Failure = failure
					guards = append(guards, keyvalue.Condition{Key: parent.Key, ModRevision: parent.ModRevision})
				}
				continue
			}
			guards = append(guards, keyvalue.Condition{Key: parent.Key, ModRevision: parent.ModRevision})
			next.Facts = append(next.Facts, taskCompletion(captured))
		} else {
			// History may expire independently. An outstanding reverse edge keeps
			// this tiny result alive, without keeping predecessor logs or inputs.
			if receipt == nil {
				return workexecution.TaskDependencyDecision{}, Change{}, corrupt()
			}
			fact, err := recordcodec.Decode[workexecution.TaskCompletion](receipt.Value, "execution_task_completion")
			if err != nil || fact.Validate() != nil || fact.TaskID != id {
				return workexecution.TaskDependencyDecision{}, Change{}, corrupt()
			}
			guards = append(guards, keyvalue.Condition{Key: TaskKey(id)},
				keyvalue.Condition{Key: receipt.Key, ModRevision: receipt.ModRevision})
			next.Facts = append(next.Facts, fact)
		}
	}
	for _, fact := range next.Facts {
		if next.UpdatedAt.Before(fact.FinishedAt) {
			next.UpdatedAt = fact.FinishedAt
		}
	}
	if next.Failure != nil && next.UpdatedAt.Before(next.Failure.ObservedAt) {
		next.UpdatedAt = next.Failure.ObservedAt
	}
	decision, err = next.Decision()
	if err != nil {
		return workexecution.TaskDependencyDecision{}, Change{}, err
	}
	if decision.Ready || decision.Failed != nil || task.Task.Stop != nil || task.Task.FinishedAt != nil {
		releasedAt := next.UpdatedAt
		next.ReleasedAt = &releasedAt
	}
	if len(next.Facts) == len(snapshot.Record.Facts) && next.ReleasedAt == nil {
		return decision, Change{}, nil
	}
	change, err := taskDependencyChange(snapshot, next)
	change.Conditions = append(change.Conditions, guards...)
	return decision, change, err
}
