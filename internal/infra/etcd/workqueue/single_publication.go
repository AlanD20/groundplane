package workqueue

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareSingleTask publishes a one-item action without staging batches. Its
// caller must join request idempotency, captured input and resource admission in
// the same transaction. There is no independently visible partially ready Task.
func PrepareSingleTask(
	task workexecution.Task,
	item workexecution.Item,
	marker idempotency.IdempotencyMarker,
) (Change, error) {
	if task.ExpectedItems != 1 {
		return Change{}, invalid("single work publication requires one work item")
	}
	return PrepareSmallGraph(task, []workexecution.Item{item}, marker)
}

// PrepareSmallGraph publishes bounded component activations atomically. Large
// Blueprint graphs retain their staged publication and complete-graph barrier.
func PrepareSmallGraph(
	task workexecution.Task,
	items []workexecution.Item,
	marker idempotency.IdempotencyMarker,
) (Change, error) {
	if len(items) < 1 || len(items) > 8 || task.PublicationComplete || task.Progress.Total() != 0 ||
		task.Outcome != "" || task.FirstFailure != nil || task.FinishedAt != nil || task.Stop != nil || task.Activation != nil ||
		!task.UpdatedAt.Equal(task.CreatedAt) {
		return Change{}, invalid("small graph publication requires fresh bounded work")
	}
	if err := workexecution.ValidateGraph(task, items); err != nil {
		return Change{}, err
	}
	next, err := workexecution.CountPublication(task, items, task.CreatedAt)
	if err != nil {
		return Change{}, err
	}
	next.PublicationComplete = true
	value, err := EncodeTask(next)
	if err != nil {
		return Change{}, err
	}
	change := Change{}
	for _, item := range items {
		part, err := prepareItemCreation(item)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return Change{}, err
		}
		change.Conditions = append(change.Conditions, part.Conditions...)
		change.Mutations = append(change.Mutations, part.Mutations...)
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: TaskKey(task.ID)})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: TaskKey(task.ID), Value: value},
	)
	if err := bindReplay(&change, task, marker); err != nil {
		return Change{}, err
	}
	publishTaskList(&change, task)
	return change, nil
}
