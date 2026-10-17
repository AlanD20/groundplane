package workqueue

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareSingleTask publishes a one-item action without staging batches. Its
// caller must join request idempotency, captured input and resource admission in
// the same transaction. There is no independently visible partially ready Task.
func PrepareSingleTask(task workexecution.Task, item workexecution.Item, marker idempotency.IdempotencyMarker) (Change, error) {
	if task.ExpectedItems != 1 || task.PublicationComplete || task.Progress.Total() != 0 ||
		task.Outcome != "" || task.FirstFailure != nil || task.FinishedAt != nil || task.Stop != nil || task.Activation != nil ||
		!task.UpdatedAt.Equal(task.CreatedAt) {
		return Change{}, invalid("single work publication requires a fresh one-item Task")
	}
	if err := workexecution.ValidateGraph(task, []workexecution.Item{item}); err != nil {
		return Change{}, err
	}
	next, err := workexecution.CountPublication(task, []workexecution.Item{item}, task.CreatedAt)
	if err != nil {
		return Change{}, err
	}
	next.PublicationComplete = true
	value, err := EncodeTask(next)
	if err != nil {
		return Change{}, err
	}
	change, err := prepareItemCreation(item)
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: TaskKey(task.ID)})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: TaskKey(task.ID), Value: value})
	if err := bindReplay(&change, task, marker); err != nil {
		return Change{}, err
	}
	publishTaskList(&change, task)
	return change, nil
}
