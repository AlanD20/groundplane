package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

type TaskSnapshot struct {
	Task     workexecution.Task
	Revision int64
}

func EncodeTask(task workexecution.Task) ([]byte, error) {
	if err := task.Validate(); err != nil {
		return nil, err
	}
	return encode("execution_task", task)
}

func DecodeTask(value []byte) (workexecution.Task, error) {
	if len(value) > maximumRecordBytes {
		return workexecution.Task{}, corrupt()
	}
	task, err := recordcodec.Decode[workexecution.Task](value, "execution_task")
	if err != nil {
		return workexecution.Task{}, err
	}
	if err := task.Validate(); err != nil {
		return workexecution.Task{}, corrupt()
	}
	return task, nil
}

// PrepareTask accepts only the small public action. Its intent, owner and request
// replay are published in the same transaction by the initiating operation.
func PrepareTask(task workexecution.Task, marker idempotency.IdempotencyMarker) (Change, error) {
	if task.PublicationComplete || task.Progress.Total() != 0 || task.Outcome != "" ||
		task.FirstFailure != nil || task.FinishedAt != nil || task.Stop != nil || task.Activation != nil || !task.UpdatedAt.Equal(task.CreatedAt) {
		return Change{}, invalid("new task must contain an unpublished captured intent")
	}
	value, err := EncodeTask(task)
	if err != nil {
		return Change{}, err
	}
	if marker.Kind != idempotency.IdempotencyMarkerTask || marker.State != idempotency.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || !marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(task.CreatedAt) {
		return Change{}, invalid("task submission requires its matching request replay record")
	}
	change := Change{
		Conditions: []keyvalue.Condition{{Key: TaskKey(task.ID)}},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: TaskKey(task.ID), Value: value},
			{Type: keyvalue.MutationPut, Key: taskPublishingKey(task.ID), Value: []byte(task.ID)},
		},
	}
	if err := bindReplay(&change, task, marker); err != nil {
		return Change{}, err
	}
	publishTaskList(&change, task)
	return change, nil
}

// PrepareItems supports bounded publication batches while the Task's barrier
// stays closed. The transaction compares the Task revision and absent item keys,
// so replay cannot double-count a batch.
func PrepareItems(snapshot TaskSnapshot, items []workexecution.Item, at time.Time) (Change, error) {
	if snapshot.Revision <= 0 || len(items) == 0 {
		return Change{}, invalid("work publication requires its accepted task")
	}
	next, err := workexecution.CountPublication(snapshot.Task, items, at)
	if err != nil {
		return Change{}, err
	}
	change, err := taskChange(snapshot, next)
	if err != nil {
		return Change{}, err
	}
	for _, item := range items {
		part, err := prepareItemCreation(item)
		if err != nil {
			return Change{}, err
		}
		change.Conditions = append(change.Conditions, part.Conditions...)
		change.Mutations = append(change.Mutations, part.Mutations...)
	}
	return change, nil
}

// PreparePublicationComplete opens dispatch only after the full persisted graph
// has been read at one revision. All writes to unpublished items must compare and
// advance the Task revision; this single barrier compare fences the entire read.
func PreparePublicationComplete(snapshot TaskSnapshot, items []workexecution.Item, at time.Time) (Change, error) {
	if snapshot.Revision <= 0 || snapshot.Task.PublicationComplete || at.Before(snapshot.Task.UpdatedAt) {
		return Change{}, invalid("task publication barrier cannot be opened")
	}
	if err := workexecution.ValidateGraph(snapshot.Task, items); err != nil {
		return Change{}, err
	}
	for _, item := range items {
		if item.AttemptNumber != 0 || item.Outcome != "" ||
			item.Phase != workexecution.Ready && item.Phase != workexecution.Waiting {
			return Change{}, invalid("unpublished task contains already executed work")
		}
	}
	next := workexecution.CloneTask(snapshot.Task)
	next.PublicationComplete, next.UpdatedAt = true, at
	return taskChange(snapshot, next)
}

func taskChange(snapshot TaskSnapshot, next workexecution.Task) (Change, error) {
	if snapshot.Revision <= 0 || snapshot.Task.ID != next.ID {
		return Change{}, invalid("task update requires matching revisioned identity")
	}
	value, err := EncodeTask(next)
	if err != nil {
		return Change{}, err
	}
	change := Change{
		Conditions: []keyvalue.Condition{{Key: TaskKey(next.ID), ModRevision: snapshot.Revision}},
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: TaskKey(next.ID), Value: value}},
	}
	if snapshot.Task.PublicationComplete && !next.PublicationComplete {
		return Change{}, invalid("published task cannot reopen publication")
	}
	if !snapshot.Task.PublicationComplete && next.PublicationComplete {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{
			Type: keyvalue.MutationDelete, Key: taskPublishingKey(next.ID),
		})
	}
	wasReady, ready := taskReadyToFinish(snapshot.Task), taskReadyToFinish(next)
	if wasReady != ready {
		mutation := keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: taskFinishKey(next.ID)}
		if ready {
			mutation.Type, mutation.Value = keyvalue.MutationPut, []byte(next.ID)
		}
		change.Mutations = append(change.Mutations, mutation)
	}
	updateRetentionIndex(&change, snapshot.Task, next)
	return change, nil
}

func taskReadyToFinish(task workexecution.Task) bool {
	return task.PublicationComplete && task.Progress.Unsettled() == 0 && task.FinishedAt == nil
}

func taskFinishKey(taskID string) string { return root + "finishing/" + taskID }

func taskPublishingKey(taskID string) string { return root + "publishing/" + taskID }

func countTransition(change *Change, task TaskSnapshot, before, after workexecution.Item, at time.Time) error {
	next, err := workexecution.CountTransition(task.Task, before, after, at)
	if err != nil {
		return err
	}
	part, err := taskChange(task, next)
	if err != nil {
		return err
	}
	change.Conditions = append(change.Conditions, part.Conditions...)
	change.Mutations = append(change.Mutations, part.Mutations...)
	return updateInputQueue(change, before, after)
}
