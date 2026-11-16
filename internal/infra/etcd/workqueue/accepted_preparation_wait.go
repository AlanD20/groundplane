package workqueue

import (
	"context"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

type acceptedPreparationWait struct {
	TaskID string             `json:"task_id"`
	Wait   workexecution.Wait `json:"wait"`
	At     time.Time          `json:"at"`
}

func acceptedPreparationWaitKey(taskID string) string { return root + "accepted-waits/" + taskID }

func (wait acceptedPreparationWait) validate() error {
	if ids.Validate(ids.KindTask, wait.TaskID) != nil ||
		wait.Wait.Reason != workexecution.WaitInput && wait.Wait.Reason != workexecution.WaitDependency &&
			wait.Wait.Reason != workexecution.WaitConflict ||
		wait.Wait.Code != "preparation.waiting" || len(wait.Wait.Detail) == 0 || len(wait.Wait.Detail) > 512 || !utf8.ValidString(wait.Wait.Detail) ||
		wait.Wait.TaskID != "" || wait.Wait.WorkItemID != "" || wait.Wait.Until == nil || !wait.Wait.Until.After(wait.At) ||
		!recordcodec.IsCanonicalUTC(wait.At) {
		return invalid("accepted Task pending reason is invalid")
	}
	return nil
}

func (repository *Repository) PrepareAcceptedPreparationWait(ctx context.Context, task TaskSnapshot,
	reason workexecution.WaitReason, detail string, at time.Time,
) (Change, error) {
	until := task.Task.Deadline
	next := acceptedPreparationWait{
		TaskID: task.Task.ID,
		Wait:   workexecution.Wait{Reason: reason, Code: "preparation.waiting", Detail: detail, Until: &until},
		At:     at,
	}
	if task.Revision <= 0 || !task.Task.PendingPreparation() || next.validate() != nil {
		return Change{}, invalid("accepted Task pending reason requires current unprepared intent")
	}
	key := acceptedPreparationWaitKey(task.Task.ID)
	read, err := repository.read(ctx, []string{key}, 0)
	if err != nil {
		return Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	revision := int64(0)
	if previous := read.Values[0]; previous != nil {
		stored, err := recordcodec.Decode[acceptedPreparationWait](previous.Value, "execution_accepted_wait")
		if err != nil || stored.validate() != nil || stored.TaskID != task.Task.ID {
			return Change{}, corrupt()
		}
		if stored.Wait.Reason == reason && stored.Wait.Detail == detail {
			return Change{}, nil
		}
		revision = previous.ModRevision
	}
	value, err := encode("execution_accepted_wait", next)
	if err != nil {
		return Change{}, err
	}
	return Change{
		Conditions: []keyvalue.Condition{
			{Key: TaskKey(task.Task.ID), ModRevision: task.Revision},
			{Key: key, ModRevision: revision},
		},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	}, nil
}

func (repository *Repository) ReadAcceptedPreparationWait(
	ctx context.Context,
	taskID string,
	revision int64,
) (*workexecution.Wait, error) {
	read, err := repository.read(ctx, []string{acceptedPreparationWaitKey(taskID)}, revision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, nil
	}
	wait, err := recordcodec.Decode[acceptedPreparationWait](read.Values[0].Value, "execution_accepted_wait")
	if err != nil || wait.validate() != nil || wait.TaskID != taskID {
		return nil, corrupt()
	}
	return &wait.Wait, nil
}
