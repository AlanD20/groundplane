package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func runnerCheckTask(task TaskRecord) bool {
	return task.Type == taskjournal.TaskCheck &&
		task.Params[taskjournal.TaskResourceKindParam] == runners.TaskResourceRunner
}
func validateRunnerCheckTask(task TaskRecord) error {
	if !runnerCheckTask(task) {
		return nil
	}
	if task.Executor != taskjournal.TaskExecutorController || task.Actor != taskjournal.TaskActorOperator ||
		task.Owner.WorkspaceType != taskjournal.TaskWorkspaceTenant ||
		ids.Validate(ids.KindRunner, task.Target) != nil ||
		len(task.Params) != 2 ||
		len(task.Steps) != 0 ||
		len(task.Materializations) != 0 ||
		task.Configuration != nil ||
		task.EntryRuntime != nil ||
		task.RenderGeneration != 1 ||
		task.TimeoutSeconds != 60 ||
		task.RetryOf != "" ||
		!recordcodec.ValidSHA256(task.Params[taskjournal.TaskRunnerCheckInputParam]) {
		return errs.New(errs.KindValidationFailed, "Runner diagnostic Task input is invalid")
	}
	return nil
}
func runnerCheckMatchesTask(record runnerchecks.Record, task TaskRecord) bool {
	hash, err := runnerchecks.PlanHash(record)
	owner, ownerErr := runners.TaskOwner(record.Source.Desired)
	return err == nil && ownerErr == nil && runnerCheckTask(task) && validateRunnerCheckTask(task) == nil &&
		task.Owner == owner &&
		record.Owner.TaskID == task.ID &&
		record.Owner.PlanID == task.PlanID &&
		record.Owner.OperationID == task.OperationID &&
		record.Source.RunnerID == task.Target &&
		hash == task.PlanHash &&
		record.InputSHA256 == task.Params[taskjournal.TaskRunnerCheckInputParam]
}

func (repository *TaskRepository) loadRunnerCheckAt(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (runnerchecks.Record, []keyvalue.Condition, error) {
	keys := []string{
		runnerchecks.RecordKey(task.ID),
		runnerchecks.OperationKey(task.OperationID),
		runnerchecks.ActiveKey(task.Target),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return runnerchecks.Record{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return runnerchecks.Record{}, nil, errs.New(errs.KindStateConflict, "Runner diagnostic input is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	record, err := runnerchecks.Decode(read.Values[0].Value)
	if err != nil {
		return runnerchecks.Record{}, nil, err
	}
	if !runnerCheckMatchesTask(record, task) {
		return runnerchecks.Record{}, nil, errs.New(
			errs.KindStateConflict,
			"Runner diagnostic differs from its sealed Task",
		)
	}
	conditions := make([]keyvalue.Condition, 0, len(keys))
	for index, key := range keys {
		value := read.Values[index]
		if index > 0 && (record.Phase != runnerchecks.Finished && (value == nil || string(value.Value) != task.ID) ||
			record.Phase == runnerchecks.Finished && value != nil && (index == 1 || string(value.Value) == task.ID || ids.Validate(ids.KindTask, string(value.Value)) != nil)) {
			return runnerchecks.Record{}, nil, errs.New(
				errs.KindStateConflict,
				"Runner diagnostic operation ownership changed",
			)
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)})
	}
	return record, conditions, nil
}

func (repository *TaskRepository) prepareRunnerCheckTerminal(
	ctx context.Context,
	task TaskRecord,
	status taskjournal.TaskStatus,
	revision int64,
	at time.Time,
) (resourceTaskChange, error) {
	record, conditions, err := repository.loadRunnerCheckAt(ctx, task, revision)
	if err != nil {
		return resourceTaskChange{}, err
	}
	change := resourceTaskChange{applies: true, conditions: conditions}
	if record.Phase != runnerchecks.Finished {
		if status == taskjournal.TaskStatusCompleted {
			return resourceTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Runner diagnostic has not recorded its results",
			)
		}
		failure := "journal"
		if status == taskjournal.TaskStatusAborted {
			failure = "canceled"
		}
		if status == taskjournal.TaskStatusTimedOut {
			failure = "timeout"
		}
		if err := record.Stop(failure, at); err != nil {
			return resourceTaskChange{}, err
		}
		value, err := runnerchecks.Encode(record)
		if err != nil {
			return resourceTaskChange{}, err
		}
		change.values = append(change.values, value)
		change.mutations = append(
			change.mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: runnerchecks.RecordKey(task.ID), Value: value},
			keyvalue.Mutation{
				Type: keyvalue.MutationDelete,
				Key:  runnerchecks.OperationKey(task.OperationID),
			},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: runnerchecks.ActiveKey(task.Target)},
		)
	}
	if status == taskjournal.TaskStatusCompleted && record.Outcome != "passed" {
		clearResourceTaskChange(change)
		return resourceTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Runner diagnostic has no successful result; inspect its individual stages",
		)
	}
	return change, nil
}
