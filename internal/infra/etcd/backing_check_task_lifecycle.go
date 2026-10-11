package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func backingCheckTask(task TaskRecord) bool {
	return task.Type == taskjournal.TaskCheck &&
		task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceAttach
}

func validateBackingCheckTask(task TaskRecord) error {
	if task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceAttach {
		return nil
	}
	if !backingCheckTask(task) || task.Executor != taskjournal.TaskExecutorController ||
		task.Actor != taskjournal.TaskActorOperator ||
		ids.Validate(ids.KindAttach, task.Target) != nil ||
		ids.Validate(ids.KindEnvironment, task.Owner.EnvironmentID) != nil ||
		len(task.Params) != 2 ||
		len(task.Steps) != 0 ||
		len(task.Materializations) != 0 ||
		task.Configuration != nil ||
		task.EntryRuntime != nil ||
		task.RenderGeneration != 1 ||
		task.TimeoutSeconds != 60 ||
		!recordcodec.ValidSHA256(task.Params[taskjournal.TaskBackingCheckInputParam]) {
		return errs.New(errs.KindValidationFailed, "Backing access check Task input is invalid")
	}
	return nil
}

func backingCheckMatchesTask(record backingchecks.Record, task TaskRecord) bool {
	hash, err := backingchecks.PlanHash(record)
	return err == nil && backingCheckTask(task) && validateBackingCheckTask(task) == nil &&
		record.Owner.TaskID == task.ID && record.Owner.PlanID == task.PlanID && record.Owner.OperationID == task.OperationID &&
		record.Owner.EnvironmentID == task.Owner.EnvironmentID && record.Source.Parameters.AttachID == task.Target &&
		record.RetryOf == task.RetryOf && hash == task.PlanHash && record.ConfigurationSHA256 == task.Params[taskjournal.TaskBackingCheckInputParam]
}

func (repository *TaskRepository) loadBackingCheckAt(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (backingchecks.Record, []keyvalue.Condition, error) {
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{backingchecks.Key(task.ID)}, Revision: revision},
	)
	if err != nil {
		return backingchecks.Record{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return backingchecks.Record{}, nil, errs.New(
			errs.KindStateConflict,
			"Backing access captured input is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	record, err := backingchecks.Decode(read.Values[0].Value)
	if err != nil {
		return backingchecks.Record{}, nil, err
	}
	if !backingCheckMatchesTask(record, task) {
		record.Clear()
		return backingchecks.Record{}, nil, errs.New(
			errs.KindStateConflict,
			"Backing access input differs from its Task",
		)
	}
	keys := append(backingchecks.ReferenceKeys(record), backingchecks.OperationKey(task.OperationID))
	refs, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		record.Clear()
		return backingchecks.Record{}, nil, err
	}
	if refs == nil || refs.ReadRevision != revision || len(refs.Values) != len(keys) {
		if refs != nil {
			keyvalue.ClearValues(refs.Values)
		}
		record.Clear()
		return backingchecks.Record{}, nil, errs.New(errs.KindInternal, "Backing access ownership read is incomplete")
	}
	defer keyvalue.ClearValues(refs.Values)
	conditions := []keyvalue.Condition{{Key: backingchecks.Key(task.ID), ModRevision: read.Values[0].ModRevision}}
	released := record.Phase == backingchecks.Finished || record.Phase == backingchecks.Transferred
	for index, key := range keys {
		value := refs.Values[index]
		if !released && (value == nil || string(value.Value) != task.ID) || released && value != nil &&
			(record.Phase == backingchecks.Finished || index != len(keys)-1 || ids.Validate(ids.KindTask, string(value.Value)) != nil || string(value.Value) == task.ID) {
			record.Clear()
			return backingchecks.Record{}, nil, errs.New(
				errs.KindStateConflict,
				"Backing access cleanup ownership changed",
			)
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)})
	}
	return record, conditions, nil
}

func (repository *TaskRepository) prepareBackingCheckTerminal(
	ctx context.Context,
	task TaskRecord,
	status taskjournal.TaskStatus,
	revision int64,
	at time.Time,
) (resourceTaskChange, error) {
	record, conditions, err := repository.loadBackingCheckAt(ctx, task, revision)
	if err != nil {
		return resourceTaskChange{}, err
	}
	defer record.Clear()
	change := resourceTaskChange{applies: true, conditions: conditions}
	if record.Phase != backingchecks.Finished {
		if status == taskjournal.TaskStatusCompleted {
			return resourceTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Backing check cannot complete without its result and exact helper cleanup",
			)
		}
		failure := backingchecks.JournalFailure
		switch status {
		case taskjournal.TaskStatusAborted:
			failure = string(backingaccess.Canceled)
		case taskjournal.TaskStatusTimedOut:
			failure = string(backingaccess.TimeoutFailure)
		default:
			if record.ProbeRecorded && record.ProbeFailure != "" {
				failure = record.ProbeFailure
			}
		}
		if err := record.Stop(failure, at); err != nil {
			return resourceTaskChange{}, err
		}
		value, err := backingchecks.Encode(record)
		if err != nil {
			return resourceTaskChange{}, err
		}
		change.values = append(change.values, value)
		change.mutations = append(
			change.mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: backingchecks.Key(task.ID), Value: value},
		)
		if record.Phase == backingchecks.Finished {
			for _, key := range append(backingchecks.ReferenceKeys(record), backingchecks.OperationKey(task.OperationID)) {
				change.mutations = append(change.mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
			}
		}
	}
	if status == taskjournal.TaskStatusCompleted && record.Outcome != backingchecks.Passed &&
		!(record.Outcome == backingchecks.Interrupted && task.RetryOf != "") {
		clearResourceTaskChange(change)
		return resourceTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Backing check has no passing probe or completed cleanup Retry",
		)
	}
	return change, nil
}

func (repository *TaskRepository) prepareBackingCheckRetry(
	ctx context.Context,
	source, retry TaskRecord,
	revision int64,
) (resourceTaskChange, error) {
	record, conditions, err := repository.loadBackingCheckAt(ctx, source, revision)
	if err != nil {
		return resourceTaskChange{}, err
	}
	defer record.Clear()
	if retry.RetryOf != source.ID || retry.OperationID != source.OperationID || retry.PlanID != source.PlanID ||
		retry.PlanHash != source.PlanHash || retry.Owner != source.Owner || retry.Target != source.Target ||
		retry.Params[taskjournal.TaskBackingCheckInputParam] != source.Params[taskjournal.TaskBackingCheckInputParam] || !backingCheckTask(retry) {
		return resourceTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Backing cleanup Retry changed its captured input",
		)
	}
	previous, next, err := backingchecks.PrepareCleanupRetry(record, retry.ID)
	if err != nil {
		return resourceTaskChange{}, err
	}
	defer previous.Clear()
	defer next.Clear()
	previousValue, err := backingchecks.Encode(previous)
	if err != nil {
		return resourceTaskChange{}, err
	}
	nextValue, err := backingchecks.Encode(next)
	if err != nil {
		clear(previousValue)
		return resourceTaskChange{}, err
	}
	change := resourceTaskChange{
		applies:    true,
		conditions: append(conditions, keyvalue.Condition{Key: backingchecks.Key(retry.ID)}),
		values:     [][]byte{previousValue, nextValue},
		mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: backingchecks.Key(source.ID), Value: previousValue},
			{Type: keyvalue.MutationPut, Key: backingchecks.Key(retry.ID), Value: nextValue},
			{Type: keyvalue.MutationPut, Key: backingchecks.OperationKey(source.OperationID), Value: []byte(retry.ID)},
		},
	}
	for _, key := range backingchecks.ReferenceKeys(previous) {
		change.mutations = append(change.mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	for _, key := range backingchecks.ReferenceKeys(next) {
		change.conditions = append(change.conditions, keyvalue.Condition{Key: key})
		change.mutations = append(
			change.mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(retry.ID)},
		)
	}
	latestKey := backingchecks.LatestKey(next.Source.Parameters.AttachID, next.Source.GrantID)
	latest, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{latestKey}, Revision: revision})
	if err != nil {
		clearResourceTaskChange(change)
		return resourceTaskChange{}, err
	}
	if latest == nil || latest.ReadRevision != revision || len(latest.Values) != 1 {
		if latest != nil {
			keyvalue.ClearValues(latest.Values)
		}
		clearResourceTaskChange(change)
		return resourceTaskChange{}, errs.New(errs.KindInternal, "Backing check latest Retry read is incomplete")
	}
	defer keyvalue.ClearValues(latest.Values)
	if latest.Values[0] != nil && string(latest.Values[0].Value) == source.ID {
		change.conditions = append(
			change.conditions,
			keyvalue.Condition{Key: latestKey, ModRevision: latest.Values[0].ModRevision},
		)
		change.mutations = append(
			change.mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: latestKey, Value: []byte(retry.ID)},
		)
	}
	return change, nil
}
