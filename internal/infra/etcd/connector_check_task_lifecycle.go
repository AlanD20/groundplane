package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func connectorCheckTask(task TaskRecord) bool {
	return task.Type == taskjournal.TaskCheck &&
		task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceConnector
}

func (repository *TaskRepository) loadConnectorCheckAt(ctx context.Context, task TaskRecord, revision int64,
) (connectorchecks.Record, []keyvalue.Condition, error) {
	keys := []string{
		connectorchecks.Key(task.ID),
		connectorchecks.ReferenceKey(task.Target, task.ID),
		connectorchecks.OperationKey(task.OperationID),
		connectorchecks.EnvironmentReferenceKey(task.Owner.EnvironmentID, task.ID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return connectorchecks.Record{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 4 || read.Values[0] == nil {
		return connectorchecks.Record{}, nil, errs.New(
			errs.KindStateConflict,
			"connector check captured input is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	record, err := connectorchecks.Decode(read.Values[0].Value)
	if err != nil {
		return connectorchecks.Record{}, nil, err
	}
	planHash, err := connectorchecks.PlanHash(record)
	if err != nil || !connectorCheckTask(task) || task.Executor != taskjournal.TaskExecutorController ||
		len(task.Params) != 2 || len(task.Steps) != 0 || task.RenderGeneration != 1 ||
		record.Owner.TaskID != task.ID || record.Owner.PlanID != task.PlanID || record.Owner.OperationID != task.OperationID ||
		record.Owner.EnvironmentID != task.Owner.EnvironmentID || record.Connector.Connector.ID != task.Target ||
		record.RetryOf != task.RetryOf || task.PlanHash != planHash ||
		task.Params[taskjournal.TaskConnectorCheckInputParam] != record.ConfigurationSHA256 {
		record.Clear()
		return connectorchecks.Record{}, nil, errs.New(
			errs.KindStateConflict,
			"connector check captured input differs from its Task",
		)
	}
	released := record.Phase == connectorchecks.Finished || record.Phase == connectorchecks.Transferred
	if released && (read.Values[1] != nil || read.Values[3] != nil) || !released &&
		(read.Values[1] == nil || string(read.Values[1].Value) != task.ID || read.Values[2] == nil || string(read.Values[2].Value) != task.ID ||
			read.Values[3] == nil || string(read.Values[3].Value) != task.ID) ||
		record.Phase == connectorchecks.Finished && read.Values[2] != nil ||
		record.Phase == connectorchecks.Transferred && read.Values[2] != nil &&
			(ids.Validate(ids.KindTask, string(read.Values[2].Value)) != nil || string(read.Values[2].Value) == task.ID) {
		record.Clear()
		return connectorchecks.Record{}, nil, errs.New(
			errs.KindStateConflict,
			"connector check cleanup ownership changed",
		)
	}
	return record, []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: keyvalue.RevisionOf(read.Values[1])},
		{Key: keys[2], ModRevision: keyvalue.RevisionOf(read.Values[2])},
		{Key: keys[3], ModRevision: keyvalue.RevisionOf(read.Values[3])},
	}, nil
}

func (repository *TaskRepository) prepareConnectorCheckTerminal(ctx context.Context, task TaskRecord,
	status taskjournal.TaskStatus, revision int64, at time.Time,
) (resourceTaskChange, error) {
	record, conditions, err := repository.loadConnectorCheckAt(ctx, task, revision)
	if err != nil {
		return resourceTaskChange{}, err
	}
	defer record.Clear()
	change := resourceTaskChange{applies: true, conditions: conditions}
	if record.Phase != connectorchecks.Finished {
		if status == taskjournal.TaskStatusCompleted {
			return resourceTaskChange{}, errs.New(
				errs.KindStateConflict,
				"connector check cannot complete before its result and cleanup are recorded",
			)
		}
		failure := s3connector.CheckFailureJournal
		switch status {
		case taskjournal.TaskStatusAborted:
			failure = s3connector.CheckFailureCanceled
		case taskjournal.TaskStatusTimedOut:
			failure = s3connector.CheckFailureTimeout
		default:
			for _, stage := range record.Stages {
				if !stage.Succeeded {
					failure = stage.Failure
					break
				}
			}
		}
		if err := record.Stop(failure, at); err != nil {
			return resourceTaskChange{}, err
		}
		value, err := connectorchecks.Encode(record)
		if err != nil {
			return resourceTaskChange{}, err
		}
		change.values = append(change.values, value)
		change.mutations = append(change.mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: connectorchecks.Key(task.ID), Value: value})
		if record.Phase == connectorchecks.Finished {
			change.mutations = append(change.mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
				Key: connectorchecks.ReferenceKey(
					task.Target,
					task.ID,
				)}, keyvalue.Mutation{Type: keyvalue.MutationDelete,
				Key: connectorchecks.OperationKey(task.OperationID)})
			change.mutations = append(change.mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
				Key: connectorchecks.EnvironmentReferenceKey(task.Owner.EnvironmentID, task.ID)})
		}
	}
	if record.Phase == connectorchecks.Finished && status == taskjournal.TaskStatusCompleted &&
		record.Outcome != connectorchecks.Passed && !(record.Outcome == connectorchecks.Interrupted && task.RetryOf != "") {
		clearResourceTaskChange(change)
		return resourceTaskChange{}, errs.New(
			errs.KindStateConflict,
			"connector check has no completed probe or cleanup result",
		)
	}
	if record.Phase == connectorchecks.Finished && task.Configuration != nil && task.Configuration.SecretPins != nil {
		pins, err := repository.beginRecoverySecretPinRelease(ctx, task)
		if err != nil {
			clearResourceTaskChange(change)
			return resourceTaskChange{}, err
		}
		change.conditions = append(change.conditions, pins.conditions...)
		change.mutations = append(change.mutations, pins.mutations...)
		for _, mutation := range pins.mutations {
			change.values = append(change.values, mutation.Value)
		}
	}
	return change, nil
}

func (repository *TaskRepository) prepareConnectorCheckRetry(ctx context.Context, source, retry TaskRecord,
	revision int64,
) (resourceTaskChange, error) {
	record, conditions, err := repository.loadConnectorCheckAt(ctx, source, revision)
	if err != nil {
		return resourceTaskChange{}, err
	}
	defer record.Clear()
	if retry.RetryOf != source.ID || retry.OperationID != source.OperationID || retry.PlanID != source.PlanID ||
		retry.PlanHash != source.PlanHash || retry.Owner != source.Owner || retry.Target != source.Target ||
		retry.Params[taskjournal.TaskConnectorCheckInputParam] != source.Params[taskjournal.TaskConnectorCheckInputParam] {
		return resourceTaskChange{}, errs.New(
			errs.KindStateConflict,
			"connector cleanup Retry changed its captured input",
		)
	}
	previous, next, err := connectorchecks.PrepareCleanupRetry(record, retry.ID)
	if err != nil {
		return resourceTaskChange{}, err
	}
	defer next.Clear()
	previousValue, err := connectorchecks.Encode(previous)
	if err != nil {
		return resourceTaskChange{}, err
	}
	nextValue, err := connectorchecks.Encode(next)
	if err != nil {
		clear(previousValue)
		return resourceTaskChange{}, err
	}
	nextKey, nextReference := connectorchecks.Key(retry.ID), connectorchecks.ReferenceKey(retry.Target, retry.ID)
	nextEnvironmentReference := connectorchecks.EnvironmentReferenceKey(retry.Owner.EnvironmentID, retry.ID)
	change := resourceTaskChange{applies: true, conditions: append(
		conditions,
		keyvalue.Condition{
			Key: nextKey,
		},
		keyvalue.Condition{Key: nextReference},
		keyvalue.Condition{Key: nextEnvironmentReference},
	),
		values: [][]byte{previousValue, nextValue}, mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: connectorchecks.Key(source.ID), Value: previousValue},
			{Type: keyvalue.MutationPut, Key: nextKey, Value: nextValue},
			{Type: keyvalue.MutationDelete, Key: connectorchecks.ReferenceKey(source.Target, source.ID)},
			{Type: keyvalue.MutationPut, Key: nextReference, Value: []byte(retry.ID)},
			{
				Type:  keyvalue.MutationPut,
				Key:   connectorchecks.OperationKey(source.OperationID),
				Value: []byte(retry.ID),
			},
			{
				Type: keyvalue.MutationDelete,
				Key:  connectorchecks.EnvironmentReferenceKey(source.Owner.EnvironmentID, source.ID),
			},
			{Type: keyvalue.MutationPut, Key: nextEnvironmentReference, Value: []byte(retry.ID)},
		}}
	if !record.Draft {
		latestKey := connectorchecks.LatestKey(source.Target)
		latest, err := repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{Keys: []string{latestKey}, Revision: revision},
		)
		if err != nil {
			clearResourceTaskChange(change)
			return resourceTaskChange{}, err
		}
		if latest == nil || latest.ReadRevision != revision || len(latest.Values) != 1 {
			clearResourceTaskChange(change)
			return resourceTaskChange{}, errs.New(
				errs.KindInternal,
				"connector cleanup Retry selection read is incomplete",
			)
		}
		defer keyvalue.ClearValues(latest.Values)
		change.conditions = append(
			change.conditions,
			keyvalue.Condition{Key: latestKey, ModRevision: keyvalue.RevisionOf(latest.Values[0])},
		)
		// Cleanup of an older failure must not replace a newer permission check.
		if latest.Values[0] != nil && string(latest.Values[0].Value) == source.ID {
			change.mutations = append(
				change.mutations,
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: latestKey, Value: []byte(retry.ID)},
			)
		}
	}
	return change, nil
}

func (repository *TaskRepository) validateConnectorCheckReplay(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) error {
	record, _, err := repository.loadConnectorCheckAt(ctx, task, revision)
	if err != nil {
		return err
	}
	defer record.Clear()
	if record.Phase != connectorchecks.Finished && record.Phase != connectorchecks.CleanupRequired &&
		record.Phase != connectorchecks.Transferred {
		return errs.New(errs.KindStateConflict, "terminal connector check still has forward execution authority")
	}
	return nil
}
