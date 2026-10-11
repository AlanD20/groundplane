package etcd

import (
	"context"
	"maps"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func edgeCheckTask(task TaskRecord) bool {
	return task.Type == taskjournal.TaskCheck &&
		(task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceRoute ||
			task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceComponent && task.Params[taskjournal.TaskEdgeCheckInputParam] != "")
}

func validateEdgeCheckTask(task TaskRecord) error {
	if !edgeCheckTask(task) {
		return nil
	}
	if task.Executor != taskjournal.TaskExecutorController || task.Actor != taskjournal.TaskActorOperator ||
		!validEdgeCheckTarget(task) || ids.Validate(ids.KindEnvironment, task.Owner.EnvironmentID) != nil ||
		len(task.Params) != 2 || len(task.Steps) != 0 || len(task.Materializations) != 0 || task.Configuration != nil ||
		task.EntryRuntime != nil || task.RenderGeneration != 1 || task.TimeoutSeconds != contract.TaskTimeoutSeconds ||
		!recordcodec.ValidSHA256(task.Params[taskjournal.TaskEdgeCheckInputParam]) {
		return errs.New(errs.KindValidationFailed, "Tunnel check Task input is invalid")
	}
	return nil
}

func edgeCheckMatchesTask(record edgechecks.Record, task TaskRecord) bool {
	hash, err := edgechecks.PlanHash(record)
	return err == nil && edgeCheckTask(task) && validateEdgeCheckTask(task) == nil &&
		record.Owner.TaskID == task.ID && record.Owner.PlanID == task.PlanID && record.Owner.OperationID == task.OperationID &&
		record.Owner.EnvironmentID == task.Owner.EnvironmentID && record.Source.Request.TargetID() == task.Target &&
		(record.Source.Request.RouteID != "") == (task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceRoute) &&
		record.RetryOf == task.RetryOf && hash == task.PlanHash && record.ConfigurationSHA256 == task.Params[taskjournal.TaskEdgeCheckInputParam]
}

func validEdgeCheckTarget(task TaskRecord) bool {
	if task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceRoute {
		return ids.Validate(ids.KindRoute, task.Target) == nil
	}
	return task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceComponent &&
		ids.Validate(ids.KindComponent, task.Target) == nil
}

func (repository *TaskRepository) loadEdgeCheckAt(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (edgechecks.Record, []keyvalue.Condition, error) {
	records, err := edgechecks.NewRepository(repository.store)
	if err != nil {
		return edgechecks.Record{}, nil, err
	}
	current, err := records.LoadAt(ctx, task.ID, revision)
	if err != nil {
		return edgechecks.Record{}, nil, err
	}
	record := current.Record
	if !edgeCheckMatchesTask(record, task) {
		return edgechecks.Record{}, nil, errs.New(
			errs.KindStateConflict,
			"Tunnel check input differs from its sealed Task",
		)
	}
	keys := append(edgechecks.ReferenceKeys(record), edgechecks.OperationKey(task.OperationID))
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return edgechecks.Record{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return edgechecks.Record{}, nil, errs.New(errs.KindInternal, "Tunnel check ownership read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := []keyvalue.Condition{{Key: edgechecks.Key(task.ID), ModRevision: current.Revision}}
	released := record.Phase == edgechecks.Finished || record.Phase == edgechecks.Transferred
	for index, key := range keys {
		value := read.Values[index]
		if !released && (value == nil || string(value.Value) != task.ID) || released && value != nil &&
			(record.Phase == edgechecks.Finished || index != len(keys)-1 || ids.Validate(ids.KindTask, string(value.Value)) != nil || string(value.Value) == task.ID) {
			return edgechecks.Record{}, nil, errs.New(errs.KindStateConflict, "Tunnel check helper ownership changed")
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)})
	}
	return record, conditions, nil
}

func (repository *TaskRepository) prepareEdgeCheckTerminal(
	ctx context.Context,
	task TaskRecord,
	status taskjournal.TaskStatus,
	revision int64,
	at time.Time,
) (resourceTaskChange, error) {
	record, conditions, err := repository.loadEdgeCheckAt(ctx, task, revision)
	if err != nil {
		return resourceTaskChange{}, err
	}
	change := resourceTaskChange{applies: true, conditions: conditions}
	if record.Phase != edgechecks.Finished && record.Phase != edgechecks.Transferred {
		if status == taskjournal.TaskStatusCompleted {
			return resourceTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Tunnel check cannot complete without its result and exact helper cleanup",
			)
		}
		if record.Phase != edgechecks.CleanupRequired {
			failure := "journal"
			switch status {
			case taskjournal.TaskStatusAborted:
				failure = string(contract.Canceled)
			case taskjournal.TaskStatusTimedOut:
				failure = string(contract.TimeoutFailure)
			default:
				if record.PrivateFailure != "" {
					failure = record.PrivateFailure
				} else if record.PublicFailure != "" {
					failure = record.PublicFailure
				}
			}
			if err := record.Stop(failure, at); err != nil {
				return resourceTaskChange{}, err
			}
			value, err := edgechecks.Encode(record)
			if err != nil {
				return resourceTaskChange{}, err
			}
			change.values = append(change.values, value)
			change.mutations = append(
				change.mutations,
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: edgechecks.Key(task.ID), Value: value},
			)
			if record.Phase == edgechecks.Finished {
				for _, key := range append(edgechecks.ReferenceKeys(record), edgechecks.OperationKey(task.OperationID)) {
					change.mutations = append(
						change.mutations,
						keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key},
					)
				}
			}
		}
	}
	if status == taskjournal.TaskStatusCompleted && (record.Phase != edgechecks.Finished ||
		record.Outcome != "passed" && record.Outcome != "protected" && !(record.Outcome == "interrupted" && task.RetryOf != "")) {
		clearResourceTaskChange(change)
		return resourceTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Tunnel check has no completed probe or proven cleanup Retry",
		)
	}
	return change, nil
}

func (repository *TaskRepository) prepareEdgeCheckRetry(
	ctx context.Context,
	source, retry TaskRecord,
	revision int64,
) (resourceTaskChange, error) {
	record, _, err := repository.loadEdgeCheckAt(ctx, source, revision)
	if err != nil {
		return resourceTaskChange{}, err
	}
	if retry.RetryOf != source.ID || retry.OperationID != source.OperationID || retry.PlanID != source.PlanID ||
		retry.PlanHash != source.PlanHash || retry.Owner != source.Owner || retry.Target != source.Target ||
		retry.Executor != source.Executor || retry.RenderGeneration != source.RenderGeneration || retry.TimeoutSeconds != source.TimeoutSeconds ||
		!maps.Equal(
			retry.Params,
			source.Params,
		) || !edgeCheckTask(retry) || record.Phase != edgechecks.CleanupRequired {
		return resourceTaskChange{}, errs.New(
			errs.KindTaskNotRetryable,
			"only pending Tunnel helper cleanup can be retried; submit a new connection check to probe again",
		)
	}
	records, err := edgechecks.NewRepository(repository.store)
	if err != nil {
		return resourceTaskChange{}, err
	}
	conditions, mutations, err := records.PrepareCleanupTransfer(ctx, source.ID, retry.ID, revision)
	if err != nil {
		return resourceTaskChange{}, err
	}
	return resourceTaskChange{applies: true, conditions: conditions, mutations: mutations}, nil
}
