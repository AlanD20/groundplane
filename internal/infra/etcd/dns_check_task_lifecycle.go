package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func dnsCheckTask(task TaskRecord) bool {
	return task.Type == taskjournal.TaskCheck &&
		task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceComponent &&
		task.Params[taskjournal.TaskEdgeCheckInputParam] == ""
}

func validateDNSCheckTask(task TaskRecord) error {
	if !dnsCheckTask(task) {
		return nil
	}
	if task.Executor != taskjournal.TaskExecutorController || task.Actor != taskjournal.TaskActorOperator ||
		task.Owner != taskjournal.PlatformTaskOwner() || ids.Validate(ids.KindComponent, task.Target) != nil ||
		len(task.Params) != 2 || len(task.Steps) != 0 || len(task.Materializations) != 0 || task.Configuration != nil ||
		task.EntryRuntime != nil || task.RenderGeneration != 1 || task.TimeoutSeconds != 90 || task.RetryOf != "" ||
		!recordcodec.ValidSHA256(task.Params[taskjournal.TaskDNSCheckInputParam]) {
		return errs.New(errs.KindValidationFailed, "DNS check Task input is invalid")
	}
	return nil
}

func dnsCheckMatchesTask(record dnschecks.Record, task TaskRecord) bool {
	hash, err := dnschecks.PlanHash(record)
	return err == nil && dnsCheckTask(task) && validateDNSCheckTask(task) == nil &&
		record.Owner.TaskID == task.ID && record.Owner.PlanID == task.PlanID && record.Owner.OperationID == task.OperationID &&
		record.Source.ComponentID == task.Target && hash == task.PlanHash &&
		record.ConfigurationSHA256 == task.Params[taskjournal.TaskDNSCheckInputParam]
}

func (repository *TaskRepository) loadDNSCheckAt(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (dnschecks.Record, []keyvalue.Condition, error) {
	keys := []string{
		dnschecks.RecordKey(task.ID),
		dnschecks.OperationKey(task.OperationID),
		dnschecks.ActiveKey(task.Target),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return dnschecks.Record{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return dnschecks.Record{}, nil, errs.New(errs.KindStateConflict, "DNS check captured input is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	record, err := dnschecks.Decode(read.Values[0].Value)
	if err != nil {
		return dnschecks.Record{}, nil, err
	}
	if !dnsCheckMatchesTask(record, task) {
		return dnschecks.Record{}, nil, errs.New(errs.KindStateConflict, "DNS check differs from its sealed Task")
	}
	conditions := make([]keyvalue.Condition, 0, len(keys))
	for index, key := range keys {
		value := read.Values[index]
		if index > 0 {
			if record.Phase != dnschecks.Finished && (value == nil || string(value.Value) != task.ID) ||
				record.Phase == dnschecks.Finished && value != nil && (index == 1 || string(value.Value) == task.ID) {
				return dnschecks.Record{}, nil, errs.New(
					errs.KindStateConflict,
					"DNS check operation ownership changed",
				)
			}
		}
		// A newer check may own ActiveKey after this one finished. Its revision
		// is fenced but neither terminal replay nor pruning may delete it.
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(value)})
	}
	return record, conditions, nil
}

func (repository *TaskRepository) prepareDNSCheckTerminal(ctx context.Context, task TaskRecord,
	status taskjournal.TaskStatus, revision int64, at time.Time,
) (resourceTaskChange, error) {
	record, conditions, err := repository.loadDNSCheckAt(ctx, task, revision)
	if err != nil {
		return resourceTaskChange{}, err
	}
	change := resourceTaskChange{applies: true, conditions: conditions}
	if record.Phase != dnschecks.Finished {
		if status == taskjournal.TaskStatusCompleted {
			return resourceTaskChange{}, errs.New(
				errs.KindStateConflict,
				"DNS check has not recorded its query results",
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
		value, err := dnschecks.Encode(record)
		if err != nil {
			return resourceTaskChange{}, err
		}
		change.values = append(change.values, value)
		change.mutations = append(change.mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: dnschecks.RecordKey(task.ID), Value: value},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: dnschecks.OperationKey(task.OperationID)},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: dnschecks.ActiveKey(task.Target)})
	}
	if status == taskjournal.TaskStatusCompleted && record.Outcome != "passed" {
		clearResourceTaskChange(change)
		return resourceTaskChange{}, errs.New(
			errs.KindStateConflict,
			"DNS check has no successful resolution result; inspect the query outcomes",
		)
	}
	return change, nil
}
