package runnerchecks

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PruneInput removes only this expired check, never the selection or active
// marker of a newer check. Operation input is retired before its Task/work.
func (repository *Repository) PruneInput(ctx context.Context, taskID string, at time.Time) (bool, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || at.IsZero() || at.Location() != time.UTC {
		return false, errs.New(errs.KindValidationFailed, "Runner retention identity or time is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{RecordKey(taskID), workqueue.TaskKey(taskID)}})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 2 {
		return false, errs.New(errs.KindInternal, "Runner retention read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return true, nil
	}
	if read.Values[1] == nil {
		return false, errs.New(errs.KindStateConflict, "Runner retention requires its settled Task")
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return false, err
	}
	task, err := workqueue.DecodeTask(read.Values[1].Value)
	if err != nil {
		return false, err
	}
	if record.Owner.TaskID != taskID || task.ID != taskID || task.Scope != (workexecution.Scope{TenantID: record.Source.Desired.TenantID, ProjectID: record.Source.Desired.ProjectID(), EnvironmentID: record.Source.Desired.EnvironmentID()}) {
		return false, errs.New(errs.KindStateConflict, "Runner retention owner differs from its Task")
	}
	if task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil || at.Before(*task.RetainUntil) || record.Phase != Finished {
		return false, nil
	}
	keys := []string{workqueue.ItemKey(record.Owner.WorkItemID), OperationKey(record.Owner.OperationID), ActiveKey(record.Source.RunnerID), LatestKey(record.Source.RunnerID)}
	owned, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: read.ReadRevision})
	if err != nil {
		return false, err
	}
	if owned == nil || owned.ReadRevision != read.ReadRevision || len(owned.Values) != len(keys) || owned.Values[0] == nil {
		return false, errs.New(errs.KindInternal, "Runner retention work read is incomplete")
	}
	defer keyvalue.ClearValues(owned.Values)
	item, err := workqueue.DecodeItem(owned.Values[0].Value)
	if err != nil {
		return false, err
	}
	if item.ID != record.Owner.WorkItemID || item.TaskID != taskID || item.TargetID != record.Source.RunnerID ||
		item.Operation != workexecution.RunnerCheck || item.Phase != workexecution.Settled {
		return false, errs.New(errs.KindStateConflict, "Runner work is not settled for retention")
	}
	if owned.Values[1] != nil || owned.Values[2] != nil && string(owned.Values[2].Value) == taskID {
		return false, nil
	}
	conditions := []keyvalue.Condition{{Key: RecordKey(taskID), ModRevision: read.Values[0].ModRevision},
		{Key: workqueue.TaskKey(taskID), ModRevision: read.Values[1].ModRevision}}
	for index, key := range keys {
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(owned.Values[index])})
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: RecordKey(taskID)}}
	if owned.Values[3] != nil && string(owned.Values[3].Value) == taskID {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[3]})
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	return result.Succeeded, nil
}
