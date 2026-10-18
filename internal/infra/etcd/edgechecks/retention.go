package edgechecks

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PruneInput precedes Task/work retention. A finished operation is insufficient:
// the visible Task must be expired, its work settled and every helper protection
// already released. Pruning never removes the latest selection of another check.
func (repository *Repository) PruneInput(ctx context.Context, taskID string, at time.Time) (bool, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || (at.IsZero() || at.Location() != time.UTC) {
		return false, errs.New(errs.KindValidationFailed, "edge check retention identity or time is invalid")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{Key(taskID), workqueue.TaskKey(taskID)}})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 2 {
		return false, errs.New(errs.KindInternal, "edge check retention read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return true, nil
	}
	if read.Values[1] == nil {
		return false, errs.New(errs.KindStateConflict, "edge check retention requires its settled Task")
	}
	record, err := Decode(read.Values[0].Value)
	if err != nil {
		return false, err
	}
	task, err := workqueue.DecodeTask(read.Values[1].Value)
	if err != nil {
		return false, err
	}
	if record.Owner.TaskID != taskID || task.ID != taskID || task.Scope.EnvironmentID != record.Owner.EnvironmentID {
		return false, errs.New(errs.KindStateConflict, "edge check retention owner differs from its Task")
	}
	if task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil || at.Before(*task.RetainUntil) || record.Phase != Finished {
		return false, nil
	}
	keys := []string{workqueue.ItemKey(record.Owner.WorkItemID), LatestKey(record.Source.Request.TargetID()), OperationKey(record.Owner.OperationID)}
	keys = append(keys, ReferenceKeys(record)...)
	owned, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: read.ReadRevision})
	if err != nil {
		return false, err
	}
	if owned == nil || owned.ReadRevision != read.ReadRevision || len(owned.Values) != len(keys) || owned.Values[0] == nil {
		return false, errs.New(errs.KindInternal, "edge check retention work read is incomplete")
	}
	defer keyvalue.ClearValues(owned.Values)
	item, err := workqueue.DecodeItem(owned.Values[0].Value)
	if err != nil {
		return false, err
	}
	if item.ID != record.Owner.WorkItemID || item.TaskID != taskID || item.TargetID != record.Source.Request.TargetID() ||
		item.Operation != workexecution.EdgeCheck || item.Phase != workexecution.Settled {
		return false, errs.New(errs.KindStateConflict, "edge check work is not settled for retention")
	}
	for _, value := range owned.Values[2:] {
		if value != nil {
			return false, nil
		}
	}
	conditions := []keyvalue.Condition{{Key: Key(taskID), ModRevision: read.Values[0].ModRevision},
		{Key: workqueue.TaskKey(taskID), ModRevision: read.Values[1].ModRevision}}
	for index, key := range keys {
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(owned.Values[index])})
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: Key(taskID)}}
	if owned.Values[1] != nil && string(owned.Values[1].Value) == taskID {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[1]})
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	return result.Succeeded, nil
}
