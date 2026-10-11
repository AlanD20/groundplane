package edgechecks

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareCleanupTransfer is joined with the native cleanup-only Retry Task and
// idempotency publication. The original helper identity is immutable. No second
// forward probe or credential material is created by the transfer.
func (repository *Repository) PrepareCleanupTransfer(
	ctx context.Context,
	taskID, retryTaskID string,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	current, err := repository.LoadAt(ctx, taskID, revision)
	if err != nil {
		return nil, nil, err
	}
	previous, retry, err := PrepareCleanupRetry(current.Record, retryTaskID)
	if err != nil {
		return nil, nil, err
	}
	keys := []string{
		Key(retryTaskID),
		OperationKey(previous.Owner.OperationID),
		LatestKey(previous.Source.Request.TargetID()),
		taskjournal.ControllerTaskClaimKey(taskID),
		taskjournal.TaskAssignmentIndexKey(taskID),
	}
	oldReferences, newReferences := ReferenceKeys(current.Record), ReferenceKeys(retry)
	keys = append(keys, oldReferences...)
	keys = append(keys, newReferences...)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: current.ReadRevision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != current.ReadRevision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return nil, nil, errs.New(errs.KindInternal, "edge diagnostic cleanup transfer read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if latest := read.Values[2]; latest != nil && ids.Validate(ids.KindTask, string(latest.Value)) != nil {
		return nil, nil, errs.New(errs.KindInternal, "edge diagnostic latest index is corrupt")
	}
	if read.Values[0] != nil || read.Values[1] == nil || string(read.Values[1].Value) != taskID ||
		read.Values[3] != nil || read.Values[4] != nil {
		return nil, nil, errs.New(
			errs.KindStateConflict,
			"edge diagnostic cleanup Retry no longer owns the interrupted operation",
		)
	}
	conditions := []keyvalue.Condition{{Key: Key(taskID), ModRevision: current.Revision}}
	for index, key := range keys {
		condition := keyvalue.Condition{Key: key}
		if read.Values[index] != nil {
			condition.ModRevision = read.Values[index].ModRevision
		}
		conditions = append(conditions, condition)
	}
	for index := range oldReferences {
		old, next := read.Values[5+index], read.Values[5+len(oldReferences)+index]
		if old == nil || string(old.Value) != taskID || next != nil {
			return nil, nil, errs.New(
				errs.KindStateConflict,
				"edge diagnostic helper references changed before cleanup Retry",
			)
		}
	}
	priorValue, err := Encode(previous)
	if err != nil {
		return nil, nil, err
	}
	retryValue, err := Encode(retry)
	if err != nil {
		return nil, nil, err
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: Key(taskID), Value: priorValue},
		{Type: keyvalue.MutationPut, Key: Key(retryTaskID), Value: retryValue},
		{Type: keyvalue.MutationPut, Key: OperationKey(previous.Owner.OperationID), Value: []byte(retryTaskID)}}
	for index, key := range oldReferences {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key},
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: newReferences[index], Value: []byte(retryTaskID)})
	}
	if latest := read.Values[2]; latest != nil && string(latest.Value) == taskID {
		mutations = append(
			mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[2], Value: []byte(retryTaskID)},
		)
	}
	return conditions, mutations, nil
}
