package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyRemovedRoot(ctx context.Context, operation hierarchydeletion.HierarchyDeletionOperation, revision int64) (workqueue.Change, error) {
	tombstone := operation.Tombstone
	primary := hierarchyDeletionPrimaryKey(tombstone.TargetKind, tombstone.TargetID)
	lockKey := hierarchydeletion.HierarchyDeletionLockKey(string(tombstone.TargetKind), tombstone.TargetID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{primary, lockKey}, Revision: revision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil || read.Values[1] == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "removal cannot finish before its root is deleted and its lock is accounted for")
	}
	var lock hierarchydeletion.HierarchyDeletionLock
	if hierarchydeletion.DecodeHierarchyDeletionRecord(read.Values[1].Value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &lock) != nil ||
		lock.ParentOperationID != tombstone.OperationID || lock.DeletionEpoch != tombstone.DeletionEpoch ||
		lock.TargetKind != tombstone.TargetKind || lock.TargetID != tombstone.TargetID {
		return workqueue.Change{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	tombstone.Phase = hierarchydeletion.HierarchyDeletionRetained
	value, err := hierarchydeletion.EncodeHierarchyDeletionRecord(tombstone, hierarchydeletion.HierarchyDeletionLargeRecordBytes)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: primary}, {Key: lockKey, ModRevision: read.Values[1].ModRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: lockKey},
			{Type: keyvalue.MutationPut, Key: hierarchydeletion.HierarchyDeletionTombstoneKey(string(tombstone.TargetKind), tombstone.TargetID), Value: value}},
	}, nil
}
