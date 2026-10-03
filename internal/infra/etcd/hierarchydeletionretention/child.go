package hierarchydeletionretention

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type Reader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

// ChildHeld preserves the immutable child plan and its native Secret inputs
// until the parent's operation and cleanup fence are retired together.
func ChildHeld(ctx context.Context, store Reader, parentOperationID string, revision int64) (bool, error) {
	if !hierarchydeletion.ValidHierarchyDeletionPrivateID(parentOperationID, "del") {
		return false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	intentKey, _ := hierarchydeletion.HierarchyDeletionIntentKey(parentOperationID)
	fenceKey, _ := hierarchydeletion.HierarchyDeletionCleanupFenceKey(parentOperationID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{intentKey, fenceKey}, Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil && read.Values[1] == nil {
		return false, nil
	}
	if read.Values[0] == nil || read.Values[1] == nil {
		return false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var intent hierarchydeletion.HierarchyDeletionIntent
	var fence hierarchydeletion.HierarchyDeletionCleanupFence
	if hierarchydeletion.DecodeHierarchyDeletionRecord(read.Values[0].Value,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes, &intent) != nil ||
		hierarchydeletion.DecodeHierarchyDeletionRecord(read.Values[1].Value,
			hierarchydeletion.HierarchyDeletionLargeRecordBytes, &fence) != nil ||
		intent.Schema != 1 || fence.Schema != 1 || intent.OperationID != parentOperationID ||
		fence.ParentOperationID != parentOperationID || intent.DeletionEpoch <= 0 ||
		fence.DeletionEpoch != intent.DeletionEpoch || fence.TargetKind != intent.TargetKind ||
		fence.TargetID != intent.TargetID || !hierarchydeletion.ValidHierarchyDeletionPhase(fence.Phase) {
		return false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return true, nil
}

// ChildRetiredConditions fences source expiry and journal pruning after the
// same parent authority checked by ChildHeld has disappeared.
func ChildRetiredConditions(parentOperationID string) []keyvalue.Condition {
	if parentOperationID == "" {
		return nil
	}
	intentKey, _ := hierarchydeletion.HierarchyDeletionIntentKey(parentOperationID)
	fenceKey, _ := hierarchydeletion.HierarchyDeletionCleanupFenceKey(parentOperationID)
	return []keyvalue.Condition{{Key: intentKey}, {Key: fenceKey}}
}
