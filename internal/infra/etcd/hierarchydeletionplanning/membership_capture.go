package hierarchydeletionplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const membershipChunkBytes = 256 << 10

type membershipCapture struct {
	OperationID string `json:"operation_id"`
	Revision    int64  `json:"revision"`
	Bytes       int    `json:"bytes"`
	SHA256      string `json:"sha256"`
}

func MembershipCapturePrefix(operationID string) string {
	return "/v1/private/hierarchy-membership/" + operationID + "/"
}

func (repository *Planner) loadMembership(ctx context.Context, operation hierarchydeletion.HierarchyDeletionTombstone) (HierarchyDeletionFrozenMembership, bool, error) {
	prefix := MembershipCapturePrefix(operation.OperationID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{prefix + "root"}})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return HierarchyDeletionFrozenMembership{}, false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		return HierarchyDeletionFrozenMembership{}, false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if read.Values[0] == nil {
		return HierarchyDeletionFrozenMembership{}, false, nil
	}
	root, err := recordcodec.Decode[membershipCapture](read.Values[0].Value, "hierarchy-membership-capture")
	if err != nil || root.OperationID != operation.OperationID || root.Revision != operation.SnapshotRevision ||
		root.Bytes <= 0 || int64(root.Bytes) > int64(workexecution.MaximumTaskItems)*int64(hierarchydeletion.HierarchyDeletionLargeRecordBytes) ||
		!recordcodec.ValidSHA256(root.SHA256) {
		return HierarchyDeletionFrozenMembership{}, false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	var raw []byte
	defer func() { clear(raw) }()
	for offset := 0; offset < root.Bytes; offset += membershipChunkBytes {
		key := prefix + "chunks/" + strconv.Itoa(offset/membershipChunkBytes)
		part, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: read.ReadRevision})
		if err != nil {
			return HierarchyDeletionFrozenMembership{}, false, err
		}
		if part == nil || part.ReadRevision != read.ReadRevision || len(part.Values) != 1 || part.Values[0] == nil ||
			part.Values[0].Key != key || len(part.Values[0].Value) != min(membershipChunkBytes, root.Bytes-offset) {
			if part != nil {
				keyvalue.ClearValues(part.Values)
			}
			return HierarchyDeletionFrozenMembership{}, false, hierarchydeletion.CorruptHierarchyDeletion()
		}
		raw = append(raw, part.Values[0].Value...)
		keyvalue.ClearValues(part.Values)
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != root.SHA256 {
		return HierarchyDeletionFrozenMembership{}, false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	frozen, err := recordcodec.Decode[HierarchyDeletionFrozenMembership](raw, "hierarchy-frozen-membership")
	if err != nil || frozen.Revision != operation.SnapshotRevision || frozen.CoordinationEpoch != operation.DeletionEpoch ||
		frozen.RootRevision != operation.TargetRevision || len(frozen.Nodes) >= int(workexecution.MaximumTaskItems) {
		return HierarchyDeletionFrozenMembership{}, false, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return frozen, true, nil
}

// Chunks are immutable and cannot be replaced while the root is absent. The
// root publishes only after all chunks exist; incomplete staging is not readable
// membership and never authorizes work. Every write compares the planning fence.
func (repository *Planner) saveMembership(ctx context.Context, operation hierarchydeletion.HierarchyDeletionTombstone,
	frozen HierarchyDeletionFrozenMembership,
) error {
	if len(frozen.Nodes) >= int(workexecution.MaximumTaskItems) {
		return errs.New(errs.KindValidationFailed, "removal membership exceeds the Task work limit")
	}
	raw, err := recordcodec.Encode("hierarchy-frozen-membership", frozen)
	if err != nil {
		return err
	}
	defer clear(raw)
	if int64(len(raw)) > int64(workexecution.MaximumTaskItems)*int64(hierarchydeletion.HierarchyDeletionLargeRecordBytes) {
		return errs.New(errs.KindValidationFailed, "removal membership exceeds the captured record limit")
	}
	prefix := MembershipCapturePrefix(operation.OperationID)
	for offset := 0; offset < len(raw); offset += membershipChunkBytes {
		key := prefix + "chunks/" + strconv.Itoa(offset/membershipChunkBytes)
		if err := repository.saveMembershipPart(ctx, operation, key, raw[offset:min(offset+membershipChunkBytes, len(raw))]); err != nil {
			return err
		}
	}
	digest := sha256.Sum256(raw)
	root, err := recordcodec.Encode("hierarchy-membership-capture", membershipCapture{
		OperationID: operation.OperationID, Revision: operation.SnapshotRevision, Bytes: len(raw), SHA256: hex.EncodeToString(digest[:]),
	})
	if err != nil {
		return err
	}
	defer clear(root)
	return repository.saveMembershipPart(ctx, operation, prefix+"root", root)
}

func (repository *Planner) saveMembershipPart(ctx context.Context, operation hierarchydeletion.HierarchyDeletionTombstone, key string, raw []byte) error {
	rootKey := MembershipCapturePrefix(operation.OperationID) + "root"
	fenceKey := hierarchydeletion.HierarchyDeletionTombstoneKey(string(operation.TargetKind), operation.TargetID)
	keys := []string{key, fenceKey}
	if key != rootKey {
		keys = append(keys, rootKey)
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != len(keys) || read.Values[1] == nil {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	if value := read.Values[0]; value != nil {
		if !bytes.Equal(value.Value, raw) {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
		return nil
	}
	rootPresent := key != rootKey && read.Values[2] != nil
	var current hierarchydeletion.HierarchyDeletionTombstone
	if hierarchydeletion.DecodeHierarchyDeletionRecord(read.Values[1].Value, hierarchydeletion.HierarchyDeletionLargeRecordBytes, &current) != nil ||
		current.OperationID != operation.OperationID || current.SnapshotRevision != operation.SnapshotRevision ||
		current.Phase != hierarchydeletion.HierarchyDeletionPlanning || rootPresent {
		return errs.New(errs.KindStateConflict, "removal membership publication lost its planning fence")
	}
	conditions := []keyvalue.Condition{{Key: key}, {Key: fenceKey, ModRevision: read.Values[1].ModRevision}}
	if key != rootKey {
		conditions = append(conditions, keyvalue.Condition{Key: rootKey})
	}
	result, err := repository.store.Transact(ctx, conditions, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "removal membership changed during capture")
	}
	return nil
}
