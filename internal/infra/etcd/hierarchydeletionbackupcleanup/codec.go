package hierarchydeletionbackupcleanup

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

const maximumCleanupIntentBytes = 4 << 10

func encodeCleanupIntent(record cleanupIntent) ([]byte, error) {
	if err := validateCleanupIntent(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("hierarchy-deletion-backup-cleanup", record)
}

func decodeCleanupIntent(value []byte) (cleanupIntent, error) {
	record, err := recordcodec.Decode[cleanupIntent](value, "hierarchy-deletion-backup-cleanup")
	if err != nil || len(value) > maximumCleanupIntentBytes || validateCleanupIntent(record) != nil {
		return cleanupIntent{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return record, nil
}

func validateCleanupIntent(record cleanupIntent) error {
	if record.Schema != 1 || !hierarchydeletion.ValidHierarchyDeletionPrivateID(record.ParentOperationID, "del") ||
		record.DeletionEpoch <= 0 || record.Ordinal < 0 || record.TargetID == "" || record.TargetRevision <= 0 ||
		!hierarchydeletion.ValidHierarchyDeletionDigest(record.FixedInputDigest) ||
		(record.ActionKind != hierarchydeletion.HierarchyDeletionRecoveryPointRemove &&
			record.ActionKind != hierarchydeletion.HierarchyDeletionOrphanObjectRemove) ||
		record.ConfirmedAbsent != record.Selected &&
			record.ConfirmedAbsent != (backupruntime.BackupObjectIdentity{}) {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func intentMatches(intent cleanupIntent, operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction, digest string,
) bool {
	return intent.ParentOperationID == action.ParentOperationID &&
		intent.DeletionEpoch == operation.Tombstone.DeletionEpoch && intent.Ordinal == action.Ordinal &&
		intent.ActionKind == action.ActionKind && intent.TargetID == action.TargetID &&
		intent.TargetRevision == action.TargetRevision && intent.FixedInputDigest == digest
}
