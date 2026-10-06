package hierarchydeletionbackupcleanup

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type cleanupStore interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

// RemoteAuthority is the complete frozen remote object authority for one
// hierarchy-deletion action. Point.Object is populated whenever an immutable
// provider identity was already retained or was durably selected.
type RemoteAuthority struct {
	Point  backupruntime.BackupRecoveryPointSnapshot
	Orphan *backupruntime.BackupOrphanRecord
}

type Repository struct{ store cleanupStore }

func NewRepository(store cleanupStore) *Repository { return &Repository{store: store} }

type cleanupIntent struct {
	Schema            int                                           `json:"schema"`
	ParentOperationID string                                        `json:"parent_operation_id"`
	DeletionEpoch     int64                                         `json:"deletion_epoch"`
	Ordinal           int64                                         `json:"ordinal"`
	ActionKind        hierarchydeletion.HierarchyDeletionActionKind `json:"action_kind"`
	TargetID          string                                        `json:"target_id"`
	TargetRevision    int64                                         `json:"target_revision"`
	FixedInputDigest  string                                        `json:"fixed_input_digest"`
	Selected          backupruntime.BackupObjectIdentity            `json:"selected,omitempty"`
	ConfirmedAbsent   backupruntime.BackupObjectIdentity            `json:"confirmed_absent,omitempty"`
}

type localAuthority struct {
	remote       RemoteAuthority
	primary      *keyvalue.KeyValue
	readRevision int64
	conditions   []keyvalue.Condition
	fixedDigest  string
}

func cleanupIntentKey(operationID string, ordinal int64) (string, error) {
	prefix, err := hierarchydeletion.HierarchyDeletionProgressPrefix(operationID)
	if err != nil {
		return "", err
	}
	return prefix + fmt.Sprintf("backup-cleanup-%020d", ordinal), nil
}

func validateActive(operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) error {
	if action.ParentOperationID != operation.Tombstone.OperationID ||
		action.Ordinal != operation.Tombstone.Checkpoint.NextOrdinal ||
		action.ProcedureKind != hierarchydeletion.HierarchyDeletionProcedureController ||
		action.ControllerProcedure == nil || action.ControllerProcedure.FixedInputRevision != action.TargetRevision ||
		operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionExecuting ||
		operation.Fence.Phase != hierarchydeletion.HierarchyDeletionExecuting ||
		operation.Fence.ActiveActionOrdinal == nil || *operation.Fence.ActiveActionOrdinal != action.Ordinal ||
		operation.Fence.ParentOperationID != action.ParentOperationID ||
		operation.Fence.DeletionEpoch != operation.Tombstone.DeletionEpoch ||
		operation.TombstoneRevision <= 0 || operation.FenceRevision <= 0 {
		return errs.New(errs.KindStateConflict, "hierarchy deletion backup cleanup action is not active")
	}
	switch action.ActionKind {
	case hierarchydeletion.HierarchyDeletionRecoveryPointRemove,
		hierarchydeletion.HierarchyDeletionOrphanObjectRemove:
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "hierarchy deletion backup cleanup action is unsupported")
	}
}

func operationConditions(operation hierarchydeletion.HierarchyDeletionOperation) ([]keyvalue.Condition, error) {
	fenceKey, err := hierarchydeletion.HierarchyDeletionCleanupFenceKey(operation.Tombstone.OperationID)
	if err != nil {
		return nil, err
	}
	return []keyvalue.Condition{
		{
			Key: hierarchydeletion.HierarchyDeletionTombstoneKey(
				string(operation.Tombstone.TargetKind),
				operation.Tombstone.TargetID,
			),
			ModRevision: operation.TombstoneRevision,
		},
		{Key: fenceKey, ModRevision: operation.FenceRevision},
	}, nil
}

func (repository *Repository) readLocalAuthority(ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (localAuthority, error) {
	switch action.ActionKind {
	case hierarchydeletion.HierarchyDeletionRecoveryPointRemove:
		return repository.readRecoveryPointAuthority(ctx, action)
	case hierarchydeletion.HierarchyDeletionOrphanObjectRemove:
		return repository.readOrphanAuthority(ctx, action)
	default:
		return localAuthority{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion backup cleanup action is unsupported",
		)
	}
}

func (repository *Repository) readRecoveryPointAuthority(ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (localAuthority, error) {
	primaryKey := backupruntime.BackupRecoveryPointKey(action.TargetID)
	first, err := repository.store.Get(ctx, primaryKey)
	if err != nil {
		return localAuthority{}, err
	}
	if first == nil || first.Entry == nil || first.Entry.ModRevision != action.TargetRevision {
		return localAuthority{}, errs.New(errs.KindStateConflict, "hierarchy deletion recovery point changed")
	}
	record, err := backupruntime.DecodeBackupRecoveryPointRecord(first.Entry.Value)
	if err != nil || record.ID != action.TargetID {
		return localAuthority{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	keys, err := backupruntime.BackupPruneAuthorityKeys(record.BackupRecoveryPointSnapshot)
	if err != nil {
		return localAuthority{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	keys = append(keys, backupruntime.BackupRecoveryPointCaptureKey(record.ID))
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: first.ReadRevision})
	if err != nil {
		return localAuthority{}, err
	}
	if read == nil || read.ReadRevision != first.ReadRevision || len(read.Values) != 6 {
		return localAuthority{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil ||
		read.Values[1] == nil ||
		read.Values[2] == nil ||
		read.Values[3] == nil ||
		read.Values[4] == nil ||
		read.Values[1].ModRevision != action.TargetRevision ||
		read.Values[2].ModRevision != action.TargetRevision ||
		read.Values[3].ModRevision != action.TargetRevision ||
		read.Values[4].ModRevision != action.TargetRevision ||
		string(read.Values[2].Value) != action.TargetID ||
		string(read.Values[3].Value) != action.TargetID ||
		string(read.Values[4].Value) != action.TargetID {
		return localAuthority{}, errs.New(errs.KindStateConflict, "hierarchy deletion recovery point authority changed")
	}
	stored, err := backupruntime.DecodeBackupRecoveryPointRecord(read.Values[1].Value)
	if err != nil || stored != record {
		return localAuthority{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])}
	}
	return localAuthority{remote: RemoteAuthority{Point: record.BackupRecoveryPointSnapshot},
		primary: first.Entry, readRevision: first.ReadRevision, conditions: conditions,
		fixedDigest: hierarchydeletion.HierarchyDeletionBytesDigest(first.Entry.Value)}, nil
}

func (repository *Repository) readOrphanAuthority(ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (localAuthority, error) {
	primaryKey := backupruntime.BackupOrphanKey(action.TargetID)
	first, err := repository.store.Get(ctx, primaryKey)
	if err != nil {
		return localAuthority{}, err
	}
	if first == nil || first.Entry == nil || first.Entry.ModRevision != action.TargetRevision {
		return localAuthority{}, errs.New(errs.KindStateConflict, "hierarchy deletion backup orphan changed")
	}
	record, err := backupruntime.DecodeBackupOrphanRecord(first.Entry.Value)
	if err != nil || record.Target.ID != action.TargetID {
		return localAuthority{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if record.CleanupProof == (backupruntime.BackupOrphanCleanupProof{}) {
		return localAuthority{}, errs.New(errs.KindStateConflict, "backup orphan staging cleanup is not proved")
	}
	environmentKey, err := backupruntime.BackupOrphanEnvironmentIndexKey(record.Target.EnvironmentID, action.TargetID)
	if err != nil {
		return localAuthority{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	connectorKey, err := backupruntime.BackupOrphanConnectorIndexKey(record.Target.ConnectorID, action.TargetID)
	if err != nil {
		return localAuthority{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	keys := []string{primaryKey, environmentKey, connectorKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: first.ReadRevision})
	if err != nil {
		return localAuthority{}, err
	}
	if read == nil || read.ReadRevision != first.ReadRevision || len(read.Values) != len(keys) ||
		backupruntime.ValidateBackupOrphanCompanionEvidence(read.Values, record) != nil ||
		read.Values[0].ModRevision != action.TargetRevision {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return localAuthority{}, errs.New(errs.KindStateConflict, "hierarchy deletion backup orphan authority changed")
	}
	defer keyvalue.ClearValues(read.Values)
	point := backupruntime.BackupRecoveryPointSnapshot{BackupRecoveryPointTargetSnapshot: record.Target,
		Evidence: record.Evidence, Postgres: record.Postgres, ConfigArchive: record.ConfigArchive,
		VolumeArchive: record.VolumeArchive}
	if record.Object != (backupruntime.BackupObjectIdentity{}) {
		point.Object = record.Object
	} else if record.Upload.Kind == backupruntime.BackupUploadReturned {
		point.Object = record.Upload.ReturnedObject
	}
	if point.Object != (backupruntime.BackupObjectIdentity{}) &&
		backupruntime.ValidateBackupRecoveryPointSnapshot(point) != nil {
		return localAuthority{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	copyRecord := record
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision}
	}
	return localAuthority{remote: RemoteAuthority{Point: point, Orphan: &copyRecord},
		primary: first.Entry, readRevision: first.ReadRevision, conditions: conditions,
		fixedDigest: hierarchydeletion.HierarchyDeletionBytesDigest(first.Entry.Value)}, nil
}
