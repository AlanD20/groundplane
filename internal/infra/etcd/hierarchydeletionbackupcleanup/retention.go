package hierarchydeletionbackupcleanup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRetention joins the caller's successful Task collection transaction.
// Resource records are already gone; the retained selection and absence proof
// must still match the immutable action before its last evidence is retired.
func (repository *Repository) PrepareRetention(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction, revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if repository == nil || repository.store == nil || revision <= 0 ||
		operation.Tombstone.Phase != hierarchydeletion.HierarchyDeletionRetained ||
		operation.Fence.Dispatch != hierarchydeletion.HierarchyDeletionDispatchClosed ||
		action.ControllerProcedure == nil || action.ParentOperationID != operation.Tombstone.OperationID {
		return nil, nil, errs.New(errs.KindStateConflict, "backup cleanup proof is still required")
	}
	if action.ActionKind != hierarchydeletion.HierarchyDeletionRecoveryPointRemove &&
		action.ActionKind != hierarchydeletion.HierarchyDeletionOrphanObjectRemove {
		return nil, nil, errs.New(errs.KindValidationFailed, "backup cleanup action is unsupported")
	}
	key, err := cleanupIntentKey(action.ParentOperationID, action.Ordinal)
	if err != nil {
		return nil, nil, err
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	intent, err := decodeCleanupIntent(read.Values[0].Value)
	if err != nil || !intentMatches(intent, operation, action, intent.FixedInputDigest) ||
		intent.Selected == (backupruntime.BackupObjectIdentity{}) || intent.ConfirmedAbsent != intent.Selected {
		return nil, nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if err := verifyRemoteTemplate(action, intent.FixedInputDigest); err != nil {
		return nil, nil, err
	}
	conditions, err := operationConditions(operation)
	if err != nil {
		return nil, nil, err
	}
	conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	return conditions, []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}}, nil
}
