package hierarchydeletionbackupcleanup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AuthorizeDelete records the single remote-delete grant for this attempt before
// contacting the provider. Reconnection may inspect absence but cannot obtain a
// second grant. Only an explicitly admitted new attempt can retry the deletion.
func (repository *Repository) AuthorizeDelete(ctx context.Context, execution Execution, identity backupruntime.BackupObjectIdentity) error {
	if repository == nil || repository.store == nil {
		return errs.New(errs.KindInternal, "backup cleanup storage is unavailable")
	}
	conditions, err := execution.conditions(true)
	if err != nil {
		return err
	}
	local, err := repository.readLocalAuthority(ctx, execution.action)
	if err != nil {
		return err
	}
	defer clear(local.primary.Value)
	if err := verifyRemoteTemplate(execution.action, local.fixedDigest); err != nil {
		return err
	}
	if err := applySelected(&local.remote, identity); err != nil {
		return err
	}
	key, err := cleanupIntentKey(execution.action.ParentOperationID, execution.action.Ordinal)
	if err != nil {
		return err
	}
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if read == nil || read.Entry == nil {
		return errs.New(errs.KindStateConflict, "backup deletion has no retained object selection")
	}
	defer clear(read.Entry.Value)
	intent, err := decodeCleanupIntent(read.Entry.Value)
	if err != nil || !intentMatches(intent, execution.operation, execution.action, local.fixedDigest) {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	if intent.Selected != identity || intent.ConfirmedAbsent != (backupruntime.BackupObjectIdentity{}) ||
		intent.DeleteAttempt == execution.view.Attempt.Attempt.ID {
		return errs.New(errs.KindStateConflict, "backup deletion already started or its selected object changed")
	}
	intent.DeleteAttempt = execution.view.Attempt.Attempt.ID
	value, err := encodeCleanupIntent(intent)
	if err != nil {
		return err
	}
	defer clear(value)
	conditions = append(conditions, local.conditions...)
	conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Entry.ModRevision})
	result, err := repository.store.Transact(ctx, conditions, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "backup deletion attempt changed before its remote grant")
	}
	return nil
}
