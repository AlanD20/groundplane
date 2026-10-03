package hierarchydeletionbackupcleanup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) PrepareRemote(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) (RemoteAuthority, error) {
	if repository == nil || repository.store == nil {
		return RemoteAuthority{}, errs.New(
			errs.KindInternal,
			"hierarchy deletion backup cleanup repository is unavailable",
		)
	}
	if err := validateActive(operation, action); err != nil {
		return RemoteAuthority{}, err
	}
	local, err := repository.readLocalAuthority(ctx, action)
	if err != nil {
		return RemoteAuthority{}, err
	}
	defer clear(local.primary.Value)
	if err := verifyRemoteTemplate(action, local.fixedDigest); err != nil {
		return RemoteAuthority{}, err
	}
	key, err := cleanupIntentKey(action.ParentOperationID, action.Ordinal)
	if err != nil {
		return RemoteAuthority{}, err
	}
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		return RemoteAuthority{}, err
	}
	if read == nil {
		return RemoteAuthority{}, errs.New(errs.KindInternal, "hierarchy deletion backup cleanup intent read is empty")
	}
	if read.Entry != nil {
		intent, decodeErr := decodeCleanupIntent(read.Entry.Value)
		if decodeErr != nil || !intentMatches(intent, operation, action, local.fixedDigest) {
			return RemoteAuthority{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		if intent.Selected != (backupruntime.BackupObjectIdentity{}) {
			if err := applySelected(&local.remote, intent.Selected); err != nil {
				return RemoteAuthority{}, err
			}
		}
		return local.remote, nil
	}
	selected := local.remote.Point.Object
	intent := cleanupIntent{Schema: 1, ParentOperationID: action.ParentOperationID,
		DeletionEpoch: operation.Tombstone.DeletionEpoch, Ordinal: action.Ordinal,
		ActionKind: action.ActionKind, TargetID: action.TargetID, TargetRevision: action.TargetRevision,
		FixedInputDigest: local.fixedDigest, Selected: selected}
	encoded, err := encodeCleanupIntent(intent)
	if err != nil {
		return RemoteAuthority{}, err
	}
	conditions, err := operationConditions(operation)
	if err != nil {
		clear(encoded)
		return RemoteAuthority{}, err
	}
	conditions = append(conditions, local.conditions...)
	conditions = append(conditions, keyvalue.Condition{Key: key})
	result, err := repository.store.Transact(ctx, conditions,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: encoded}})
	clear(encoded)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return RemoteAuthority{}, err
	}
	if !result.Succeeded {
		return RemoteAuthority{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion backup cleanup authority changed",
		)
	}
	return local.remote, nil
}

func verifyRemoteTemplate(action hierarchydeletion.HierarchyDeletionAction, fixedInputDigest string) error {
	expected, err := hierarchydeletionplanning.BindHierarchyDeletionControllerProcedure(
		hierarchydeletionplanning.HierarchyDeletionPlannedAction{
			NodeID: action.NodeID, Ordinal: action.Ordinal, ParentOperationID: action.ParentOperationID,
			ActionKind: action.ActionKind, TargetKind: action.TargetKind, TargetID: action.TargetID,
			TargetRevision: action.TargetRevision,
		},
		hierarchydeletionplanning.HierarchyDeletionControllerFinalizerInput{
			Finalizer: action.ControllerProcedure.Finalizer, TargetKind: action.TargetKind,
			TargetID: action.TargetID, FixedInputRevision: action.TargetRevision,
			FixedInputDigest: fixedInputDigest, BatchOrdinal: 0, BatchCount: 1,
		},
	)
	if err != nil || expected != *action.ControllerProcedure {
		return errs.New(errs.KindStateConflict, "hierarchy deletion backup cleanup template changed")
	}
	return nil
}

func (repository *Repository) SelectObject(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	identity backupruntime.BackupObjectIdentity,
) error {
	return repository.updateIntent(ctx, operation, action, identity, false)
}

func (repository *Repository) ConfirmAbsent(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	identity backupruntime.BackupObjectIdentity,
) error {
	return repository.updateIntent(ctx, operation, action, identity, true)
}

func (repository *Repository) updateIntent(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
	identity backupruntime.BackupObjectIdentity,
	confirm bool,
) error {
	if repository == nil || repository.store == nil {
		return errs.New(errs.KindInternal, "hierarchy deletion backup cleanup repository is unavailable")
	}
	if err := validateActive(operation, action); err != nil {
		return err
	}
	local, err := repository.readLocalAuthority(ctx, action)
	if err != nil {
		return err
	}
	defer clear(local.primary.Value)
	if err := applySelected(&local.remote, identity); err != nil {
		return err
	}
	key, err := cleanupIntentKey(action.ParentOperationID, action.Ordinal)
	if err != nil {
		return err
	}
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if read == nil || read.Entry == nil {
		return errs.New(errs.KindStateConflict, "hierarchy deletion backup cleanup intent is absent")
	}
	intent, err := decodeCleanupIntent(read.Entry.Value)
	if err != nil || !intentMatches(intent, operation, action, local.fixedDigest) {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	if intent.Selected != (backupruntime.BackupObjectIdentity{}) && intent.Selected != identity {
		return errs.New(errs.KindStateConflict, "hierarchy deletion backup selected object changed")
	}
	if intent.ConfirmedAbsent != (backupruntime.BackupObjectIdentity{}) && intent.ConfirmedAbsent != identity {
		return errs.New(errs.KindStateConflict, "hierarchy deletion backup absence proof changed")
	}
	if !confirm && intent.Selected == identity || confirm && intent.ConfirmedAbsent == identity {
		return nil
	}
	intent.Selected = identity
	if confirm {
		intent.ConfirmedAbsent = identity
	}
	encoded, err := encodeCleanupIntent(intent)
	if err != nil {
		return err
	}
	conditions, err := operationConditions(operation)
	if err != nil {
		clear(encoded)
		return err
	}
	conditions = append(conditions, local.conditions...)
	conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: read.Entry.ModRevision})
	result, err := repository.store.Transact(ctx, conditions,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: encoded}})
	clear(encoded)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "hierarchy deletion backup cleanup authority changed")
	}
	return nil
}

func applySelected(remote *RemoteAuthority, identity backupruntime.BackupObjectIdentity) error {
	if identity == (backupruntime.BackupObjectIdentity{}) || identity.Target != remote.Point.ObjectTarget() ||
		(remote.Point.Object != (backupruntime.BackupObjectIdentity{}) && remote.Point.Object != identity) {
		return errs.New(errs.KindStateConflict, "hierarchy deletion backup selected object changed")
	}
	remote.Point.Object = identity
	if backupruntime.ValidateBackupRecoveryPointSnapshot(remote.Point) != nil {
		return backupruntime.CorruptBackupRuntimeRecord()
	}
	return nil
}
