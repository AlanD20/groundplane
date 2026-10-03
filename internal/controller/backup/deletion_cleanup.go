package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionbackupcleanup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DeletionCleanup removes only objects sealed into a hierarchy deletion plan.
// Storage records the selected identity before deletion and the exact absence
// before retiring local ownership; Connector credentials remain scoped to use.
type DeletionCleanup struct {
	repository *hierarchydeletionbackupcleanup.Repository
	stores     BackupOrphanStoreUse
}

func NewDeletionCleanup(repository *hierarchydeletionbackupcleanup.Repository,
	stores BackupOrphanStoreUse,
) (*DeletionCleanup, error) {
	if repository == nil || stores == nil {
		return nil, errs.New(errs.KindInternal, "backup deletion cleanup dependencies are required")
	}
	return &DeletionCleanup{repository: repository, stores: stores}, nil
}

func (service *DeletionCleanup) Execute(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) error {
	if action.ActionKind != hierarchydeletion.HierarchyDeletionRecoveryPointRemove &&
		action.ActionKind != hierarchydeletion.HierarchyDeletionOrphanObjectRemove {
		return errs.New(errs.KindInternal, "backup deletion cleanup action is unsupported")
	}
	deadline := time.Now().Add(30 * time.Second)
	if operation.Tombstone.AttemptDeadline.Before(deadline) {
		deadline = operation.Tombstone.AttemptDeadline
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	remote, err := service.repository.PrepareRemote(ctx, operation, action)
	if err != nil {
		return err
	}
	point := remote.Point
	identity := point.Object
	return service.stores.WithBackupOrphanStore(ctx, point.BackupRecoveryPointTargetSnapshot,
		func(store BackupOrphanExactStore) error {
			if identity == (backupruntime.BackupObjectIdentity{}) {
				artifact, err := backupObjectArtifact(point)
				if err != nil {
					return err
				}
				head, err := store.HeadExact(ctx, artifact, nil)
				if err != nil {
					return err
				}
				if !head.Present {
					return errs.New(errs.KindStateConflict, "uncertain backup upload has no exact object cleanup proof")
				}
				identity = backupruntime.BackupObjectIdentity{Target: point.ObjectTarget(),
					Discriminator: backupruntime.BackupObjectDiscriminator{
						Kind: head.Object.Discriminator.Kind, Value: head.Object.Discriminator.Value}}
				if err := service.repository.SelectObject(ctx, operation, action, identity); err != nil {
					return err
				}
			}
			artifact, authority, err := backupObjectAuthority(point, identity)
			if err != nil {
				return err
			}
			if err := store.PruneExact(ctx, authority); err != nil {
				return err
			}
			head, err := store.HeadExact(ctx, artifact, &authority.Discriminator)
			if err != nil {
				return err
			}
			if head.Present {
				return errs.New(errs.KindStateConflict, "backup deletion exact object is still present")
			}
			return service.repository.ConfirmAbsent(ctx, operation, action, identity)
		})
}
