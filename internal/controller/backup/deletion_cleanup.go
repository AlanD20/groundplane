package backup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
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

func (service *DeletionCleanup) Execute(ctx context.Context, execution hierarchydeletionbackupcleanup.Execution, reporter *RemovalReporter) (executionErr error) {
	if reporter == nil {
		return errs.New(errs.KindInternal, "backup removal requires its diagnostic reporter")
	}
	deleteGranted := false
	defer func() {
		if executionErr != nil {
			reporter.failed(ctx, deleteGranted)
		}
	}()
	ctx, cancel := context.WithDeadline(ctx, execution.Deadline())
	defer cancel()
	reporter.start(ctx, "prepare_object", "Prepare captured backup object", "Load the retained object identity and scoped Connector credentials without selecting newer configuration.")
	remote, err := service.repository.PrepareRemote(ctx, execution)
	if err != nil {
		return err
	}
	if remote.ConfirmedAbsent {
		reporter.finish(ctx, "Exact object absence was already recorded; no new delete request is needed.")
		return nil
	}
	point := remote.Point
	identity := point.Object
	return service.stores.WithBackupOrphanStore(ctx, point.BackupRecoveryPointTargetSnapshot,
		func(store BackupOrphanExactStore) error {
			if identity == (backupruntime.BackupObjectIdentity{}) {
				if execution.IsRecovery() {
					return errs.New(errs.KindStateConflict, "backup cleanup requires its previously selected object")
				}
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
				if err := service.repository.SelectObject(ctx, execution, identity); err != nil {
					return err
				}
			}
			artifact, authority, err := backupObjectAuthority(point, identity)
			if err != nil {
				return err
			}
			if execution.IsRecovery() {
				head, err := store.HeadExact(ctx, artifact, &authority.Discriminator)
				if err != nil {
					return err
				}
				if !head.Present {
					if err := service.repository.ConfirmAbsent(ctx, execution, identity); err != nil {
						return err
					}
					reporter.finish(ctx, "Storage confirms that the previously selected object is absent.")
					return nil
				}
			}
			reporter.finish(ctx, "The captured object identity and Connector credentials are ready.")
			reporter.start(ctx, "delete_object", "Delete captured backup object", "Authorize one conditional deletion of the selected version or ETag; do not delete another object at the same key.")
			if err := service.repository.AuthorizeDelete(ctx, execution, identity); err != nil {
				return err
			}
			deleteGranted = true
			if err := store.PruneExact(ctx, authority); err != nil {
				return err
			}
			reporter.finish(ctx, "Storage's conditional removal operation confirmed that the selected object is absent.")
			reporter.start(ctx, "confirm_absence", "Record confirmed object absence", "Confirm exact object absence and retain the proof before retiring recovery-point metadata.")
			head, err := store.HeadExact(ctx, artifact, &authority.Discriminator)
			if err != nil {
				return err
			}
			if head.Present {
				return errs.New(errs.KindStateConflict, "backup deletion exact object is still present")
			}
			if err := service.repository.ConfirmAbsent(ctx, execution, identity); err != nil {
				return err
			}
			reporter.finish(ctx, "Exact object absence is saved. Metadata retirement follows in the atomic Task settlement.")
			return nil
		})
}

// Reconcile only observes the captured provider identity. A saved Start can
// precede an interrupted delete request; it never authorizes command replay.
func (service *DeletionCleanup) Reconcile(ctx context.Context, execution hierarchydeletionbackupcleanup.Execution) error {
	remote, err := service.repository.PrepareRemote(ctx, execution)
	if err != nil {
		return err
	}
	if remote.ConfirmedAbsent {
		return nil
	}
	if remote.Point.Object == (backupruntime.BackupObjectIdentity{}) {
		return errs.New(errs.KindStateConflict, "backup deletion was interrupted before an exact object was selected")
	}
	artifact, authority, err := backupObjectAuthority(remote.Point, remote.Point.Object)
	if err != nil {
		return err
	}
	return service.stores.WithBackupOrphanStore(ctx, remote.Point.BackupRecoveryPointTargetSnapshot,
		func(store BackupOrphanExactStore) error {
			head, err := store.HeadExact(ctx, artifact, &authority.Discriminator)
			if err != nil {
				return err
			}
			if head.Present {
				return errs.New(errs.KindStateConflict, "backup object remains after interrupted deletion")
			}
			return service.repository.ConfirmAbsent(ctx, execution, remote.Point.Object)
		})
}
