package backup

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backupOrphanReconcileTimeout = 30 * time.Second

type BackupOrphanExactStore interface {
	HeadExact(context.Context, backupobject.Artifact, *backupobject.Discriminator) (backupobject.HeadResult, error)
	PruneExact(context.Context, backupobject.PruneAuthority) error
}

// BackupOrphanStoreUse keeps credentials inside the Connector/Secret value
// owner. A store exists only for this call; the coordinator retains no keys.
type BackupOrphanStoreUse interface {
	WithBackupOrphanStore(context.Context, backupruntime.BackupRecoveryPointTargetSnapshot,
		func(BackupOrphanExactStore) error) error
}

// BackupOrphanReconciliationService drains one retained orphan per scheduler
// tick. Unknown uploads require exact metadata and cleanup proof before a
// selected discriminator can become independently retained ownership.
type BackupOrphanReconciliationService struct {
	runtime *etcd.BackupRuntimeRepository
	stores  BackupOrphanStoreUse
	now     func() time.Time
}

func NewBackupOrphanReconciliationService(runtime *etcd.BackupRuntimeRepository,
	stores BackupOrphanStoreUse,
) (*BackupOrphanReconciliationService, error) {
	if runtime == nil || stores == nil {
		return nil, errs.New(errs.KindInternal, "backup orphan reconciliation dependencies are required")
	}
	return &BackupOrphanReconciliationService{runtime: runtime, stores: stores, now: time.Now}, nil
}

// Tick returns the last inspected Point ID. An empty cursor wraps on the next
// tick. The caller owns scheduling; no credential or remote operation outlives
// this bounded call.
func (service *BackupOrphanReconciliationService) Tick(ctx context.Context, afterID string) (string, bool, error) {
	if service == nil || service.runtime == nil || service.stores == nil {
		return afterID, false, errs.New(errs.KindInternal, "backup orphan reconciliation is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, backupOrphanReconcileTimeout)
	defer cancel()
	current, next, found, err := service.runtime.NextReconciledBackupOrphan(ctx, afterID)
	if err != nil || !found {
		return next, false, err
	}
	if current.Record.CleanupProof == (backupruntime.BackupOrphanCleanupProof{}) {
		proved, ready, err := service.runtime.CaptureBackupOrphanCleanupProof(
			ctx, current, service.now().UTC().Truncate(time.Millisecond))
		if err != nil || !ready {
			return next, false, err
		}
		current = proved
	}
	identity := current.Record.Object
	if identity == (backupruntime.BackupObjectIdentity{}) &&
		current.Record.Upload.Kind == backupruntime.BackupUploadReturned {
		identity = current.Record.Upload.ReturnedObject
	}
	if identity == (backupruntime.BackupObjectIdentity{}) {
		if current.Record.Upload.Kind != backupruntime.BackupUploadUnknown ||
			current.Record.State != backupruntime.BackupOrphanInspect {
			return next, false, nil
		}
		artifact, err := backupOrphanArtifact(current.Record)
		if err != nil {
			return next, false, err
		}
		resolved := false
		err = service.stores.WithBackupOrphanStore(
			ctx,
			current.Record.Target,
			func(store BackupOrphanExactStore) error {
				// The unknown Put has no returned discriminator. Full metadata and
				// evidence are checked by this single HeadExact; absence cannot
				// prove that an uncertain upload will never finish, so it is held.
				head, err := store.HeadExact(ctx, artifact, nil)
				if err != nil || !head.Present {
					return err
				}
				selected := backupruntime.BackupObjectIdentity{Target: current.Record.Target.ObjectTarget(),
					Discriminator: backupruntime.BackupObjectDiscriminator{
						Kind: head.Object.Discriminator.Kind, Value: head.Object.Discriminator.Value}}
				_, err = service.runtime.ResolveReconciledBackupOrphanUpload(
					ctx, current, selected, service.now().UTC().Truncate(time.Millisecond))
				resolved = err == nil
				return err
			},
		)
		return next, resolved, err
	}
	artifact, authority, err := backupOrphanObjectAuthority(current.Record, identity)
	if err != nil {
		return next, false, err
	}
	err = service.stores.WithBackupOrphanStore(ctx, current.Record.Target, func(store BackupOrphanExactStore) error {
		if current.Record.State == backupruntime.BackupOrphanInspect {
			// Exact HEAD verifies the selected immutable object and metadata. A
			// missing exact version may advance to durable delete intent as well.
			head, err := store.HeadExact(ctx, artifact, &authority.Discriminator)
			if err != nil {
				return err
			}
			if head.Present {
				at := service.now().UTC().Truncate(time.Millisecond)
				if at.Before(current.Record.Target.CreatedAt) {
					at = current.Record.Target.CreatedAt
				}
				point := backupruntime.BackupRecoveryPointRecord{
					BackupRecoveryPointSnapshot: backupruntime.BackupRecoveryPointSnapshot{
						BackupRecoveryPointTargetSnapshot: current.Record.Target,
						Evidence:                          current.Record.Evidence, Object: identity, Postgres: current.Record.Postgres,
						ConfigArchive: current.Record.ConfigArchive, VolumeArchive: current.Record.VolumeArchive,
					}, VerifiedAt: at,
				}
				sweep := backupruntime.BackupRetentionSweepRecord{
					SourceID: point.SourceID, TriggerRecoveryPointID: point.ID,
					Keep:         current.Record.Reconciliation.RetentionKeep,
					Revision:     current.Record.Reconciliation.PolicyRevision,
					PolicySHA256: current.Record.Reconciliation.PolicySHA256,
					State:        backupruntime.BackupRetentionPending, CreatedAt: at, UpdatedAt: at,
				}
				_, err := service.runtime.AdoptReconciledBackupOrphan(ctx, current, point, sweep)
				return err
			}
			updated := current.Record
			updated.State = backupruntime.BackupOrphanDelete
			updated.UpdatedAt = service.now().UTC().Truncate(time.Millisecond)
			if !updated.UpdatedAt.After(current.Record.UpdatedAt) {
				updated.UpdatedAt = current.Record.UpdatedAt.Add(time.Millisecond)
			}
			_, err = service.runtime.TransitionReconciledBackupOrphan(ctx, current, updated)
			return err
		}
		if current.Record.State != backupruntime.BackupOrphanDelete {
			return errs.New(errs.KindStateConflict, "backup orphan state is not reconcilable")
		}
		if err := store.PruneExact(ctx, authority); err != nil {
			return err
		}
		head, err := store.HeadExact(ctx, artifact, &authority.Discriminator)
		if err != nil {
			return err
		}
		if head.Present {
			return errs.New(errs.KindStateConflict, "backup orphan exact object is still present")
		}
		return service.runtime.DeleteReconciledBackupOrphan(ctx, current)
	})
	if err != nil {
		return next, false, err
	}
	return next, true, nil
}

func backupOrphanObjectAuthority(orphan backupruntime.BackupOrphanRecord,
	identity backupruntime.BackupObjectIdentity,
) (backupobject.Artifact, backupobject.PruneAuthority, error) {
	target := orphan.Target
	if identity.Target != target.ObjectTarget() ||
		orphan.Upload.Kind == backupruntime.BackupUploadReturned && identity != orphan.Upload.ReturnedObject ||
		orphan.Object != (backupruntime.BackupObjectIdentity{}) && identity != orphan.Object {
		return backupobject.Artifact{}, backupobject.PruneAuthority{},
			errs.New(errs.KindStateConflict, "backup orphan selected object changed")
	}
	artifact, err := backupOrphanArtifact(orphan)
	if err != nil {
		return backupobject.Artifact{}, backupobject.PruneAuthority{}, err
	}
	discriminator := backupobject.Discriminator{Kind: identity.Discriminator.Kind, Value: identity.Discriminator.Value}
	count, digest := artifact.MetadataEvidence()
	authority := backupobject.PruneAuthority{Key: artifact.Key, EnvironmentID: artifact.EnvironmentID,
		SourceID: artifact.SourceID, RecoveryPointID: artifact.RecoveryPointID,
		Evidence: artifact.Evidence, Discriminator: discriminator, MetadataCount: count, MetadataSHA256: digest}
	if err := authority.Validate(target.ConnectorPrefix); err != nil {
		return backupobject.Artifact{}, backupobject.PruneAuthority{}, err
	}
	return artifact, authority, nil
}

func backupOrphanArtifact(orphan backupruntime.BackupOrphanRecord) (backupobject.Artifact, error) {
	target := orphan.Target
	sourceDigest, err := hex.DecodeString(orphan.Evidence.SourceSHA256)
	if err != nil || len(sourceDigest) != 32 {
		return backupobject.Artifact{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	storedDigest, err := hex.DecodeString(orphan.Evidence.StoredSHA256)
	if err != nil || len(storedDigest) != 32 {
		return backupobject.Artifact{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	artifact := backupobject.Artifact{Key: target.ObjectKey, EnvironmentID: target.EnvironmentID,
		SourceID: target.SourceID, RecoveryPointID: target.ID,
		SourceFormat: backupobject.SourceFormat(
			target.SourceFormat,
		), Encryption: backupobject.Encryption(target.Encryption),
		Evidence: backupobject.Evidence{SourceSizeBytes: orphan.Evidence.SourceSizeBytes,
			StoredSizeBytes: orphan.Evidence.StoredSizeBytes}}
	copy(artifact.Evidence.SourceSHA256[:], sourceDigest)
	copy(artifact.Evidence.StoredSHA256[:], storedDigest)
	if target.Encryption == backupruntime.BackupRuntimeEncryptionAge {
		era := uint64(target.KeyEra)
		artifact.KeyEra = &era
	}
	if err := artifact.Validate(); err != nil || artifact.ExpectedKey(target.ConnectorPrefix) != target.ObjectKey {
		return backupobject.Artifact{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	return artifact, nil
}
