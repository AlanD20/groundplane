package backupruntime

import (
	"context"
	"time"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveReconciledBackupOrphanUpload persists the discriminator measured by
// a complete-metadata HeadExact after an uncertain Put. It cannot replace a
// discriminator returned by Put or select one twice.
func (repository *Writer) ResolveReconciledBackupOrphanUpload(ctx context.Context,
	current etcdstore.Versioned[BackupOrphanRecord], object BackupObjectIdentity, at time.Time,
) (etcdstore.Versioned[BackupOrphanRecord], error) {
	if current.Revision <= 0 || current.Record.State != BackupOrphanInspect ||
		current.Record.CleanupProof == (BackupOrphanCleanupProof{}) ||
		current.Record.Upload.Kind != BackupUploadUnknown ||
		current.Record.Object != (BackupObjectIdentity{}) || current.Record.Phase != BackupSourcePhaseHeadVerification ||
		!validBackupObjectIdentity(object) || object.Target != current.Record.Target.ObjectTarget() ||
		!ValidBackupRuntimeInstant(at) {
		return etcdstore.Versioned[BackupOrphanRecord]{},
			errs.New(errs.KindValidationFailed, "backup orphan unknown upload resolution is invalid")
	}
	next := current.Record
	next.Object = object
	next.Phase = BackupSourcePhasePointCommit
	next.UnknownResolvedByReconciler = true
	next.UpdatedAt = at
	if !next.UpdatedAt.After(current.Record.UpdatedAt) {
		next.UpdatedAt = current.Record.UpdatedAt.Add(time.Millisecond)
	}
	value, err := EncodeBackupOrphanRecord(next)
	if err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, err
	}
	defer clear(value)
	connectorIndex, err := BackupOrphanConnectorIndexKey(next.Target.ConnectorID, next.Target.ID)
	if err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, err
	}
	environmentIndex, err := BackupOrphanEnvironmentIndexKey(next.Target.EnvironmentID, next.Target.ID)
	if err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, err
	}
	keys := []string{BackupOrphanKey(next.Target.ID), connectorIndex, environmentIndex}
	anchor, err := repository.ReadCurrentKeys(ctx, keys)
	if err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != current.Revision {
		return etcdstore.Versioned[BackupOrphanRecord]{},
			errs.New(errs.KindStateConflict, "backup orphan upload authority changed")
	}
	if err := ValidateBackupOrphanCompanionEvidence(anchor.Values, current.Record); err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: current.Revision},
		{Key: keys[1], ModRevision: current.Revision},
		{Key: keys[2], ModRevision: current.Revision},
	}
	result, err := repository.TransactRuntime(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: keys[0], Value: value},
		{Type: etcdstore.MutationPut, Key: keys[1], Value: []byte(next.Target.ID)},
		{Type: etcdstore.MutationPut, Key: keys[2], Value: []byte(next.Target.ID)},
	})
	if err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, err
	}
	if !result.Succeeded {
		etcdstore.ClearValues(result.FailureReads)
		return etcdstore.Versioned[BackupOrphanRecord]{},
			errs.New(errs.KindStateConflict, "backup orphan upload authority changed")
	}
	return etcdstore.Versioned[BackupOrphanRecord]{Record: next,
		Revision: result.Revision, ReadRevision: result.Revision}, nil
}
