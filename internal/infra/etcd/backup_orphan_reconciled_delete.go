package etcd

import (
	"context"

	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumecleanup"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DeleteReconciledBackupOrphan consumes Controller-verified remote absence.
// For Volume artifacts it commits the independent manifest cleanup intent in
// the same native transaction as removal of the last orphan owner.
func (repository *BackupRuntimeRepository) DeleteReconciledBackupOrphan(
	ctx context.Context, current etcdstore.Versioned[backupruntime.BackupOrphanRecord],
) error {
	if current.Record.Target.SourceKind != backupruntime.BackupRuntimeSourceVolume {
		return repository.Writer.DeleteReconciledBackupOrphan(ctx, current)
	}
	if current.Revision <= 0 || current.Record.State != backupruntime.BackupOrphanDelete {
		return errs.New(errs.KindValidationFailed, "backup orphan deletion requires retained Delete authority")
	}
	connectorIndex, err := backupruntime.BackupOrphanConnectorIndexKey(
		current.Record.Target.ConnectorID,
		current.Record.Target.ID,
	)
	if err != nil {
		return err
	}
	environmentIndex, err := backupruntime.BackupOrphanEnvironmentIndexKey(
		current.Record.Target.EnvironmentID,
		current.Record.Target.ID,
	)
	if err != nil {
		return err
	}
	keys := []string{backupruntime.BackupOrphanKey(current.Record.Target.ID), connectorIndex, environmentIndex}
	anchor, err := repository.ReadCurrentKeys(ctx, keys)
	if err != nil {
		return err
	}
	defer etcdstore.ClearValues(anchor.Values)
	if anchor.Values[0] == nil && anchor.Values[1] == nil && anchor.Values[2] == nil {
		return nil
	}
	if anchor.Values[0] == nil || anchor.Values[0].ModRevision != current.Revision {
		return errs.New(errs.KindStateConflict, "backup orphan deletion authority changed")
	}
	if err := backupruntime.ValidateBackupOrphanCompanionEvidence(anchor.Values, current.Record); err != nil {
		return err
	}
	point := backupruntime.BackupRecoveryPointSnapshot{
		BackupRecoveryPointTargetSnapshot: current.Record.Target,
		Evidence:                          current.Record.Evidence, Object: current.Record.Object,
		VolumeArchive: current.Record.VolumeArchive,
	}
	cleanupConditions, cleanupMutations, err := backupvolumecleanup.Prepare(
		ctx, repository.store, point, anchor.ReadRevision,
	)
	if err != nil {
		return err
	}
	defer etcdstore.ClearMutationValues(cleanupMutations)
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: current.Revision},
		{Key: keys[1], ModRevision: current.Revision},
		{Key: keys[2], ModRevision: current.Revision},
	}
	for _, condition := range cleanupConditions {
		if condition.Key == keys[0] {
			continue
		}
		conditions = append(conditions, condition)
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: keys[0]},
		{Type: etcdstore.MutationDelete, Key: keys[1]},
		{Type: etcdstore.MutationDelete, Key: keys[2]},
	}
	mutations = append(mutations, cleanupMutations...)
	binding, err := repository.Writer.BindBackupOrphanMutation(
		ctx, current.Record.Target.EnvironmentID, anchor.ReadRevision, conditions, mutations,
	)
	if err != nil {
		return err
	}
	defer binding.Clear()
	result, err := repository.TransactRuntime(ctx, binding.Conditions(), binding.Mutations())
	if err != nil {
		return err
	}
	if !result.Succeeded {
		etcdstore.ClearValues(result.FailureReads)
		return errs.New(errs.KindStateConflict, "backup orphan deletion authority changed")
	}
	return nil
}
