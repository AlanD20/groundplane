package etcd

import (
	"context"
	backupconfigrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupscheduling"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) prepareBackupRunPublication(
	ctx context.Context,
	record backupruntime.BackupRunRecord,
	lock backupruntime.BackupOperationLockRecord,
	fixedRevision int64,
) (backupRunPublicationPlan, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return backupRunPublicationPlan{}, err
	}
	if record.State != backupruntime.BackupRunQueued || fixedRevision <= 0 ||
		lock.EnvironmentID != record.EnvironmentID ||
		lock.OperationID != record.OperationID || lock.TaskID != record.TaskID ||
		lock.Kind != backupruntime.BackupOperationBackup || lock.CreatedAt != record.CreatedAt ||
		lock.UpdatedAt != record.CreatedAt {
		return backupRunPublicationPlan{}, errs.New(
			errs.KindValidationFailed,
			"backup run publication ownership is invalid",
		)
	}
	runValue, err := backupruntime.EncodeBackupRunRecord(record)
	if err != nil {
		return backupRunPublicationPlan{}, err
	}
	lockValue, err := backupruntime.EncodeBackupOperationLockRecord(lock)
	if err != nil {
		clear(runValue)
		return backupRunPublicationPlan{}, err
	}
	membershipKey, err := backupruntime.BackupRunEnvironmentIndexKey(record.EnvironmentID, record.TaskID)
	if err != nil {
		clear(runValue)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	exclusions, err := backupRunExclusionRecords(record, record.CreatedAt)
	if err != nil {
		clear(runValue)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	keys := []string{backupruntime.BackupRunKey(record.TaskID), membershipKey}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: keys[0], Value: runValue},
		{Type: etcdstore.MutationPut, Key: keys[1], Value: []byte(record.TaskID)},
	}
	for _, exclusion := range exclusions {
		key, keyErr := backupruntime.BackupSourceTargetExclusionKey(exclusion.TargetKind, exclusion.TargetID)
		if keyErr != nil {
			etcdstore.ClearMutationValues(mutations)
			clear(lockValue)
			return backupRunPublicationPlan{}, keyErr
		}
		value, encodeErr := backupruntime.EncodeBackupSourceTargetExclusionRecord(exclusion)
		if encodeErr != nil {
			etcdstore.ClearMutationValues(mutations)
			clear(lockValue)
			return backupRunPublicationPlan{}, encodeErr
		}
		keys = append(keys, key)
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: value})
	}
	anchor, err := repository.ReadFixedKeys(ctx, keys, fixedRevision)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	defer etcdstore.ClearValues(anchor.Values)
	connectorEvidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			connectorrecord.RecordKey(record.ConnectorID), connectorrecord.CredentialValueKey(record.ConnectorID),
		},
		Revision: fixedRevision,
	})
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	if connectorEvidence == nil || connectorEvidence.ReadRevision != anchor.ReadRevision ||
		len(connectorEvidence.Values) != 2 {
		etcdstore.ClearMutationValues(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, errs.New(
			errs.KindInternal,
			"backup connector publication evidence is incomplete",
		)
	}
	defer etcdstore.ClearValues(connectorEvidence.Values)
	if err := backupruntime.ValidateBackupConnectorSnapshotEvidence(
		connectorEvidence.Values,
		record,
	); err != nil {
		etcdstore.ClearMutationValues(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	snapshotConditions, snapshotMutations, err := repository.loadBackupRunPublicationEvidence(
		ctx,
		record,
		fixedRevision,
	)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	for index, value := range anchor.Values {
		if index == 1 {
			continue
		}
		if value != nil {
			etcdstore.ClearMutationValues(mutations)
			clear(lockValue)
			return backupRunPublicationPlan{}, errs.New(
				errs.KindStateConflict,
				"backup run publication authority already exists",
			)
		}
	}
	fence, err := environmentfence.LoadOrdinary(
		ctx,
		repository.store,
		record.EnvironmentID,
		anchor.ReadRevision,
	)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	backingEnvironmentIDs, err := backupruntime.DatabaseBackingEnvironmentIDs(record)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	backingGuards, err := postgresbackingguard.PrepareAcquisition(
		ctx,
		repository.store,
		backingEnvironmentIDs,
		record.EnvironmentID,
		postgresbackingguard.Owner(
			backupruntime.BackupOperationBackup,
			record.OperationID,
			record.TaskID,
		),
		record.CreatedAt,
		anchor.ReadRevision,
	)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}
	defer backingGuards.Clear()
	policyFence, err := repository.loadManualBackupPolicyFence(ctx, record, fixedRevision)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		clear(lockValue)
		return backupRunPublicationPlan{}, err
	}

	conditions := make([]etcdstore.Condition, 0, len(keys)+fence.ConditionCount()+len(policyFence))
	for index, key := range keys {
		if index == 1 {
			continue
		}
		conditions = append(conditions, etcdstore.Condition{Key: key})
	}
	conditions = append(conditions, backupRunExternalConditions(record, snapshotConditions)...)
	conditions = append(conditions, fence.TransactionConditions()...)
	conditions = append(conditions, backingGuards.Conditions...)
	conditions = append(conditions, policyFence...)

	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentOperationLockKey(record.EnvironmentID), Value: lockValue,
	})
	for _, mutation := range backingGuards.Mutations {
		copyOfMutation := mutation
		copyOfMutation.Value = append([]byte(nil), mutation.Value...)
		mutations = append(mutations, copyOfMutation)
	}
	mutations = append(mutations, snapshotMutations...)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return backupRunPublicationPlan{}, err
	}
	mutations = append(mutations, epoch)
	if record.Initiator == backupruntime.BackupRunInitiatorSchedule {
		scheduleConditions, scheduleMutations, scheduleErr := backupscheduling.New(repository.store).PreparePublication(
			ctx, record, fixedRevision,
		)
		if scheduleErr != nil {
			etcdstore.ClearMutationValues(mutations)
			return backupRunPublicationPlan{}, scheduleErr
		}
		conditions = append(conditions, scheduleConditions...)
		mutations = append(mutations, scheduleMutations...)
	}
	if err := backupruntime.ValidateBackupRuntimeTransactionBounds(conditions, mutations); err != nil {
		etcdstore.ClearMutationValues(mutations)
		return backupRunPublicationPlan{}, err
	}
	return backupRunPublicationPlan{
		conditions: conditions,
		mutations:  mutations,
		record:     record,
	}, nil
}

// backupRunExternalConditions retains CAS for records that can change outside
// an Environment mutation. The consumer and every PostgreSQL backing
// Environment use their exact mutation epochs and canonical operation locks.
func backupRunExternalConditions(
	run backupruntime.BackupRunRecord,
	conditions []etcdstore.Condition,
) []etcdstore.Condition {
	allowed := make(map[string]struct{}, len(run.Sources)*2+2)
	allowed[connectorrecord.RecordKey(run.ConnectorID)] = struct{}{}
	if run.ConnectorHasDirectCredentials {
		allowed[connectorrecord.CredentialValueKey(run.ConnectorID)] = struct{}{}
	}
	for _, source := range run.Sources {
		allowed[backuppolicy.BackupSourceKey(source.SourceID)] = struct{}{}
		if source.Snapshot.Volume != nil {
			allowed[projectionrecord.EnvironmentComposeProjectionStorageKey(source.Snapshot.Volume.EnvironmentID)] = struct{}{}
			for _, service := range source.Snapshot.Volume.Services {
				allowed[servicerecord.ServiceRuntimeKey(service.ServiceID)] = struct{}{}
			}
		}
		if source.Snapshot.Config != nil {
			snapshotID := source.Snapshot.Config.ConfigSnapshotID
			allowed[backupconfigrecord.BackupConfigSnapshotKey(snapshotID)] = struct{}{}
			allowed[backupconfigrecord.BackupConfigSnapshotTaskReferenceKey(run.TaskID, snapshotID)] = struct{}{}
			allowed[backupconfigrecord.BackupConfigSnapshotReferenceTaskKey(snapshotID, run.TaskID)] = struct{}{}
		}
	}
	result := make([]etcdstore.Condition, 0, len(allowed))
	for _, condition := range conditions {
		if _, keep := allowed[condition.Key]; keep {
			result = append(result, condition)
		}
	}
	return result
}
