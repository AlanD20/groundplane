package etcd

import (
	"context"
	backupconfigrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupscheduling"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) prepareBackupRunPublication(
	ctx context.Context,
	record backupruntime.BackupRunRecord,
	lock backupruntime.BackupOperationLockRecord,
	fixedRevision int64,
) (backupRunPublicationPlan, error) {
	return repository.prepareBackupRunPublicationWithRetry(
		ctx, record, lock, nil, fixedRevision,
	)
}

func (repository *BackupRuntimeRepository) prepareBackupRunPublicationWithRetry(
	ctx context.Context,
	record backupruntime.BackupRunRecord,
	lock backupruntime.BackupOperationLockRecord,
	retrySource *backupRunRetrySource,
	fixedRevision int64,
) (backupRunPublicationPlan, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return backupRunPublicationPlan{}, err
	}
	if record.State != backupruntime.BackupRunQueued || fixedRevision <= 0 ||
		(record.RetryOfTaskID == "") != (retrySource == nil) ||
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
		retrySource,
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
	policyFence := []etcdstore.Condition(nil)
	if retrySource == nil {
		policyFence, err = repository.loadManualBackupPolicyFence(ctx, record, fixedRevision)
		if err != nil {
			etcdstore.ClearMutationValues(mutations)
			clear(lockValue)
			return backupRunPublicationPlan{}, err
		}
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
	conditions = append(conditions, policyFence...)
	if retrySource != nil {
		conditions = append(
			conditions,
			etcdstore.Condition{
				Key:         taskjournal.TaskStorageKey(retrySource.task.Record.ID),
				ModRevision: retrySource.task.Revision,
			},
			etcdstore.Condition{
				Key:         backupruntime.BackupRunKey(retrySource.run.Record.TaskID),
				ModRevision: retrySource.run.Revision,
			},
			etcdstore.Condition{
				Key:         backupruntime.BackupTerminalReceiptKey(retrySource.task.Record.ID),
				ModRevision: retrySource.receiptRevision,
			},
		)
	}
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentOperationLockKey(record.EnvironmentID), Value: lockValue,
	})
	mutations = append(mutations, snapshotMutations...)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return backupRunPublicationPlan{}, err
	}
	mutations = append(mutations, epoch)
	if retrySource == nil && record.Initiator == backupruntime.BackupRunInitiatorSchedule {
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
		replay:     repository.validateExistingBackupRunPublication,
	}, nil
}

// backupRunExternalConditions retains CAS for each source definition and for
// backing Services outside the consumer Environment. Source definitions may
// change independently of an Environment operation; the exact Environment
// epoch and operation lock serialize the remaining consumer-owned evidence.
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
		if source.Snapshot.Postgres != nil {
			allowed[blueprints.EnvironmentBlueprintHeadKey(source.Snapshot.Postgres.BackingEnvironmentID)] = struct{}{}
		}
		if source.Snapshot.Volume != nil {
			allowed[projectionrecord.EnvironmentComposeProjectionStorageKey(source.Snapshot.Volume.EnvironmentID)] = struct{}{}
			for _, service := range source.Snapshot.Volume.Services {
				allowed[servicerecord.ServiceRuntimeKey(service.ServiceID)] = struct{}{}
			}
		}
		if source.Snapshot.Config != nil {
			snapshotID := source.Snapshot.Config.ConfigSnapshotID
			allowed[backupconfigrecord.BackupConfigSnapshotKey(snapshotID)] = struct{}{}
			allowed[backupconfigrecord.BackupConfigSnapshotTaskReferenceKey(run.RetryOfTaskID, snapshotID)] = struct{}{}
			allowed[backupconfigrecord.BackupConfigSnapshotReferenceTaskKey(snapshotID, run.RetryOfTaskID)] = struct{}{}
			allowed[backupconfigrecord.BackupConfigSnapshotTaskReferenceKey(run.TaskID, snapshotID)] = struct{}{}
			allowed[backupconfigrecord.BackupConfigSnapshotReferenceTaskKey(snapshotID, run.TaskID)] = struct{}{}
			allowed[hierarchyrecord.EnvironmentKey(run.EnvironmentID)] = struct{}{}
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

func (repository *BackupRuntimeRepository) exactBackupRunConfigCompanions(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	readRevision int64,
	resultRevision int64,
) bool {
	for _, source := range run.Sources {
		if source.Kind != backupruntime.BackupRuntimeSourceConfig {
			continue
		}
		snapshot := source.Snapshot.Config
		keys := []string{
			backupconfigrecord.BackupConfigSnapshotKey(snapshot.ConfigSnapshotID),
			backupconfigrecord.BackupConfigSnapshotTaskReferenceKey(run.TaskID, snapshot.ConfigSnapshotID),
			backupconfigrecord.BackupConfigSnapshotReferenceTaskKey(snapshot.ConfigSnapshotID, run.TaskID),
		}
		read, err := repository.ReadFixedKeys(ctx, keys, readRevision)
		if err != nil {
			return false
		}
		primaryRevisionValid := read.Values[0] != nil &&
			((run.RetryOfTaskID == "" && read.Values[0].ModRevision == resultRevision) ||
				(run.RetryOfTaskID != "" && read.Values[0].ModRevision > 0 &&
					read.Values[0].ModRevision < resultRevision))
		if !primaryRevisionValid || read.Values[1] == nil || read.Values[2] == nil ||
			read.Values[1].ModRevision != resultRevision ||
			read.Values[2].ModRevision != resultRevision ||
			string(read.Values[1].Value) != snapshot.ConfigSnapshotID ||
			string(read.Values[2].Value) != run.TaskID {
			etcdstore.ClearValues(read.Values)
			return false
		}
		stored, decodeErr := backupconfigrecord.DecodeBackupConfigSnapshotRecord(read.Values[0].Value)
		etcdstore.ClearValues(read.Values)
		createdAtValid := (run.RetryOfTaskID == "" && stored.CreatedAt.Equal(run.CreatedAt)) ||
			(run.RetryOfTaskID != "" && stored.CreatedAt.Before(run.CreatedAt))
		if decodeErr != nil || stored.SnapshotID != snapshot.ConfigSnapshotID ||
			stored.EnvironmentID != run.EnvironmentID || stored.SourceID != source.SourceID ||
			stored.State == backupconfigrecord.BackupConfigSnapshotUninitialized ||
			stored.ReadRevision != snapshot.ReadRevision || !createdAtValid ||
			(run.RetryOfTaskID == "" && (!stored.UpdatedAt.Equal(run.CreatedAt) ||
				stored.State != backupconfigrecord.BackupConfigSnapshotBuilding)) {
			return false
		}
	}
	return true
}
