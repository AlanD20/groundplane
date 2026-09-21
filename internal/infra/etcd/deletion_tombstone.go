package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyRepository) GetDeletionTombstone(
	ctx context.Context,
	targetKind deletionrecord.DeletionTargetKind,
	targetID string,
) (etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{}, false, err
	}
	if err := deletionrecord.ValidateDeletionTarget(targetKind, targetID); err != nil {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, deletionTombstoneKey(string(targetKind), targetID))
	if err != nil {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{}, false, errs.New(
			errs.KindInternal,
			"deletion tombstone read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := deletionrecord.DecodeDeletionTombstone(result.Entry.Value)
	if err != nil || record.TargetKind != targetKind || record.TargetID != targetID {
		return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{}, false, deletionrecord.CorruptDeletionTombstone()
	}
	return etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

// BeginEnvironmentDeletionWithTask atomically fences an Environment and
// publishes the immutable Agent Task that performs its host effects. The
// Environment and all indexes remain visible until finalization.
func (repository *HierarchyRepository) BeginEnvironmentDeletionWithTask(
	ctx context.Context,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	expectedBlueprintRevision int64,
	tombstone deletionrecord.DeletionTombstoneRecord,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if expectedBlueprintRevision < 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"expected Blueprint revision cannot be negative",
		)
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant || project.Revision <= 0 ||
		environment.Revision <= 0 ||
		project.ReadRevision < project.Revision ||
		environment.ReadRevision < environment.Revision ||
		environment.Record.ProjectID != project.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"environment hierarchy changed before deletion",
		)
	}
	if err := deletionrecord.ValidateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment || tombstone.TargetID != environment.Record.ID ||
		tombstone.TargetRevision != environment.Revision ||
		tombstone.TaskID != task.ID ||
		tombstone.Phase != deletionrecord.DeletionPhaseHostEffects ||
		!tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) ||
		task.Executor != taskjournal.TaskExecutorAgent ||
		task.Type != taskjournal.TaskRemove ||
		task.Target != environment.Record.ID ||
		task.Status != taskjournal.TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"environment deletion Task and tombstone do not match",
		)
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"environment deletion marker does not match its Task",
		)
	}

	readRevision := environment.ReadRevision
	if project.ReadRevision > readRevision {
		readRevision = project.ReadRevision
	}
	evidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			hierarchyrecord.EnvironmentKey(environment.Record.ID),
			hierarchyrecord.ProjectKey(project.Record.ID),
			hierarchyrecord.TenantKey(project.Record.TenantID),
			hierarchyrecord.EnvironmentNameKey(environment.Record.ProjectID, environment.Record.Name),
			hierarchyrecord.EnvironmentOwnerKey(environment.Record.ProjectID, environment.Record.ID),
			environmentBlueprintHeadKey(environment.Record.ID),
			projectionrecord.EnvironmentComposeProjectionStorageKey(environment.Record.ID),
			hierarchyrecord.EnvironmentMutationEpochKey(environment.Record.ID),
			hierarchyrecord.EnvironmentOperationLockKey(environment.Record.ID),
			deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.Record.ID),
			deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), project.Record.ID),
			deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), project.Record.TenantID),
		},
		Revision: readRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if evidence == nil || evidence.ReadRevision != readRevision || len(evidence.Values) != 12 ||
		evidence.Values[0] == nil || evidence.Values[1] == nil || evidence.Values[2] == nil ||
		evidence.Values[3] == nil || evidence.Values[4] == nil ||
		evidence.Values[7] == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"environment deletion fixed-revision evidence is incomplete",
		)
	}
	if evidence.Values[0].ModRevision != environment.Revision {
		return IdempotencyTransactionResult{}, stateConflict("environment", environment.Record.ID)
	}
	if evidence.Values[1].ModRevision != project.Revision {
		return IdempotencyTransactionResult{}, stateConflict("project", project.Record.ID)
	}
	storedTenant, err := hierarchyrecord.DecodeTenant(evidence.Values[2].Value)
	if err != nil || storedTenant.ID != project.Record.TenantID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"environment deletion Tenant is corrupt",
		)
	}
	if string(
		evidence.Values[3].Value,
	) != environment.Record.ID || string(evidence.Values[4].Value) != environment.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"environment deletion index evidence is corrupt",
		)
	}
	if evidence.Values[9] != nil || evidence.Values[10] != nil || evidence.Values[11] != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindResourceInUse,
			"environment deletion hierarchy is unavailable",
		)
	}
	epoch, err := decodeEnvironmentDeletionEpoch(evidence.Values[7], environment.Record.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	epochValue, err := backupruntime.EncodeEnvironmentMutationEpochRecord(epoch)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochValue)
	if evidence.Values[8] != nil {
		if _, err := decodeEnvironmentOperationLock(
			evidence.Values[8],
			environment.Record.ID,
		); err != nil {
			return IdempotencyTransactionResult{}, err
		}
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindResourceInUse,
			"environment operation is in progress",
		)
	}
	if expectedBlueprintRevision == 0 {
		if evidence.Values[5] != nil || evidence.Values[6] != nil || len(task.Params) != 1 {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindStateConflict,
				"environment Blueprint state changed",
			)
		}
	} else if evidence.Values[5] == nil || evidence.Values[6] == nil ||
		evidence.Values[5].ModRevision != expectedBlueprintRevision ||
		evidence.Values[6].ModRevision != expectedBlueprintRevision ||
		len(task.Params) != 4 || task.Params[EnvironmentDesiredRevisionParam] == "" ||
		task.Params[TaskMaterializationEnvironmentParam] != environment.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"environment Blueprint state changed",
		)
	}
	lock := backupruntime.BackupOperationLockRecord{
		EnvironmentID: environment.Record.ID,
		OperationID:   task.OperationID,
		TaskID:        task.ID,
		Kind:          backupruntime.BackupOperationDeletion,
		CreatedAt:     task.CreatedAt,
		UpdatedAt:     task.CreatedAt,
	}
	lockValue, err := backupruntime.EncodeBackupOperationLockRecord(lock)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(lockValue)

	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	tombstoneKey := deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.Record.ID)
	conditions := []etcdstore.Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{
			Key:         hierarchyrecord.EnvironmentNameKey(environment.Record.ProjectID, environment.Record.Name),
			ModRevision: evidence.Values[3].ModRevision,
		},
		{
			Key:         hierarchyrecord.EnvironmentOwnerKey(environment.Record.ProjectID, environment.Record.ID),
			ModRevision: evidence.Values[4].ModRevision,
		},
		{Key: tombstoneKey},
		{
			Key:         environmentBlueprintHeadKey(environment.Record.ID),
			ModRevision: expectedBlueprintRevision,
		},
		{
			Key:         projectionrecord.EnvironmentComposeProjectionStorageKey(environment.Record.ID),
			ModRevision: expectedBlueprintRevision,
		},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), project.Record.ID)},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), project.Record.TenantID)},
		{
			Key:         hierarchyrecord.EnvironmentMutationEpochKey(environment.Record.ID),
			ModRevision: evidence.Values[7].ModRevision,
		},
		{Key: hierarchyrecord.EnvironmentOperationLockKey(environment.Record.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentOperationLockKey(environment.Record.ID),
			Value: lockValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentMutationEpochKey(environment.Record.ID),
			Value: epochValue,
		},
	}
	fixedTenant := etcdstore.Versioned[hierarchyrecord.TenantRecord]{
		Record: storedTenant, Revision: evidence.Values[2].ModRevision, ReadRevision: readRevision,
	}
	fixedProject := project
	fixedProject.ReadRevision = readRevision
	fixedEnvironment := environment
	fixedEnvironment.ReadRevision = readRevision
	initiation, err := newEnvironmentTaskInitiation(
		&fixedTenant, fixedProject, fixedEnvironment, TaskActorOperator,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	cleanupSnapshot, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		hierarchyrecord.EnvironmentMutationEpochKey(environment.Record.ID),
	}})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if cleanupSnapshot == nil || cleanupSnapshot.ReadRevision < readRevision ||
		len(cleanupSnapshot.Values) != 1 {
		if cleanupSnapshot != nil {
			clearKeyValues(cleanupSnapshot.Values)
		}
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"environment deletion cleanup snapshot is incomplete",
		)
	}
	cleanupReadRevision := cleanupSnapshot.ReadRevision
	clearKeyValues(cleanupSnapshot.Values)
	cleanupPhase, err := initialEnvironmentDeletionCleanupPhase(
		ctx,
		repository.store,
		environment.Record.ID,
		task.OperationID,
		cleanupReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	intent, err := newEnvironmentDeletionIntent(
		environment.Record.ID,
		task.OperationID,
		task.ID,
		environment.Revision,
		cleanupPhase,
		task.CreatedAt,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	intentValue, err := encodeEnvironmentDeletionIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)
	conditions = append(conditions, etcdstore.Condition{Key: environmentDeletionIntentKey(task.OperationID)})
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: environmentDeletionIntentKey(task.OperationID), Value: intentValue,
	})

	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifyEnvironmentDeletionIntentStartConflict(classifyEnvironmentDeletionStartConflict(
			project,
			environment,
			task.OperationID,
			expectedBlueprintRevision,
			evidence.Values[7].ModRevision,
		)),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyEnvironmentDeletionStartConflict(
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	operationID string,
	expectedBlueprintRevision int64,
	expectedEpochRevision int64,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != 15 {
			return errs.New(
				errs.KindInternal,
				"environment deletion compare evidence is incomplete",
			)
		}
		if values[2] != nil {
			activeTaskID, err := idempotencyrecord.DecodeTaskReference(values[2].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				operationID,
				activeTaskID,
			)
		}
		for _, index := range []int{0, 1, 3} {
			if values[index] != nil {
				return errs.New(
					errs.KindInternal,
					"environment deletion collided with durable Task state",
				)
			}
		}
		if values[4] == nil {
			return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
		}
		if values[4].ModRevision != environment.Revision {
			return stateConflict("environment", environment.Record.ID)
		}
		for _, index := range []int{5, 6} {
			if values[index] == nil || string(values[index].Value) != environment.Record.ID {
				return errs.New(
					errs.KindInternal,
					"environment deletion index changed or is corrupt",
				)
			}
		}
		if values[7] != nil {
			return errs.New(errs.KindResourceInUse, "environment deletion is already in progress")
		}
		if keyValueRevision(values[8]) != expectedBlueprintRevision ||
			keyValueRevision(values[9]) != expectedBlueprintRevision {
			return errs.New(errs.KindStateConflict, "environment Blueprint state changed")
		}
		if values[10] == nil {
			return errs.New(errs.KindProjectNotFound, "project was not found")
		}
		if values[10].ModRevision != project.Revision {
			return stateConflict("project", project.Record.ID)
		}
		if values[11] != nil {
			return errs.New(errs.KindResourceInUse, "project deletion is in progress")
		}
		if values[12] != nil {
			return errs.New(errs.KindResourceInUse, "tenant deletion is in progress")
		}
		if values[13] == nil {
			return errs.New(errs.KindInternal, "environment mutation epoch is missing")
		}
		if _, err := decodeEnvironmentDeletionEpoch(values[13], environment.Record.ID); err != nil {
			return err
		}
		if values[13].ModRevision != expectedEpochRevision {
			return errs.New(errs.KindStateConflict, "environment mutation epoch advanced")
		}
		if values[14] != nil {
			if _, err := decodeEnvironmentOperationLock(
				values[14],
				environment.Record.ID,
			); err != nil {
				return err
			}
			return errs.New(errs.KindResourceInUse, "environment operation is in progress")
		}
		return errs.New(errs.KindStateConflict, "environment deletion state changed")
	}
}

func decodeEnvironmentDeletionEpoch(
	value *etcdstore.KeyValue,
	environmentID string,
) (backupruntime.EnvironmentMutationEpochRecord, error) {
	if value == nil {
		return backupruntime.EnvironmentMutationEpochRecord{}, errs.New(
			errs.KindInternal,
			"environment mutation epoch is missing",
		)
	}
	record, err := backupruntime.DecodeEnvironmentMutationEpochRecord(value.Value)
	if err != nil || record.EnvironmentID != environmentID {
		return backupruntime.EnvironmentMutationEpochRecord{}, errs.New(
			errs.KindInternal,
			"environment mutation epoch is corrupt",
		)
	}
	return record, nil
}

func decodeEnvironmentOperationLock(
	value *etcdstore.KeyValue,
	environmentID string,
) (backupruntime.BackupOperationLockRecord, error) {
	if value == nil {
		return backupruntime.BackupOperationLockRecord{}, errs.New(
			errs.KindStateConflict,
			"environment operation lock is missing",
		)
	}
	record, err := backupruntime.DecodeBackupOperationLockRecord(value.Value)
	if err != nil || record.EnvironmentID != environmentID {
		return backupruntime.BackupOperationLockRecord{}, errs.New(
			errs.KindInternal,
			"environment operation lock is corrupt",
		)
	}
	return record, nil
}

func decodeOwnedEnvironmentDeletionLock(
	value *etcdstore.KeyValue,
	task TaskRecord,
) (backupruntime.BackupOperationLockRecord, error) {
	record, err := decodeEnvironmentOperationLock(value, task.Target)
	if err != nil {
		return backupruntime.BackupOperationLockRecord{}, err
	}
	if record.Kind != backupruntime.BackupOperationDeletion || record.OperationID != task.OperationID ||
		record.TaskID != task.ID {
		return backupruntime.BackupOperationLockRecord{}, errs.New(
			errs.KindStateConflict,
			"environment deletion operation lock ownership changed",
		)
	}
	return record, nil
}

func keyValueRevision(value *etcdstore.KeyValue) int64 {
	if value == nil {
		return 0
	}
	return value.ModRevision
}
