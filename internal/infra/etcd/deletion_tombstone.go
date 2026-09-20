package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DeletionTargetKind is the closed hierarchy deletion catalog. Additional
// resource kinds enter this catalog only with their resource-specific
// finalization contract.
type DeletionTargetKind string

const (
	DeletionTargetTenant       DeletionTargetKind = "tenant"
	DeletionTargetProject      DeletionTargetKind = "project"
	DeletionTargetEnvironment  DeletionTargetKind = "environment"
	DeletionTargetEntry        DeletionTargetKind = "entry"
	DeletionTargetRoute        DeletionTargetKind = "route"
	DeletionTargetScript       DeletionTargetKind = "script"
	DeletionTargetSecret       DeletionTargetKind = "secret"
	DeletionTargetConnector    DeletionTargetKind = "connector"
	DeletionTargetZone         DeletionTargetKind = "zone"
	DeletionTargetReleaseGroup DeletionTargetKind = "release_group"
	DeletionTargetService      DeletionTargetKind = "service"
	DeletionTargetRunner       DeletionTargetKind = "runner"
)

// DeletionPhase records which authority may advance a destructive operation.
// A completed tombstone is never retained.
type DeletionPhase string

const (
	DeletionPhaseHostEffects DeletionPhase = "host_effects"
	DeletionPhaseFinalizing  DeletionPhase = "finalizing"
)

// DeletionCheckpoint identifies the last fully finalized descendant. Empty
// fields are the initial checkpoint; nonempty fields are always paired.
type DeletionCheckpoint struct {
	ResourceKind string `json:"resource_kind,omitempty"`
	StableID     string `json:"stable_id,omitempty"`
}

// DeletionTombstoneRecord fences one resource while its destructive Task
// performs any authority-owned effects and atomic finalization.
type DeletionTombstoneRecord struct {
	TargetKind     DeletionTargetKind `json:"target_kind"`
	TargetID       string             `json:"target_id"`
	TargetRevision int64              `json:"target_revision"`
	TaskID         string             `json:"task_id"`
	Phase          DeletionPhase      `json:"phase"`
	Checkpoint     DeletionCheckpoint `json:"checkpoint"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

func (repository *HierarchyRepository) GetDeletionTombstone(
	ctx context.Context,
	targetKind DeletionTargetKind,
	targetID string,
) (Versioned[DeletionTombstoneRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[DeletionTombstoneRecord]{}, false, err
	}
	if err := validateDeletionTarget(targetKind, targetID); err != nil {
		return Versioned[DeletionTombstoneRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, deletionTombstoneKey(string(targetKind), targetID))
	if err != nil {
		return Versioned[DeletionTombstoneRecord]{}, false, err
	}
	if result == nil {
		return Versioned[DeletionTombstoneRecord]{}, false, errs.New(
			errs.KindInternal,
			"deletion tombstone read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[DeletionTombstoneRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := decodeDeletionTombstone(result.Entry.Value)
	if err != nil || record.TargetKind != targetKind || record.TargetID != targetID {
		return Versioned[DeletionTombstoneRecord]{}, false, corruptDeletionTombstone()
	}
	return Versioned[DeletionTombstoneRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

// BeginEnvironmentDeletionWithTask atomically fences an Environment and
// publishes the immutable Agent Task that performs its host effects. The
// Environment and all indexes remain visible until finalization.
func (repository *HierarchyRepository) BeginEnvironmentDeletionWithTask(
	ctx context.Context,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	expectedBlueprintRevision int64,
	tombstone DeletionTombstoneRecord,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateProject(project.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEnvironment(environment.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if expectedBlueprintRevision < 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"expected Blueprint revision cannot be negative",
		)
	}
	if project.Record.Kind != ProjectKindTenant || project.Revision <= 0 ||
		environment.Revision <= 0 ||
		project.ReadRevision < project.Revision ||
		environment.ReadRevision < environment.Revision ||
		environment.Record.ProjectID != project.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"environment hierarchy changed before deletion",
		)
	}
	if err := validateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if tombstone.TargetKind != DeletionTargetEnvironment || tombstone.TargetID != environment.Record.ID ||
		tombstone.TargetRevision != environment.Revision ||
		tombstone.TaskID != task.ID ||
		tombstone.Phase != DeletionPhaseHostEffects ||
		!tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) ||
		task.Executor != TaskExecutorAgent ||
		task.Type != TaskRemove ||
		task.Target != environment.Record.ID ||
		task.Status != TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"environment deletion Task and tombstone do not match",
		)
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
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
			environmentKey(environment.Record.ID),
			projectKey(project.Record.ID),
			tenantKey(project.Record.TenantID),
			environmentNameKey(environment.Record.ProjectID, environment.Record.Name),
			environmentOwnerKey(environment.Record.ProjectID, environment.Record.ID),
			environmentBlueprintHeadKey(environment.Record.ID),
			environmentComposeProjectionKey(environment.Record.ID),
			environmentMutationEpochKey(environment.Record.ID),
			environmentOperationLockKey(environment.Record.ID),
			deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID),
			deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID),
			deletionTombstoneKey(string(DeletionTargetTenant), project.Record.TenantID),
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
	storedTenant, err := decodeTenant(evidence.Values[2].Value)
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
	epochValue, err := encodeEnvironmentMutationEpochRecord(epoch)
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
	lock := BackupOperationLockRecord{
		EnvironmentID: environment.Record.ID,
		OperationID:   task.OperationID,
		TaskID:        task.ID,
		Kind:          BackupOperationDeletion,
		CreatedAt:     task.CreatedAt,
		UpdatedAt:     task.CreatedAt,
	}
	lockValue, err := encodeBackupOperationLockRecord(lock)
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
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	tombstoneValue, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	tombstoneKey := deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID)
	conditions := []etcdstore.Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{
			Key:         environmentNameKey(environment.Record.ProjectID, environment.Record.Name),
			ModRevision: evidence.Values[3].ModRevision,
		},
		{
			Key:         environmentOwnerKey(environment.Record.ProjectID, environment.Record.ID),
			ModRevision: evidence.Values[4].ModRevision,
		},
		{Key: tombstoneKey},
		{
			Key:         environmentBlueprintHeadKey(environment.Record.ID),
			ModRevision: expectedBlueprintRevision,
		},
		{
			Key:         environmentComposeProjectionKey(environment.Record.ID),
			ModRevision: expectedBlueprintRevision,
		},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetTenant), project.Record.TenantID)},
		{
			Key:         environmentMutationEpochKey(environment.Record.ID),
			ModRevision: evidence.Values[7].ModRevision,
		},
		{Key: environmentOperationLockKey(environment.Record.ID)},
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
			Key:   environmentOperationLockKey(environment.Record.ID),
			Value: lockValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   environmentMutationEpochKey(environment.Record.ID),
			Value: epochValue,
		},
	}
	fixedTenant := Versioned[TenantRecord]{
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
		environmentMutationEpochKey(environment.Record.ID),
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
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
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
			activeTaskID, err := decodeTaskReference(values[2].Value)
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
) (EnvironmentMutationEpochRecord, error) {
	if value == nil {
		return EnvironmentMutationEpochRecord{}, errs.New(
			errs.KindInternal,
			"environment mutation epoch is missing",
		)
	}
	record, err := decodeEnvironmentMutationEpochRecord(value.Value)
	if err != nil || record.EnvironmentID != environmentID {
		return EnvironmentMutationEpochRecord{}, errs.New(
			errs.KindInternal,
			"environment mutation epoch is corrupt",
		)
	}
	return record, nil
}

func decodeEnvironmentOperationLock(
	value *etcdstore.KeyValue,
	environmentID string,
) (BackupOperationLockRecord, error) {
	if value == nil {
		return BackupOperationLockRecord{}, errs.New(
			errs.KindStateConflict,
			"environment operation lock is missing",
		)
	}
	record, err := decodeBackupOperationLockRecord(value.Value)
	if err != nil || record.EnvironmentID != environmentID {
		return BackupOperationLockRecord{}, errs.New(
			errs.KindInternal,
			"environment operation lock is corrupt",
		)
	}
	return record, nil
}

func decodeOwnedEnvironmentDeletionLock(
	value *etcdstore.KeyValue,
	task TaskRecord,
) (BackupOperationLockRecord, error) {
	record, err := decodeEnvironmentOperationLock(value, task.Target)
	if err != nil {
		return BackupOperationLockRecord{}, err
	}
	if record.Kind != BackupOperationDeletion || record.OperationID != task.OperationID ||
		record.TaskID != task.ID {
		return BackupOperationLockRecord{}, errs.New(
			errs.KindStateConflict,
			"environment deletion operation lock ownership changed",
		)
	}
	return record, nil
}

func encodeDeletionTombstone(record DeletionTombstoneRecord) ([]byte, error) {
	if err := validateDeletionTombstone(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("deletion-tombstone", record)
}

func decodeDeletionTombstone(value []byte) (DeletionTombstoneRecord, error) {
	record, err := decodeEnvelope[DeletionTombstoneRecord](value, "deletion-tombstone")
	if err != nil {
		return DeletionTombstoneRecord{}, err
	}
	if err := validateDeletionTombstone(record); err != nil {
		return DeletionTombstoneRecord{}, corruptDeletionTombstone()
	}
	return record, nil
}

func validateDeletionTombstone(record DeletionTombstoneRecord) error {
	if err := validateDeletionTarget(record.TargetKind, record.TargetID); err != nil {
		return err
	}
	if record.TargetRevision <= 0 || validateStableID(ids.KindTask, record.TaskID) != nil ||
		(record.Phase != DeletionPhaseHostEffects && record.Phase != DeletionPhaseFinalizing) ||
		record.CreatedAt.IsZero() || !record.CreatedAt.Equal(record.CreatedAt.UTC()) ||
		record.UpdatedAt.Before(
			record.CreatedAt,
		) || !record.UpdatedAt.Equal(record.UpdatedAt.UTC()) {
		return errs.New(errs.KindValidationFailed, "deletion tombstone lifecycle is invalid")
	}
	checkpoint := record.Checkpoint
	if (checkpoint.ResourceKind == "") != (checkpoint.StableID == "") {
		return errs.New(errs.KindValidationFailed, "deletion checkpoint is incomplete")
	}
	if checkpoint.ResourceKind != "" && (!validDeletionCheckpointKind(checkpoint.ResourceKind) ||
		!utf8.ValidString(checkpoint.StableID) || checkpoint.StableID == "") {
		return errs.New(errs.KindValidationFailed, "deletion checkpoint is invalid")
	}
	return nil
}

func validateDeletionTarget(kind DeletionTargetKind, id string) error {
	var expected ids.Kind
	switch kind {
	case DeletionTargetTenant:
		expected = ids.KindTenant
	case DeletionTargetProject:
		expected = ids.KindProject
	case DeletionTargetEnvironment:
		expected = ids.KindEnvironment
	case DeletionTargetEntry:
		expected = ids.KindEnvEntry
	case DeletionTargetRoute:
		expected = ids.KindRoute
	case DeletionTargetScript:
		expected = ids.KindScript
	case DeletionTargetSecret:
		expected = ids.KindSecret
	case DeletionTargetConnector:
		expected = ids.KindConnector
	case DeletionTargetZone:
		expected = ids.KindNetwork
	case DeletionTargetReleaseGroup:
		expected = ids.KindReleaseGroup
	case DeletionTargetService:
		expected = ids.KindService
	case DeletionTargetRunner:
		expected = ids.KindRunner
	default:
		return errs.New(errs.KindValidationFailed, "deletion target kind is invalid")
	}
	if validateStableID(expected, id) != nil {
		return errs.New(errs.KindValidationFailed, "deletion target id is invalid")
	}
	return nil
}

func validDeletionCheckpointKind(value string) bool {
	switch value {
	case "zone", "service", "route", "volume", "entry", "script", "release_group", "component",
		"connector", "backup_policy", "blueprint_revision", "environment", "project":
		return true
	default:
		return false
	}
}

func keyValueRevision(value *etcdstore.KeyValue) int64 {
	if value == nil {
		return 0
	}
	return value.ModRevision
}

func corruptDeletionTombstone() error {
	return errs.New(errs.KindInternal, "deletion tombstone is corrupt")
}
