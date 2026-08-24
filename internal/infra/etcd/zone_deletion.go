package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginZoneDeletionWithTask atomically fences one Zone and publishes either
// the direct Agent removal Task or the backing-Zone Controller cascade. The
// Zone, indexes, and subnet reservation stay visible until finalization.
func (repository *ZoneRepository) BeginZoneDeletionWithTask(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	zone Versioned[ZoneRecord],
	tombstone DeletionTombstoneRecord,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateZoneHierarchy(ctx, environment, project, zone.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	ordinary := zone.Record.Desired.OwnerKind == core.ZoneOwnerEnvironment &&
		zone.Record.Desired.OwnerID == environment.Record.ID && project.Record.Kind == ProjectKindTenant
	backing := zone.Record.Desired.OwnerKind == core.ZoneOwnerBackingProject &&
		zone.Record.Desired.OwnerID == project.Record.ID && project.Record.Kind == ProjectKindBacking
	if zone.Revision <= 0 || zone.ReadRevision < zone.Revision || (!ordinary && !backing) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Zone deletion ownership is invalid",
		)
	}
	if err := validateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	ordinaryTask := ordinary && task.Executor == TaskExecutorAgent && len(task.Params) == 1 &&
		task.Params[TaskZoneEnvironmentParam] == environment.Record.ID
	backingTask := backing && task.Executor == TaskExecutorController && len(task.Params) == 3 &&
		task.Params[TaskResourceKindParam] == TaskResourceBackingZone &&
		task.Params[TaskZoneEnvironmentParam] == environment.Record.ID &&
		validSHA256(task.Params[TaskZoneImpactTokenParam])
	if tombstone.TargetKind != DeletionTargetZone || tombstone.TargetID != zone.Record.Desired.ID ||
		tombstone.TargetRevision != zone.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != DeletionPhaseHostEffects || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) ||
		task.Type != TaskRemove || task.Target != zone.Record.Desired.ID || task.Status != TaskStatusPending ||
		(!ordinaryTask && !backingTask) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Zone deletion Task and tombstone do not match",
		)
	}
	wantReplayTarget := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetZone, ID: zone.Record.Desired.ID}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Zone deletion marker does not match its Task",
		)
	}

	secondary, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		zoneNameKey(zone.Record.EnvironmentID, zone.Record.Desired.Name),
		zoneOwnerKey(zone.Record.EnvironmentID, zone.Record.Desired.ID),
	}})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if secondary == nil || len(secondary.Values) != 2 || secondary.Values[0] == nil ||
		secondary.Values[1] == nil || string(secondary.Values[0].Value) != zone.Record.Desired.ID ||
		string(secondary.Values[1].Value) != zone.Record.Desired.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Zone deletion indexes are corrupt")
	}
	pool, err := repository.getZonePoolRegistry(ctx, environment.Record.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if pool.Revision <= 0 || pool.Record.Reservations[zone.Record.Desired.ID] != zone.Record.Desired.Subnet {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Zone subnet reservation is inconsistent")
	}
	addresses, err := getComponentAddressRegistry(ctx, repository.store, zone.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(addresses.Record.Reservations) != 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindResourceInUse,
			"Zone is selected by an enabled Caddy Component; move or disable Caddy first",
		)
	}

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

	tombstoneKey := deletionTombstoneKey(string(DeletionTargetZone), zone.Record.Desired.ID)
	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: zoneKey(zone.Record.Desired.ID), ModRevision: zone.Revision},
		{
			Key:         zoneNameKey(zone.Record.EnvironmentID, zone.Record.Desired.Name),
			ModRevision: secondary.Values[0].ModRevision,
		},
		{
			Key:         zoneOwnerKey(zone.Record.EnvironmentID, zone.Record.Desired.ID),
			ModRevision: secondary.Values[1].ModRevision,
		},
		{Key: zonePoolRegistryKey(zone.Record.EnvironmentID), ModRevision: pool.Revision},
		{Key: componentAddressRegistryKey(zone.Record.Desired.ID), ModRevision: addresses.Revision},
		{Key: tombstoneKey},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID)},
	}
	if ordinary {
		conditions = append(conditions, Condition{
			Key: deletionTombstoneKey(string(DeletionTargetTenant), project.Record.TenantID),
		})
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: tombstoneKey, Value: tombstoneValue},
	}
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifyZoneDeletionStartConflict(
			environment, project, zone, pool, addresses, task.OperationID, ordinary,
		),
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

func classifyZoneDeletionStartConflict(
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	zone Versioned[ZoneRecord],
	pool Versioned[zonePoolRegistry],
	addresses Versioned[componentAddressRegistry],
	operationID string,
	hasTenantFence bool,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		expectedValues := 14
		if hasTenantFence {
			expectedValues++
		}
		if len(values) != expectedValues {
			return errs.New(errs.KindInternal, "Zone deletion compare evidence is incomplete")
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
				return errs.New(errs.KindInternal, "Zone deletion collided with durable Task state")
			}
		}
		if values[4] == nil {
			return errs.New(errs.KindZoneNotFound, "zone was not found")
		}
		if values[4].ModRevision != zone.Revision {
			return stateConflict("zone", zone.Record.Desired.ID)
		}
		for _, index := range []int{5, 6} {
			if values[index] == nil || string(values[index].Value) != zone.Record.Desired.ID {
				return errs.New(errs.KindInternal, "Zone deletion index changed or is corrupt")
			}
		}
		if values[7] == nil || values[7].ModRevision != pool.Revision {
			return stateConflict("Zone pool registry", environment.Record.ID)
		}
		if keyValueRevision(values[8]) != addresses.Revision {
			return errs.New(errs.KindResourceInUse, "Zone Component address reservations changed")
		}
		if values[9] != nil {
			return errs.New(errs.KindResourceInUse, "Zone deletion is already in progress")
		}
		if values[10] == nil || values[10].ModRevision != environment.Revision {
			return stateConflict("environment", environment.Record.ID)
		}
		if values[11] == nil || values[11].ModRevision != project.Revision {
			return stateConflict("project", project.Record.ID)
		}
		for index := 12; index < expectedValues; index++ {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Zone hierarchy deletion is in progress")
			}
		}
		return errs.New(errs.KindStateConflict, "Zone deletion state changed")
	}
}

// HandoffBackingZoneDeletion atomically transfers a fenced backing Zone from
// its running Controller parent to the normal Agent network-removal Task.
func (repository *ZoneRepository) HandoffBackingZoneDeletion(
	ctx context.Context,
	zone Versioned[ZoneRecord],
	parentTaskID string,
	tombstone Versioned[DeletionTombstoneRecord],
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if zone.Revision <= 0 || tombstone.Revision <= 0 ||
		zone.Record.Desired.OwnerKind != core.ZoneOwnerBackingProject ||
		validateStableID(ids.KindTask, parentTaskID) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "backing Zone handoff is invalid")
	}
	currentTombstone := tombstone.Record
	if currentTombstone.TargetKind != DeletionTargetZone ||
		currentTombstone.TargetID != zone.Record.Desired.ID ||
		currentTombstone.TargetRevision != zone.Revision || currentTombstone.TaskID != parentTaskID ||
		currentTombstone.Phase != DeletionPhaseHostEffects || currentTombstone.Checkpoint != (DeletionCheckpoint{}) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "backing Zone cascade fence changed")
	}
	if task.Executor != TaskExecutorAgent || task.Type != TaskRemove || task.Target != zone.Record.Desired.ID ||
		task.Status != TaskStatusPending || len(task.Params) != 1 ||
		task.Params[TaskZoneEnvironmentParam] != zone.Record.EnvironmentID ||
		marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.ReplayTarget != nil ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != zone.Record.EnvironmentID ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backing Zone Agent handoff Task is invalid",
		)
	}
	parentResult, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		taskKey(parentTaskID), componentAddressRegistryKey(zone.Record.Desired.ID),
	}, Revision: tombstone.ReadRevision})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if parentResult == nil || len(parentResult.Values) != 2 || parentResult.Values[0] == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "backing Zone parent Task is missing")
	}
	parent, err := decodeTaskRecord(parentResult.Values[0].Value)
	if err != nil || parent.ID != parentTaskID || parent.Executor != TaskExecutorController ||
		parent.Status != TaskStatusRunning || parent.Target != zone.Record.Desired.ID ||
		parent.Params[TaskResourceKindParam] != TaskResourceBackingZone {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"backing Zone parent Task is not running",
		)
	}
	if parentResult.Values[1] != nil {
		addresses, decodeErr := decodeEnvelope[componentAddressRegistry](
			parentResult.Values[1].Value, "component_address_registry",
		)
		if decodeErr != nil || validateComponentAddressRegistry(zone.Record, addresses) != nil ||
			len(addresses.Reservations) != 0 {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindResourceInUse,
				"Zone gained a Component address reservation",
			)
		}
	}
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
	currentTombstone.TaskID = task.ID
	currentTombstone.UpdatedAt = task.CreatedAt
	tombstoneValue, err := encodeDeletionTombstone(currentTombstone)
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
	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: zoneKey(zone.Record.Desired.ID), ModRevision: zone.Revision},
		{
			Key:         deletionTombstoneKey(string(DeletionTargetZone), zone.Record.Desired.ID),
			ModRevision: tombstone.Revision,
		},
		{Key: taskKey(parentTaskID), ModRevision: parentResult.Values[0].ModRevision},
		{
			Key:         componentAddressRegistryKey(zone.Record.Desired.ID),
			ModRevision: keyValueRevision(parentResult.Values[1]),
		},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type:  MutationPut,
			Key:   deletionTombstoneKey(string(DeletionTargetZone), zone.Record.Desired.ID),
			Value: tombstoneValue,
		},
	}
	initiation, err := newInheritedTaskInitiation(Versioned[TaskRecord]{
		Record: parent, Revision: parentResult.Values[0].ModRevision, ReadRevision: parentResult.ReadRevision,
	}, TaskActorSystem)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		func(_ int64, values []*KeyValue) error {
			if len(values) != len(conditions) {
				return errs.New(errs.KindInternal, "backing Zone handoff compare evidence is incomplete")
			}
			if values[2] != nil {
				return errs.New(errs.KindStateConflict, "backing Zone final removal is already active")
			}
			return errs.New(errs.KindStateConflict, "backing Zone handoff state changed")
		},
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
