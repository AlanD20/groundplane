package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginZoneDeletionWithTask atomically fences one Zone and publishes either
// the direct Agent removal Task or the backing-Zone Controller cascade. The
// Zone, indexes, and subnet reservation stay visible until finalization.
func (repository *ZoneRepository) BeginZoneDeletionWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	zone etcdstore.Versioned[zonerecord.Record],
	authorities EnvironmentZoneRemovalAuthorities,
	tombstone deletionrecord.DeletionTombstoneRecord,
	intent ZoneRemovalIntent,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := deletionrecord.ValidateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateZoneRemovalIntent(intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	selected, err := selectedZoneDeletionRecord(authorities.Desired, zone, intent.ZoneID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	zone = selected
	if selectedByEnabledComponent(authorities.Desired.Record.Components, zone.Record.Desired.ID) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindResourceInUse,
			"Zone is selected by an enabled Component; change or disable the Component first",
		)
	}
	if err := validateZoneDeletionHierarchy(ctx, environment, project, zone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	ordinary := zone.Record.Desired.OwnerKind == core.ZoneOwnerEnvironment &&
		zone.Record.Desired.OwnerID == environment.Record.ID && project.Record.Kind == hierarchyrecord.ProjectKindTenant
	backing := zone.Record.Desired.OwnerKind == core.ZoneOwnerBackingProject &&
		zone.Record.Desired.OwnerID == project.Record.ID && project.Record.Kind == hierarchyrecord.ProjectKindBacking
	if !ordinary && !backing {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Zone deletion ownership is invalid",
		)
	}
	if err := validateZoneRemovalTaskOwner(task, intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	ordinaryTask := ordinary && task.Executor == taskjournal.TaskExecutorAgent
	backingTask := backing && task.Executor == taskjournal.TaskExecutorController
	if tombstone.TargetKind != deletionrecord.DeletionTargetZone || tombstone.TargetID != zone.Record.Desired.ID ||
		tombstone.TargetRevision != zone.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != deletionrecord.DeletionPhaseHostEffects || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) ||
		task.Type != taskjournal.TaskRemove || task.Target != zone.Record.Desired.ID || task.Status != taskjournal.TaskStatusPending ||
		(!ordinaryTask && !backingTask) || intent.ZoneRevision != zone.Revision ||
		authorities.Desired.Revision != intent.DesiredHeadRevision ||
		authorities.Applied.Revision != intent.AppliedProjectionRevision ||
		!sameServiceRemovalProjection(authorities.Desired.Record, intent.DesiredProjection) ||
		!sameServiceRemovalProjection(authorities.Applied.Record, intent.AppliedProjection) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Zone deletion Task and tombstone do not match",
		)
	}
	wantReplayTarget := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetZone, ID: zone.Record.Desired.ID}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || marker.Locator != intent.Claim.Locator ||
		!sameBlueprintProtectedIntent(marker.Intent, intent.Claim.Intent) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Zone deletion marker does not match its Task",
		)
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
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	hierarchy := &HierarchyRepository{store: repository.store}
	publication, err := hierarchy.prepareEnvironmentDirectPublication(
		ctx, intent.Claim,
		EnvironmentDesiredRevisionIdentity{EnvironmentID: intent.EnvironmentID, RevisionID: intent.Claim.RevisionID},
		intent.CandidateProjection,
		idempotencyrecord.IdempotencyMarker{Locator: intent.Claim.Locator, Intent: intent.Claim.Intent},
		intent.DesiredHeadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)
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
	intentValue, err := encodeZoneRemovalIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)

	tombstoneKey := deletionTombstoneKey(string(deletionrecord.DeletionTargetZone), zone.Record.Desired.ID)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		{Key: zonePoolRegistryKey(zone.Record.EnvironmentID), ModRevision: pool.Revision},
		{Key: componentAddressRegistryKey(zone.Record.Desired.ID), ModRevision: addresses.Revision},
		{Key: tombstoneKey},
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), project.Record.ID)},
		{Key: zoneRemovalIntentKey(intent.OperationID)},
		{Key: blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID), ModRevision: intent.DesiredHeadRevision},
		{Key: projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID), ModRevision: intent.AppliedProjectionRevision},
		{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID)},
		{
			Key:         environmentBlueprintRootKey(intent.EnvironmentID, intent.Claim.RevisionID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
	}
	if ordinary {
		conditions = append(conditions, etcdstore.Condition{
			Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), project.Record.TenantID),
		})
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		{Type: etcdstore.MutationPut, Key: zoneRemovalIntentKey(intent.OperationID), Value: intentValue},
		{Type: etcdstore.MutationPut, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), Value: []byte(task.ID)},
	}
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifyZoneDeletionStartConflict(
			environment, project, pool, addresses, task.OperationID,
			intent.DesiredHeadRevision, intent.AppliedProjectionRevision, ordinary,
		),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := plan.enforceTransactionBounds(zoneRemovalTransactionBudgetValidator(zoneRemovalTransactionBegin)); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func selectedByEnabledComponent(components []componentrecord.Record, zoneID string) bool {
	for _, record := range components {
		if !record.Desired.Enabled {
			continue
		}
		switch record.Desired.Kind {
		case core.ComponentKindIngressCaddy:
			if record.Desired.Config.Caddy != nil && slices.Contains(record.Desired.Config.Caddy.ZoneIDs, zoneID) {
				return true
			}
		case core.ComponentKindEdgeCloudflare:
			if record.Desired.Config.CloudflareTunnel != nil &&
				slices.Contains(record.Desired.Config.CloudflareTunnel.ZoneIDs, zoneID) {
				return true
			}
		}
	}
	return false
}

func classifyZoneDeletionStartConflict(
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	pool etcdstore.Versioned[zonePoolRegistry],
	addresses etcdstore.Versioned[componentAddressRegistry],
	operationID string,
	expectedHeadRevision int64,
	projectionRevision int64,
	hasTenantFence bool,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		expectedValues := 18
		if hasTenantFence {
			expectedValues++
		}
		if len(values) != expectedValues {
			return errs.New(errs.KindInternal, "Zone deletion compare evidence is incomplete")
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
				return errs.New(errs.KindInternal, "Zone deletion collided with durable Task state")
			}
		}
		if values[4] == nil || values[4].ModRevision != pool.Revision {
			return stateConflict("Zone pool registry", environment.Record.ID)
		}
		if keyValueRevision(values[5]) != addresses.Revision {
			return errs.New(errs.KindResourceInUse, "Zone Component address reservations changed")
		}
		if values[6] != nil {
			return errs.New(errs.KindResourceInUse, "Zone deletion is already in progress")
		}
		if values[7] == nil || values[7].ModRevision != environment.Revision {
			return stateConflict("environment", environment.Record.ID)
		}
		if values[8] == nil || values[8].ModRevision != project.Revision {
			return stateConflict("project", project.Record.ID)
		}
		for _, index := range []int{9, 10} {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Zone hierarchy deletion is in progress")
			}
		}
		if hasTenantFence && values[18] != nil {
			return errs.New(errs.KindResourceInUse, "Zone hierarchy deletion is in progress")
		}
		if values[11] != nil || values[14] != nil {
			return errs.New(errs.KindResourceInUse, "Zone removal or Environment mutation is already active")
		}
		if values[12] == nil || values[12].ModRevision != expectedHeadRevision ||
			values[13] == nil || values[13].ModRevision != projectionRevision ||
			values[15] == nil || values[16] == nil || values[17] == nil {
			return errs.New(errs.KindStateConflict, "Zone sealed desired state changed")
		}
		return errs.New(errs.KindStateConflict, "Zone deletion state changed")
	}
}

// HandoffBackingZoneDeletion atomically transfers a fenced backing Zone from
// its running Controller parent to the normal Agent network-removal Task.
func (repository *ZoneRepository) HandoffBackingZoneDeletion(
	ctx context.Context,
	zone etcdstore.Versioned[zonerecord.Record],
	parentTaskID string,
	tombstone etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord],
	intent ZoneRemovalIntent,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if tombstone.Revision <= 0 ||
		recordcodec.ValidateID(ids.KindTask, parentTaskID) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "backing Zone handoff is invalid")
	}
	if err := validateZoneRemovalIntent(intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	projection := etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: intent.DesiredProjection, Revision: intent.DesiredHeadRevision, ReadRevision: zone.ReadRevision,
	}
	selected, err := selectedZoneDeletionRecord(projection, zone, intent.ZoneID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	zone = selected
	if zone.Record.Desired.OwnerKind != core.ZoneOwnerBackingProject {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "backing Zone handoff is invalid")
	}
	currentTombstone := tombstone.Record
	if currentTombstone.TargetKind != deletionrecord.DeletionTargetZone ||
		currentTombstone.TargetID != zone.Record.Desired.ID ||
		currentTombstone.TargetRevision != zone.Revision || currentTombstone.TaskID != parentTaskID ||
		currentTombstone.Phase != deletionrecord.DeletionPhaseHostEffects || currentTombstone.Checkpoint != (deletionrecord.DeletionCheckpoint{}) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "backing Zone cascade fence changed")
	}
	if err := validateZoneRemovalTaskOwner(task, intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskRemove || task.Target != zone.Record.Desired.ID ||
		task.Status != taskjournal.TaskStatusPending ||
		marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.ReplayTarget != nil ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != zone.Record.EnvironmentID ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backing Zone Agent handoff Task is invalid",
		)
	}
	pool, err := repository.getZonePoolRegistry(ctx, intent.EnvironmentID)
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
			"Zone gained a Component address reservation",
		)
	}
	parentResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskjournal.TaskStorageKey(parentTaskID), zoneRemovalIntentKey(intent.OperationID),
		componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID), projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID),
	}, Revision: tombstone.ReadRevision})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if parentResult == nil || len(parentResult.Values) != 5 || parentResult.Values[0] == nil ||
		parentResult.Values[1] == nil || parentResult.Values[2] == nil || parentResult.Values[3] == nil ||
		parentResult.Values[4] == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "backing Zone parent Task is missing")
	}
	parent, err := decodeTaskRecord(parentResult.Values[0].Value)
	if err != nil || parent.ID != parentTaskID || parent.Executor != taskjournal.TaskExecutorController ||
		parent.Status != taskjournal.TaskStatusRunning || parent.Target != zone.Record.Desired.ID ||
		parent.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceBackingZone {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"backing Zone parent Task is not running",
		)
	}
	currentIntent, err := decodeZoneRemovalIntent(parentResult.Values[1].Value)
	headID, headErr := idempotencyrecord.DecodeTaskReference(parentResult.Values[3].Value)
	applied, appliedErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(parentResult.Values[4].Value)
	if err != nil || currentIntent.OperationID != intent.OperationID || currentIntent.ActiveTaskID != parentTaskID ||
		currentIntent.Status != taskjournal.TaskStatusPending || string(parentResult.Values[2].Value) != parentTaskID ||
		headErr != nil || headID != currentIntent.DesiredProjection.RevisionID ||
		parentResult.Values[3].ModRevision != currentIntent.DesiredHeadRevision ||
		appliedErr != nil || parentResult.Values[4].ModRevision != currentIntent.AppliedProjectionRevision ||
		!sameServiceRemovalProjection(applied, currentIntent.AppliedProjection) ||
		intent.Claim.RevisionID != currentIntent.Claim.RevisionID ||
		!sameServiceRemovalProjection(intent.DesiredProjection, currentIntent.DesiredProjection) ||
		!sameServiceRemovalProjection(intent.AppliedProjection, currentIntent.AppliedProjection) ||
		!sameServiceRemovalProjection(intent.CandidateProjection, currentIntent.CandidateProjection) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "backing Zone removal intent changed")
	}
	hierarchy := &HierarchyRepository{store: repository.store}
	publication, err := hierarchy.prepareEnvironmentDirectPublication(
		ctx, intent.Claim,
		EnvironmentDesiredRevisionIdentity{EnvironmentID: intent.EnvironmentID, RevisionID: intent.Claim.RevisionID},
		intent.CandidateProjection,
		idempotencyrecord.IdempotencyMarker{Locator: intent.Claim.Locator, Intent: intent.Claim.Intent},
		intent.DesiredHeadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)
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
	currentTombstone.TaskID = task.ID
	currentTombstone.UpdatedAt = task.CreatedAt
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(currentTombstone)
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
	intentValue, err := encodeZoneRemovalIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		{
			Key:         deletionTombstoneKey(string(deletionrecord.DeletionTargetZone), zone.Record.Desired.ID),
			ModRevision: tombstone.Revision,
		},
		{Key: taskjournal.TaskStorageKey(parentTaskID), ModRevision: parentResult.Values[0].ModRevision},
		{Key: zonePoolRegistryKey(intent.EnvironmentID), ModRevision: pool.Revision},
		{
			Key:         componentAddressRegistryKey(zone.Record.Desired.ID),
			ModRevision: addresses.Revision,
		},
		{Key: zoneRemovalIntentKey(intent.OperationID), ModRevision: parentResult.Values[1].ModRevision},
		{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), ModRevision: parentResult.Values[2].ModRevision},
		{Key: blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID), ModRevision: parentResult.Values[3].ModRevision},
		{Key: projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID), ModRevision: parentResult.Values[4].ModRevision},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), intent.EnvironmentID)},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), zone.Record.Desired.OwnerID)},
		{
			Key:         environmentBlueprintRootKey(intent.EnvironmentID, intent.Claim.RevisionID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type:  etcdstore.MutationPut,
			Key:   deletionTombstoneKey(string(deletionrecord.DeletionTargetZone), zone.Record.Desired.ID),
			Value: tombstoneValue,
		},
		{Type: etcdstore.MutationPut, Key: zoneRemovalIntentKey(intent.OperationID), Value: intentValue},
		{Type: etcdstore.MutationPut, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), Value: []byte(task.ID)},
	}
	initiation, err := newInheritedTaskInitiation(etcdstore.Versioned[TaskRecord]{
		Record: parent, Revision: parentResult.Values[0].ModRevision, ReadRevision: parentResult.ReadRevision,
	}, taskjournal.TaskActorSystem)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		func(_ int64, values []*etcdstore.KeyValue) error {
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

func selectedZoneDeletionRecord(
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	supplied etcdstore.Versioned[zonerecord.Record],
	zoneID string,
) (etcdstore.Versioned[zonerecord.Record], error) {
	if projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		supplied.Revision != projection.Revision || supplied.ReadRevision < supplied.Revision {
		return etcdstore.Versioned[zonerecord.Record]{}, errs.New(errs.KindValidationFailed, "Zone deletion projection is invalid")
	}
	var selected *etcdstore.Versioned[zonerecord.Record]
	for _, desired := range projection.Record.DesiredZones {
		if desired.Desired.ID != zoneID {
			continue
		}
		if selected != nil {
			return etcdstore.Versioned[zonerecord.Record]{}, projectionrecord.CorruptEnvironmentComposeProjection()
		}
		joined, err := joinEnvironmentZone(projection, desired)
		if err != nil {
			return etcdstore.Versioned[zonerecord.Record]{}, err
		}
		selected = &joined
	}
	if selected == nil || selected.Record != supplied.Record {
		return etcdstore.Versioned[zonerecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Zone does not match the selected projection",
		)
	}
	return *selected, nil
}

func validateZoneDeletionHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	zone etcdstore.Versioned[zonerecord.Record],
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || zone.Record.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID {
		return errs.New(errs.KindValidationFailed, "Zone hierarchy ownership or revisions are invalid")
	}
	return nil
}
