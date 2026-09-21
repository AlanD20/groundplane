package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// HandoffBackingZoneDeletion atomically transfers a fenced backing Zone from
// its running Controller parent to the normal Agent network-removal Task.
func (repository *ZoneRepository) HandoffBackingZoneDeletion(
	ctx context.Context,
	zone etcdstore.Versioned[zonerecord.Record],
	parentTaskID string,
	tombstone etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord],
	intent environmentchanges.ZoneRemovalIntent,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if tombstone.Revision <= 0 ||
		recordcodec.ValidateID(ids.KindTask, parentTaskID) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "backing Zone handoff is invalid")
	}
	if err := environmentchanges.ValidateZoneRemovalIntent(intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	projection := etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: intent.DesiredProjection, Revision: intent.DesiredHeadRevision, ReadRevision: zone.ReadRevision,
	}
	selected, err := environmentqueries.SelectZoneForDeletion(projection, zone, intent.ZoneID)
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
	if task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskRemove ||
		task.Target != zone.Record.Desired.ID ||
		task.Status != taskjournal.TaskStatusPending ||
		marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID ||
		marker.ReplayTarget != nil ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != zone.Record.EnvironmentID ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
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
	addresses, err := networkreservations.GetComponentAddressRegistry(ctx, repository.store, zone.Record)
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
		taskjournal.TaskStorageKey(parentTaskID), environmentchanges.ZoneRemovalIntentKey(intent.OperationID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
		blueprints.EnvironmentBlueprintHeadKey(
			intent.EnvironmentID,
		), projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID),
	}, Revision: tombstone.ReadRevision})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if parentResult == nil || len(parentResult.Values) != 5 || parentResult.Values[0] == nil ||
		parentResult.Values[1] == nil || parentResult.Values[2] == nil || parentResult.Values[3] == nil ||
		parentResult.Values[4] == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "backing Zone parent Task is missing")
	}
	parent, err := DecodeTaskRecord(parentResult.Values[0].Value)
	if err != nil || parent.ID != parentTaskID || parent.Executor != taskjournal.TaskExecutorController ||
		parent.Status != taskjournal.TaskStatusRunning || parent.Target != zone.Record.Desired.ID ||
		parent.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceBackingZone {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"backing Zone parent Task is not running",
		)
	}
	currentIntent, err := environmentchanges.DecodeZoneRemovalIntent(parentResult.Values[1].Value)
	headID, headErr := idempotencyrecord.DecodeTaskReference(parentResult.Values[3].Value)
	applied, appliedErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(parentResult.Values[4].Value)
	if err != nil || currentIntent.OperationID != intent.OperationID || currentIntent.ActiveTaskID != parentTaskID ||
		currentIntent.Status != taskjournal.TaskStatusPending || string(parentResult.Values[2].Value) != parentTaskID ||
		headErr != nil || headID != currentIntent.DesiredProjection.RevisionID ||
		parentResult.Values[3].ModRevision != currentIntent.DesiredHeadRevision ||
		appliedErr != nil || parentResult.Values[4].ModRevision != currentIntent.AppliedProjectionRevision ||
		!environmentchanges.SameServiceRemovalProjection(applied, currentIntent.AppliedProjection) ||
		intent.Claim.RevisionID != currentIntent.Claim.RevisionID ||
		!environmentchanges.SameServiceRemovalProjection(intent.DesiredProjection, currentIntent.DesiredProjection) ||
		!environmentchanges.SameServiceRemovalProjection(intent.AppliedProjection, currentIntent.AppliedProjection) ||
		!environmentchanges.SameServiceRemovalProjection(
			intent.CandidateProjection,
			currentIntent.CandidateProjection,
		) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "backing Zone removal intent changed")
	}
	hierarchy := composeHierarchyRepository(repository.store)
	publication, err := hierarchy.prepareEnvironmentDirectPublication(
		ctx,
		intent.Claim,
		blueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: intent.EnvironmentID,
			RevisionID:    intent.Claim.RevisionID,
		},
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
	if err := ValidateTaskRecord(task); err != nil {
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
	taskValue, err := EncodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	intentValue, err := environmentchanges.EncodeZoneRemovalIntent(intent)
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
			Key:         deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), zone.Record.Desired.ID),
			ModRevision: tombstone.Revision,
		},
		{Key: taskjournal.TaskStorageKey(parentTaskID), ModRevision: parentResult.Values[0].ModRevision},
		{Key: networkreservations.ZonePoolRegistryKey(intent.EnvironmentID), ModRevision: pool.Revision},
		{
			Key:         networkreservations.ComponentAddressRegistryKey(zone.Record.Desired.ID),
			ModRevision: addresses.Revision,
		},
		{
			Key:         environmentchanges.ZoneRemovalIntentKey(intent.OperationID),
			ModRevision: parentResult.Values[1].ModRevision,
		},
		{
			Key:         environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
			ModRevision: parentResult.Values[2].ModRevision,
		},
		{
			Key:         blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
			ModRevision: parentResult.Values[3].ModRevision,
		},
		{
			Key:         projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID),
			ModRevision: parentResult.Values[4].ModRevision,
		},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), intent.EnvironmentID)},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), zone.Record.Desired.OwnerID)},
		{
			Key:         blueprints.EnvironmentBlueprintRootKey(intent.EnvironmentID, intent.Claim.RevisionID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type:  etcdstore.MutationPut,
			Key:   deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), zone.Record.Desired.ID),
			Value: tombstoneValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   environmentchanges.ZoneRemovalIntentKey(intent.OperationID),
			Value: intentValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
			Value: []byte(task.ID),
		},
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
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}
