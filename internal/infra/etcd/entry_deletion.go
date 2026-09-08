package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginEntryDeletionWithTask atomically fences one Entry, pins its exact
// applied-projection candidate, and publishes the Task that owns finalization.
// The Entry and every immutable generation remain visible until success.
func (repository *EntryRepository) BeginEntryDeletionWithTask(
	ctx context.Context, environment Versioned[EnvironmentRecord], project Versioned[ProjectRecord],
	entry Versioned[EntryRecord],
	projection *Versioned[EnvironmentComposeProjection],
	tombstone DeletionTombstoneRecord, intent EntryRemovalIntent, task TaskRecord, marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateEntryHierarchy(ctx, environment, project, entry.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEntryVersion(entry); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEntryRemovalIntent(intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEntryDeletionProjection(projection, intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEntryRemovalTaskOwner(task, intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if tombstone.TargetKind != DeletionTargetEntry || tombstone.TargetID != entry.Record.Entry.ID ||
		tombstone.TargetRevision != entry.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != entryRemovalTombstonePhase(intent) || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) || intent.TaskID != task.ID ||
		intent.EnvironmentID != environment.Record.ID || intent.EntryID != entry.Record.Entry.ID ||
		intent.EntryRevision != entry.Revision || intent.Status != TaskStatusPending ||
		!intent.CreatedAt.Equal(task.CreatedAt) || intent.TerminalAt != nil || task.Type != TaskRemove ||
		task.Target != entry.Record.Entry.ID || task.Status != TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"entry deletion Task, intent, and tombstone do not match",
		)
	}
	wantReplayTarget := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetEntry, ID: entry.Record.Entry.ID}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"entry deletion marker does not match its Task",
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
	intentValue, err := encodeEntryRemovalIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)
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

	tombstoneKey := deletionTombstoneKey(string(DeletionTargetEntry), entry.Record.Entry.ID)
	domainKeys := []string{
		taskKey(task.ID),
		taskOperationIndexKey(task.OperationID, task.ID),
		taskActiveOperationKey(task.OperationID),
		taskQueueKey(task.Executor, task.ID),
		entryRecordKey(entry.Record.Entry.ID),
		entryOwnerKey(entry.Record.EnvironmentID, entry.Record.Entry.ID),
		entryRemovalIntentKey(task.ID),
		tombstoneKey,
	}
	if projection != nil {
		domainKeys = append(
			domainKeys,
			environmentComposeProjectionKey(environment.Record.ID),
			componentTaskActiveEnvironmentKey(environment.Record.ID),
		)
	}
	fence, ownerRevision, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		domainKeys,
		5,
		entry.Record.Entry.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)
	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: entryRecordKey(entry.Record.Entry.ID), ModRevision: entry.Revision},
		{Key: entryOwnerKey(entry.Record.EnvironmentID, entry.Record.Entry.ID), ModRevision: ownerRevision},
		{Key: entryRemovalIntentKey(task.ID)},
		{Key: tombstoneKey},
	}
	if projection != nil {
		conditions = append(conditions,
			Condition{Key: environmentComposeProjectionKey(environment.Record.ID), ModRevision: projection.Revision},
			Condition{Key: componentTaskActiveEnvironmentKey(environment.Record.ID)},
		)
	}
	conditions = append(conditions, fence.transactionConditions()...)
	scriptConditions, err := prepareEntryScriptAbsence(
		ctx,
		repository.store,
		entry.Record.Entry.ID,
		fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	baseCount := len(conditions)
	conditions = append(conditions, scriptConditions...)
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		{Type: MutationPut, Key: entryRemovalIntentKey(task.ID), Value: intentValue},
		epochMutation,
	}
	if projection != nil {
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: componentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
		})
	}
	taskTenant, err := loadEntryTaskInitiationTenantAtRevision(
		ctx,
		repository.store,
		project,
		fence.readAtRevision(),
	)
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
		classifyEntryScriptAbsenceConflict(
			[]string{entry.Record.Entry.ID},
			baseCount,
			classifyEntryDeletionStartConflict(
				entry,
				projection,
				ownerRevision,
				task.OperationID,
				fence,
			),
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

func validateEntryDeletionProjection(
	projection *Versioned[EnvironmentComposeProjection], intent EntryRemovalIntent,
) error {
	if intent.CurrentProjection == nil {
		if projection != nil {
			return errs.New(errs.KindValidationFailed, "entry deletion projection is unexpected")
		}
		return nil
	}
	if projection == nil || projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		projection.Revision != intent.CurrentProjectionRevision ||
		!sameEntryRemovalProjection(projection.Record, *intent.CurrentProjection) {
		return errs.New(errs.KindStateConflict, "entry applied projection changed before deletion")
	}
	return nil
}

func classifyEntryDeletionStartConflict(
	entry Versioned[EntryRecord], projection *Versioned[EnvironmentComposeProjection],
	ownerRevision int64, operationID string,
	fence environmentMutationFenceEvidence,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		domainCount := 8
		if projection != nil {
			domainCount += 2
		}
		expected := domainCount + len(fence.conditions)
		if len(values) != expected {
			return errs.New(errs.KindInternal, "entry deletion compare evidence is incomplete")
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
				return errs.New(errs.KindInternal, "entry deletion collided with durable Task state")
			}
		}
		if values[4] == nil {
			return errs.New(errs.KindEntryNotFound, "entry was not found")
		}
		if values[4].ModRevision != entry.Revision {
			return stateConflict("entry", entry.Record.Entry.ID)
		}
		if values[5] == nil || string(values[5].Value) != entry.Record.Entry.ID {
			return errs.New(errs.KindInternal, "entry owner index changed or is corrupt")
		}
		if values[5].ModRevision != ownerRevision {
			return stateConflict("entry", entry.Record.Entry.ID)
		}
		if values[6] != nil || values[7] != nil {
			return errs.New(errs.KindResourceInUse, "entry deletion is already in progress")
		}
		position := 8
		if projection != nil {
			if values[position] == nil || values[position].ModRevision != projection.Revision {
				return stateConflict("environment projection", entry.Record.EnvironmentID)
			}
			position++
			if values[position] != nil {
				return errs.New(errs.KindResourceInUse, "environment reconciliation is in progress")
			}
			position++
		}
		if conflict := fence.classifyCAS(values[domainCount:]); conflict != nil {
			return conflict
		}
		return errs.New(errs.KindStateConflict, "entry deletion state changed")
	}
}

func loadEntryTaskInitiationTenantAtRevision(
	ctx context.Context, store hierarchyStore, project Versioned[ProjectRecord], readRevision int64,
) (*Versioned[TenantRecord], error) {
	if project.Record.Kind == ProjectKindBacking {
		return nil, nil
	}
	if project.Record.Kind != ProjectKindTenant || ids.Validate(ids.KindTenant, project.Record.TenantID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "task initiation project ancestry is invalid")
	}
	result, err := store.GetMany(ctx, GetManyRequest{
		Keys: []string{tenantKey(project.Record.TenantID)}, Revision: readRevision,
	})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision != readRevision || len(result.Values) != 1 || result.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "task initiation tenant is missing")
	}
	defer clearKeyValues(result.Values)
	tenant, err := decodeTenant(result.Values[0].Value)
	if err != nil || result.Values[0].Key != tenantKey(project.Record.TenantID) ||
		tenant.ID != project.Record.TenantID {
		return nil, errs.New(errs.KindInternal, "task initiation tenant is corrupt")
	}
	return &Versioned[TenantRecord]{
		Record: tenant, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}
