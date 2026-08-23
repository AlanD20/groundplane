package etcd

import (
	"context"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginEntryDeletionWithTask atomically fences one Entry, pins its exact
// applied-projection candidate, and publishes the Task that owns finalization.
// The Entry and every immutable generation remain visible until success.
func (repository *EntryRepository) BeginEntryDeletionWithTask(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	entry Versioned[EntryRecord],
	projection *Versioned[EnvironmentComposeProjection],
	cloudflare *Versioned[ComponentRecord],
	tombstone DeletionTombstoneRecord,
	intent EntryRemovalIntent,
	task TaskRecord,
	marker IdempotencyMarker,
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
	if err := validateEntryDeletionCloudflareComponent(cloudflare, intent); err != nil {
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
			"Entry deletion Task, intent, and tombstone do not match",
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
			"Entry deletion marker does not match its Task",
		)
	}

	owner, err := repository.entryOwnerAtCurrentRevision(ctx, entry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
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
	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: entryRecordKey(entry.Record.Entry.ID), ModRevision: entry.Revision},
		{Key: entryOwnerKey(entry.Record.EnvironmentID, entry.Record.Entry.ID), ModRevision: owner.ModRevision},
		{Key: entryRemovalIntentKey(task.ID)},
		{Key: tombstoneKey},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{
			Key: deletionTombstoneKey(string(DeletionTargetTenant), project.Record.TenantID),
		})
	}
	if projection != nil {
		conditions = append(conditions,
			Condition{Key: environmentComposeProjectionKey(environment.Record.ID), ModRevision: projection.Revision},
			Condition{Key: componentTaskActiveEnvironmentKey(environment.Record.ID)},
		)
	}
	if cloudflare == nil {
		conditions = append(conditions, Condition{
			Key: componentEnvironmentKindKey(environment.Record.ID, core.ComponentKindEdgeCloudflare),
		})
	} else {
		conditions = append(conditions, Condition{
			Key: componentKey(cloudflare.Record.Desired.ID), ModRevision: cloudflare.Revision,
		})
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		{Type: MutationPut, Key: entryRemovalIntentKey(task.ID), Value: intentValue},
	}
	if projection != nil {
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: componentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
		})
	}
	plan, err := newTaskIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyEntryDeletionStartConflict(
			environment, project, entry, projection, cloudflare, owner, task.OperationID,
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

func validateEntryDeletionCloudflareComponent(
	component *Versioned[ComponentRecord],
	intent EntryRemovalIntent,
) error {
	if component == nil {
		return nil
	}
	record := component.Record
	if component.Revision <= 0 || component.ReadRevision < component.Revision ||
		validateComponentRecord(record) != nil || record.Desired.Owner != core.ComponentOwnerEnvironment ||
		record.Desired.OwnerID != intent.EnvironmentID || record.Desired.Kind != core.ComponentKindEdgeCloudflare {
		return errs.New(errs.KindValidationFailed, "Entry deletion Cloudflare Component is invalid")
	}
	if !record.Desired.Enabled {
		return nil
	}
	raw, exists := record.Desired.Config["token_entry_id"]
	var entryID string
	if !exists || json.Unmarshal(raw, &entryID) != nil || ids.Validate(ids.KindEnvEntry, entryID) != nil {
		return errs.New(errs.KindInternal, "enabled Cloudflare Component token reference is invalid")
	}
	if entryID == intent.EntryID {
		return errs.New(errs.KindResourceInUse, "Entry is the enabled Cloudflare Tunnel token")
	}
	return nil
}

func validateEntryDeletionProjection(
	projection *Versioned[EnvironmentComposeProjection],
	intent EntryRemovalIntent,
) error {
	if intent.CurrentProjection == nil {
		if projection != nil {
			return errs.New(errs.KindValidationFailed, "Entry deletion projection is unexpected")
		}
		return nil
	}
	if projection == nil || projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		projection.Revision != intent.CurrentProjectionRevision ||
		!sameEntryRemovalProjection(projection.Record, *intent.CurrentProjection) {
		return errs.New(errs.KindStateConflict, "Entry applied projection changed before deletion")
	}
	return nil
}

func classifyEntryDeletionStartConflict(
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	entry Versioned[EntryRecord],
	projection *Versioned[EnvironmentComposeProjection],
	cloudflare *Versioned[ComponentRecord],
	owner *KeyValue,
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		expected := 12
		if project.Record.TenantID != "" {
			expected++
		}
		if projection != nil {
			expected += 2
		}
		expected++
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Entry deletion compare evidence is incomplete")
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
				return errs.New(errs.KindInternal, "Entry deletion collided with durable Task state")
			}
		}
		if values[4] == nil {
			return errs.New(errs.KindEntryNotFound, "Entry was not found")
		}
		if values[4].ModRevision != entry.Revision {
			return stateConflict("entry", entry.Record.Entry.ID)
		}
		if values[5] == nil || string(values[5].Value) != entry.Record.Entry.ID {
			return errs.New(errs.KindInternal, "Entry owner index changed or is corrupt")
		}
		if values[5].ModRevision != owner.ModRevision {
			return stateConflict("entry", entry.Record.Entry.ID)
		}
		if values[6] != nil || values[7] != nil {
			return errs.New(errs.KindResourceInUse, "Entry deletion is already in progress")
		}
		if values[8] == nil {
			return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
		}
		if values[8].ModRevision != environment.Revision {
			return stateConflict("environment", environment.Record.ID)
		}
		if values[9] == nil {
			return errs.New(errs.KindProjectNotFound, "project was not found")
		}
		if values[9].ModRevision != project.Revision {
			return stateConflict("project", project.Record.ID)
		}
		position := 10
		hierarchyFenceCount := 2
		if project.Record.TenantID != "" {
			hierarchyFenceCount++
		}
		for index := position; index < position+hierarchyFenceCount; index++ {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Entry hierarchy deletion is in progress")
			}
		}
		position += hierarchyFenceCount
		if projection != nil {
			if values[position] == nil || values[position].ModRevision != projection.Revision {
				return stateConflict("Environment projection", environment.Record.ID)
			}
			position++
			if values[position] != nil {
				return errs.New(errs.KindResourceInUse, "Environment reconciliation is in progress")
			}
			position++
		}
		if cloudflare == nil {
			if values[position] != nil {
				return stateConflict("Cloudflare Component", environment.Record.ID)
			}
		} else if values[position] == nil || values[position].ModRevision != cloudflare.Revision {
			return stateConflict("Cloudflare Component", cloudflare.Record.Desired.ID)
		}
		return errs.New(errs.KindStateConflict, "Entry deletion state changed")
	}
}
