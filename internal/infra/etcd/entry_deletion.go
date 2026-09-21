package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginEntryDeletionWithTask atomically fences one Entry, pins its exact
// applied-projection candidate, and publishes the Task that owns finalization.
// The Entry and every immutable generation remain visible until success.
func (repository *EntryRepository) BeginEntryDeletionWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	entry etcdstore.Versioned[entryrecord.Record],
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	tombstone deletionrecord.DeletionTombstoneRecord,
	intent environmentchanges.EntryRemovalIntent,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (_ IdempotencyTransactionResult, publicationErr error) {
	if err := entryrecord.ValidateEntryHierarchy(ctx, environment, project, entry.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := entryrecord.ValidateEntryVersion(entry); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := deletionrecord.ValidateDeletionTombstone(tombstone); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := environmentchanges.ValidateEntryRemovalIntent(intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEntryDeletionProjection(projection, intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEntryRemovalTaskOwner(task, intent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if tombstone.TargetKind != deletionrecord.DeletionTargetEntry || tombstone.TargetID != entry.Record.Entry.ID ||
		tombstone.TargetRevision != entry.Revision || tombstone.TaskID != task.ID ||
		tombstone.Phase != entryRemovalTombstonePhase(intent) || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) || intent.TaskID != task.ID ||
		intent.EnvironmentID != environment.Record.ID || intent.EntryID != entry.Record.Entry.ID ||
		intent.EntryRevision != entry.Revision || intent.Status != taskjournal.TaskStatusPending ||
		!intent.CreatedAt.Equal(task.CreatedAt) || intent.TerminalAt != nil || task.Type != taskjournal.TaskRemove ||
		task.Target != entry.Record.Entry.ID || task.Status != taskjournal.TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"entry deletion Task, intent, and tombstone do not match",
		)
	}
	wantReplayTarget := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetEntry,
		ID:   entry.Record.Entry.ID,
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID ||
		marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
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
	if err := ValidateTaskRecord(task); err != nil {
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
	intentValue, err := environmentchanges.EncodeEntryRemovalIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	tombstoneKey := deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEntry), entry.Record.Entry.ID)
	domainKeys := []string{
		taskjournal.TaskStorageKey(task.ID),
		taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
		taskjournal.TaskActiveOperationKey(task.OperationID),
		taskjournal.TaskQueueKey(task.Executor, task.ID),
		entryrecord.RecordKey(entry.Record.Entry.ID),
		entryrecord.EntryOwnerKey(entry.Record.EnvironmentID, entry.Record.Entry.ID),
		environmentchanges.EntryRemovalIntentKey(task.ID),
		tombstoneKey,
	}
	if projection != nil {
		domainKeys = append(
			domainKeys,
			projectionrecord.EnvironmentComposeProjectionStorageKey(environment.Record.ID),
			environmentchanges.ComponentTaskActiveEnvironmentKey(environment.Record.ID),
		)
	}
	fence, ownerRevision, err := repository.LoadEntryMutationFence(
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
	configuration, err := prepareConfigurationTaskPublication(
		ctx, repository.store, task, fence.ReadRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task = configuration.task
	defer configuration.clear()
	defer func() { publicationErr = configuration.finish(ctx, repository.store, publicationErr) }()
	taskValue, err := EncodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		{Key: entryrecord.RecordKey(entry.Record.Entry.ID), ModRevision: entry.Revision},
		{Key: entryrecord.EntryOwnerKey(entry.Record.EnvironmentID, entry.Record.Entry.ID), ModRevision: ownerRevision},
		{Key: environmentchanges.EntryRemovalIntentKey(task.ID)},
		{Key: tombstoneKey},
	}
	if projection != nil {
		conditions = append(
			conditions,
			etcdstore.Condition{
				Key:         projectionrecord.EnvironmentComposeProjectionStorageKey(environment.Record.ID),
				ModRevision: projection.Revision,
			},
			etcdstore.Condition{Key: environmentchanges.ComponentTaskActiveEnvironmentKey(environment.Record.ID)},
		)
	}
	conditions = append(conditions, fence.TransactionConditions()...)
	scriptConditions, err := entryrecord.PrepareEntryScriptAbsence(
		ctx,
		repository.store,
		entry.Record.Entry.ID,
		fence.ReadRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	baseCount := len(conditions)
	conditions = append(conditions, scriptConditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: tombstoneKey, Value: tombstoneValue},
		{Type: etcdstore.MutationPut, Key: environmentchanges.EntryRemovalIntentKey(task.ID), Value: intentValue},
		epochMutation,
	}
	if projection != nil {
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: environmentchanges.ComponentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
		})
	}
	taskTenant, err := loadEntryTaskInitiationTenantAtRevision(
		ctx,
		repository.store,
		project,
		fence.ReadRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	classify := entryrecord.ClassifyEntryScriptAbsenceConflict(
		[]string{entry.Record.Entry.ID},
		baseCount,
		classifyEntryDeletionStartConflict(
			entry,
			projection,
			ownerRevision,
			task.OperationID,
			fence,
		),
	)
	conditions, mutations, classify, err = configuration.bind(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateEntryDeletionProjection(
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	intent environmentchanges.EntryRemovalIntent,
) error {
	if intent.CurrentProjection == nil {
		if projection != nil {
			return errs.New(errs.KindValidationFailed, "entry deletion projection is unexpected")
		}
		return nil
	}
	if projection == nil || projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		projection.Revision != intent.CurrentProjectionRevision ||
		!environmentchanges.SameEntryRemovalProjection(projection.Record, *intent.CurrentProjection) {
		return errs.New(errs.KindStateConflict, "entry applied projection changed before deletion")
	}
	return nil
}

func classifyEntryDeletionStartConflict(
	entry etcdstore.Versioned[entryrecord.Record],
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	ownerRevision int64,
	operationID string,
	fence environmentfence.Evidence,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		domainCount := 8
		if projection != nil {
			domainCount += 2
		}
		expected := domainCount + fence.ConditionCount()
		if len(values) != expected {
			return errs.New(errs.KindInternal, "entry deletion compare evidence is incomplete")
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
				return errs.New(errs.KindInternal, "entry deletion collided with durable Task state")
			}
		}
		if values[4] == nil {
			return errs.New(errs.KindEntryNotFound, "entry was not found")
		}
		if values[4].ModRevision != entry.Revision {
			return recordcodec.StateConflict("entry", entry.Record.Entry.ID)
		}
		if values[5] == nil || string(values[5].Value) != entry.Record.Entry.ID {
			return errs.New(errs.KindInternal, "entry owner index changed or is corrupt")
		}
		if values[5].ModRevision != ownerRevision {
			return recordcodec.StateConflict("entry", entry.Record.Entry.ID)
		}
		if values[6] != nil || values[7] != nil {
			return errs.New(errs.KindResourceInUse, "entry deletion is already in progress")
		}
		position := 8
		if projection != nil {
			if values[position] == nil || values[position].ModRevision != projection.Revision {
				return recordcodec.StateConflict("environment projection", entry.Record.EnvironmentID)
			}
			position++
			if values[position] != nil {
				return errs.New(errs.KindResourceInUse, "environment reconciliation is in progress")
			}
			position++
		}
		if conflict := fence.ClassifyConflict(values[domainCount:]); conflict != nil {
			return conflict
		}
		return errs.New(errs.KindStateConflict, "entry deletion state changed")
	}
}

func loadEntryTaskInitiationTenantAtRevision(
	ctx context.Context,
	store hierarchyStore,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	readRevision int64,
) (*etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	if project.Record.Kind == hierarchyrecord.ProjectKindBacking {
		return nil, nil
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		ids.Validate(ids.KindTenant, project.Record.TenantID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "task initiation project ancestry is invalid")
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{hierarchyrecord.TenantKey(project.Record.TenantID)}, Revision: readRevision,
	})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision != readRevision || len(result.Values) != 1 || result.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "task initiation tenant is missing")
	}
	defer etcdstore.ClearValues(result.Values)
	tenant, err := hierarchyrecord.DecodeTenant(result.Values[0].Value)
	if err != nil || result.Values[0].Key != hierarchyrecord.TenantKey(project.Record.TenantID) ||
		tenant.ID != project.Record.TenantID {
		return nil, errs.New(errs.KindInternal, "task initiation tenant is corrupt")
	}
	return &etcdstore.Versioned[hierarchyrecord.TenantRecord]{
		Record: tenant, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}
