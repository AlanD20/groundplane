package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginRunnerRemovalWithTask atomically fences the Runner and publishes the
// finalizer Task. The record and every allocation stay durable until a
// successful terminal acknowledgement.
func (repository *RunnerRepository) BeginRunnerRemovalWithTask(
	ctx context.Context,
	current etcdstore.Versioned[runnerrecord.RunnerRecord],
	tombstone deletionrecord.DeletionTombstoneRecord,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := runnerrecord.ValidateRunnerRecord(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision ||
		current.Record.ProvisioningState == runnerrecord.RunnerProvisioningProvisioning {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "runner is not available for removal")
	}
	if err := validateRunnerDeletionTask(current, tombstone, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRunnerDeleteMarker(current.Record.Desired, task, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	sourceResult, err := repository.store.Get(ctx, taskjournal.TaskStorageKey(current.Record.CreateTaskID))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if sourceResult == nil || sourceResult.Entry == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "runner create task is missing")
	}
	source, err := decodeTaskRecord(sourceResult.Entry.Value)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	applies, sourceErr := taskOwnsRunnerCreation(source)
	if sourceErr != nil || !applies || !runnerTerminal(source.Status) || source.Target != current.Record.Desired.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "runner create task is not terminal")
	}
	parents, err := repository.resolveRunnerParents(ctx, current.Record.Desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	allocation, err := repository.readRunnerAllocationEvidence(ctx, current.Record, current.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	runtimeEvidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{runnerRuntimeOwnershipKey(current.Record.Desired.ID)}, Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if runtimeEvidence == nil || len(runtimeEvidence.Values) != 1 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"runner runtime ownership evidence is incomplete",
		)
	}
	if runtimeEvidence.Values[0] != nil {
		ownership, decodeErr := runnerrecord.DecodeRunnerRuntimeOwnership(runtimeEvidence.Values[0].Value)
		if decodeErr != nil || ownership.RunnerID != current.Record.Desired.ID ||
			ownership.RuntimeEpoch != current.Record.RuntimeEpoch || current.Record.ContainerID == "" {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindInternal,
				"runner runtime ownership does not match lifecycle",
			)
		}
	} else if current.Record.ContainerID != "" {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "runner lifecycle lost runtime ownership")
	}
	cleanupLifecycle, err := runnerrecord.TakeRunnerRuntimeCleanupOwnership(current.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	lifecycleValue, err := runnerrecord.EncodeRunnerLifecycleRecord(cleanupLifecycle.RunnerLifecycleRecord)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(lifecycleValue)
	intent := RunnerRemovalIntent{
		RunnerID: current.Record.Desired.ID, TaskID: task.ID,
		OwnerKind: current.Record.Desired.OwnerKind, OwnerID: current.Record.Desired.OwnerID,
		TenantID: current.Record.Desired.TenantID, Allocation: current.Record.Allocation,
		CreatedAt: task.CreatedAt,
	}
	task = bindRunnerTaskMarker(task, marker)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
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
	tombstoneValue, err := encodeRunnerDeletionTombstone(tombstone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(tombstoneValue)
	intentValue, err := encodeRunnerRemovalIntent(intent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(intentValue)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		{Key: runnerKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(current.Record.Desired.ID), ModRevision: current.Record.LifecycleRevision},
		{
			Key:         runnerRuntimeOwnershipKey(current.Record.Desired.ID),
			ModRevision: keyValueRevision(runtimeEvidence.Values[0]),
		},
		{
			Key: runnerOwnerKey(
				current.Record.Desired.OwnerKind,
				current.Record.Desired.OwnerID,
				current.Record.Desired.ID,
			),
			ModRevision: allocation.owner.ModRevision,
		},
		{
			Key:         runnerTenantSlugKey(current.Record.Desired.TenantID, current.Record.Desired.Slug),
			ModRevision: allocation.slug.ModRevision,
		},
		{Key: runnerTenantQuotaKey(current.Record.Desired.TenantID), ModRevision: allocation.quota.ModRevision},
		{Key: runnerHostSlotKey(current.Record.Allocation.Slot), ModRevision: allocation.host.ModRevision},
		{Key: systemPoolRegistryKey, ModRevision: allocation.system.ModRevision},
		{Key: taskjournal.TaskStorageKey(source.ID), ModRevision: sourceResult.Entry.ModRevision},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), current.Record.Desired.ID)},
		{Key: runnerRemovalIntentKey(current.Record.Desired.ID)},
		{Key: hierarchyrecord.TenantKey(current.Record.Desired.TenantID), ModRevision: parents.tenant.Revision},
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), current.Record.Desired.TenantID)},
	}
	if current.Record.Desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		conditions = append(conditions,
			etcdstore.Condition{Key: hierarchyrecord.ProjectKey(current.Record.Desired.OwnerID), ModRevision: parents.project.Revision},
			etcdstore.Condition{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), current.Record.Desired.OwnerID)},
		)
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type: etcdstore.MutationPut, Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), current.Record.Desired.ID),
			Value: tombstoneValue,
		},
		{Type: etcdstore.MutationPut, Key: runnerRemovalIntentKey(current.Record.Desired.ID), Value: intentValue},
		{Type: etcdstore.MutationPut, Key: runnerLifecycleKey(current.Record.Desired.ID), Value: lifecycleValue},
	}
	classifier := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "runner removal compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, decodeErr := idempotencyrecord.DecodeTaskReference(values[2].Value)
			if decodeErr != nil {
				return decodeErr
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				task.OperationID,
				activeTaskID,
			)
		}
		if values[13] != nil || values[14] != nil {
			return errs.New(errs.KindResourceInUse, "runner removal is already in progress")
		}
		return stateConflict("runner", current.Record.Desired.ID)
	}
	initiation, err := newRunnerTaskInitiation(current.Record.Desired, parents, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateRunnerDeletionTask(
	current etcdstore.Versioned[runnerrecord.RunnerRecord],
	tombstone deletionrecord.DeletionTombstoneRecord,
	task TaskRecord,
) error {
	evidence, evidenceErr := decodeRunnerRemovalTaskEvidence(task)
	if validateRunnerDeletionTombstone(tombstone) != nil ||
		tombstone.TargetID != current.Record.Desired.ID || tombstone.TargetRevision != current.Revision ||
		tombstone.TaskID != task.ID || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) ||
		task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskRemove ||
		task.Target != current.Record.Desired.ID || task.Status != taskjournal.TaskStatusPending ||
		evidenceErr != nil || !evidence.matchesRecord(current.Record) {
		return errs.New(errs.KindValidationFailed, "runner removal task and tombstone do not match")
	}
	return nil
}
