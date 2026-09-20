package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	TaskResourceRunner                  = "runner"
	RunnerRegistrationTokenPresentParam = "registration_token_present"
	RunnerTenantIDParam                 = "runner_tenant_id"
	RunnerOwnerKindParam                = "runner_owner_kind"
	RunnerOwnerIDParam                  = "runner_owner_id"
	RunnerHostSlotParam                 = "runner_host_slot"
	RunnerNetworkCIDRParam              = "runner_network_cidr"
	runnerRemovalIntentPrefix           = "/v1/runtime/runner-removal-intents/"
)

type runnerRemovalTaskEvidence struct {
	tenantID    string
	ownerKind   RunnerOwnerKind
	ownerID     string
	hostSlot    uint32
	networkCIDR string
}

// RunnerRemovalTaskParams encodes the canonical durable input for one Runner
// removal Task. Publication and validation must share this representation.
func RunnerRemovalTaskParams(record RunnerRecord) map[string]string {
	return map[string]string{
		TaskResourceKindParam:  TaskResourceRunner,
		RunnerTenantIDParam:    record.Desired.TenantID,
		RunnerOwnerKindParam:   string(record.Desired.OwnerKind),
		RunnerOwnerIDParam:     record.Desired.OwnerID,
		RunnerHostSlotParam:    runnerHostSlotSegment(record.Allocation.Slot),
		RunnerNetworkCIDRParam: record.Allocation.NetworkCIDR,
	}
}

func decodeRunnerRemovalTaskEvidence(task TaskRecord) (runnerRemovalTaskEvidence, error) {
	if task.Executor != TaskExecutorController || task.Type != TaskRemove ||
		ids.Validate(ids.KindRunner, task.Target) != nil || len(task.Params) != 6 ||
		task.Params[TaskResourceKindParam] != TaskResourceRunner ||
		ids.Validate(ids.KindTenant, task.Params[RunnerTenantIDParam]) != nil {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	ownerKind := RunnerOwnerKind(task.Params[RunnerOwnerKindParam])
	ownerID := task.Params[RunnerOwnerIDParam]
	if (ownerKind == RunnerOwnerTenant &&
		(ids.Validate(ids.KindTenant, ownerID) != nil || ownerID != task.Params[RunnerTenantIDParam])) ||
		(ownerKind == RunnerOwnerProject && ids.Validate(ids.KindProject, ownerID) != nil) ||
		(ownerKind != RunnerOwnerTenant && ownerKind != RunnerOwnerProject) {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	expectedOwner, err := runnerTaskOwner(RunnerDesiredRecord{
		ID: task.Target, OwnerKind: ownerKind, OwnerID: ownerID, TenantID: task.Params[RunnerTenantIDParam],
	})
	if err != nil || task.Owner != expectedOwner {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid owner")
	}
	hostSlot, err := parseRunnerHostSlotSegment(task.Params[RunnerHostSlotParam])
	if err != nil {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	if _, err := runnerAllocationPrefix(task.Params[RunnerNetworkCIDRParam]); err != nil {
		return runnerRemovalTaskEvidence{}, errs.New(errs.KindInternal, "runner removal task has invalid durable input")
	}
	return runnerRemovalTaskEvidence{
		tenantID: task.Params[RunnerTenantIDParam], ownerKind: ownerKind, ownerID: ownerID,
		hostSlot: hostSlot, networkCIDR: task.Params[RunnerNetworkCIDRParam],
	}, nil
}

func (evidence runnerRemovalTaskEvidence) matchesRecord(record RunnerRecord) bool {
	return evidence.tenantID == record.Desired.TenantID &&
		evidence.ownerKind == record.Desired.OwnerKind && evidence.ownerID == record.Desired.OwnerID &&
		evidence.hostSlot == record.Allocation.Slot && evidence.networkCIDR == record.Allocation.NetworkCIDR
}

func (evidence runnerRemovalTaskEvidence) matchesIntent(intent RunnerRemovalIntent) bool {
	return evidence.tenantID == intent.TenantID && evidence.ownerKind == intent.OwnerKind &&
		evidence.ownerID == intent.OwnerID && evidence.hostSlot == intent.Allocation.Slot &&
		evidence.networkCIDR == intent.Allocation.NetworkCIDR
}

type RunnerRemovalIntent struct {
	RunnerID   string                                      `json:"runner_id"`
	TaskID     string                                      `json:"task_id"`
	OwnerKind  RunnerOwnerKind                             `json:"owner_kind"`
	OwnerID    string                                      `json:"owner_id"`
	TenantID   string                                      `json:"tenant_id"`
	Allocation runnerallocation.RunnerHostAllocationRecord `json:"allocation"`
	CreatedAt  time.Time                                   `json:"created_at"`
}

func runnerRemovalIntentKey(runnerID string) string {
	return runnerRemovalIntentPrefix + runnerID
}

func validateRunnerRemovalIntent(intent RunnerRemovalIntent) error {
	desired := RunnerDesiredRecord{
		ID: intent.RunnerID, OwnerKind: intent.OwnerKind, OwnerID: intent.OwnerID, TenantID: intent.TenantID,
	}
	if validateRunnerOwnership(desired) != nil || ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		intent.Allocation.Validate() != nil || !validMarkerTime(intent.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "runner removal intent is invalid")
	}
	return nil
}

func encodeRunnerRemovalIntent(intent RunnerRemovalIntent) ([]byte, error) {
	if err := validateRunnerRemovalIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_removal_intent", intent)
}

func decodeRunnerRemovalIntent(value []byte) (RunnerRemovalIntent, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return RunnerRemovalIntent{}, errs.New(errs.KindInternal, "runner removal intent is corrupt")
	}
	intent, err := recordcodec.Decode[RunnerRemovalIntent](value, "runner_removal_intent")
	if err != nil || validateRunnerRemovalIntent(intent) != nil {
		return RunnerRemovalIntent{}, errs.New(errs.KindInternal, "runner removal intent is corrupt")
	}
	return intent, nil
}

func validateRunnerDeletionTombstone(record DeletionTombstoneRecord) error {
	if record.TargetKind != DeletionTargetRunner || ids.Validate(ids.KindRunner, record.TargetID) != nil ||
		record.TargetRevision <= 0 || ids.Validate(ids.KindTask, record.TaskID) != nil ||
		record.Phase != DeletionPhaseFinalizing || record.Checkpoint != (DeletionCheckpoint{}) ||
		!validMarkerTime(record.CreatedAt) || !validMarkerTime(record.UpdatedAt) ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "runner deletion tombstone is invalid")
	}
	return nil
}

func encodeRunnerDeletionTombstone(record DeletionTombstoneRecord) ([]byte, error) {
	if err := validateRunnerDeletionTombstone(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("deletion-tombstone", record)
}

func decodeRunnerDeletionTombstone(value []byte) (DeletionTombstoneRecord, error) {
	if len(value) > maximumRunnerPersistenceBytes {
		return DeletionTombstoneRecord{}, errs.New(errs.KindInternal, "runner deletion tombstone is corrupt")
	}
	record, err := recordcodec.Decode[DeletionTombstoneRecord](value, "deletion-tombstone")
	if err != nil || validateRunnerDeletionTombstone(record) != nil {
		return DeletionTombstoneRecord{}, errs.New(errs.KindInternal, "runner deletion tombstone is corrupt")
	}
	return record, nil
}

type runnerAllocationEvidence struct {
	owner  *etcdstore.KeyValue
	slug   *etcdstore.KeyValue
	quota  *etcdstore.KeyValue
	host   *etcdstore.KeyValue
	system *etcdstore.KeyValue
}

func (repository *RunnerRepository) readRunnerAllocationEvidence(
	ctx context.Context,
	record RunnerRecord,
	revision int64,
) (runnerAllocationEvidence, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID),
			runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug),
			runnerTenantQuotaKey(record.Desired.TenantID),
			runnerHostSlotKey(record.Allocation.Slot),
			systemPoolRegistryKey,
		},
		Revision: revision,
	})
	if err != nil {
		return runnerAllocationEvidence{}, err
	}
	if result == nil || len(result.Values) != 5 {
		return runnerAllocationEvidence{}, errs.New(errs.KindInternal, "runner allocation evidence is incomplete")
	}
	evidence := runnerAllocationEvidence{
		owner:  result.Values[0],
		slug:   result.Values[1],
		quota:  result.Values[2],
		host:   result.Values[3],
		system: result.Values[4],
	}
	if err := runnerAllocationEvidenceOwns(record, evidence); err != nil {
		return runnerAllocationEvidence{}, err
	}
	return evidence, nil
}

func runnerAllocationEvidenceOwns(record RunnerRecord, evidence runnerAllocationEvidence) error {
	if evidence.owner == nil || string(evidence.owner.Value) != record.Desired.ID ||
		evidence.slug == nil || string(evidence.slug.Value) != record.Desired.ID ||
		evidence.quota == nil || evidence.host == nil || evidence.system == nil {
		return errs.New(errs.KindInternal, "runner allocation evidence is incomplete")
	}
	quota, err := decodeRunnerTenantQuota(evidence.quota.Value)
	if err != nil || quota.Validate() != nil {
		return corruptRunnerTenantQuota()
	}
	index := sortSearchRunnerID(quota.RunnerIDs, record.Desired.ID)
	if index >= len(quota.RunnerIDs) || quota.RunnerIDs[index] != record.Desired.ID {
		return errs.New(errs.KindInternal, "runner tenant quota lost its owner")
	}
	host, err := decodeRunnerHostSlotRecord(evidence.host.Value)
	if err != nil || host.Slot != record.Allocation.Slot || host.RunnerID != record.Desired.ID {
		return corruptRunnerHostSlotRecord()
	}
	system, err := decodeSystemPoolRegistry(evidence.system.Value)
	if err != nil ||
		system.Reservations[runnerallocation.RunnerReservationOwner(record.Desired.ID)] != record.Allocation.NetworkCIDR {
		return corruptSystemPoolRegistry()
	}
	return nil
}

func sortSearchRunnerID(values []string, id string) int {
	left, right := 0, len(values)
	for left < right {
		middle := int(uint(left+right) >> 1)
		if values[middle] < id {
			left = middle + 1
		} else {
			right = middle
		}
	}
	return left
}

// BeginRunnerRemovalWithTask atomically fences the Runner and publishes the
// finalizer Task. The record and every allocation stay durable until a
// successful terminal acknowledgement.
func (repository *RunnerRepository) BeginRunnerRemovalWithTask(
	ctx context.Context,
	current Versioned[RunnerRecord],
	tombstone DeletionTombstoneRecord,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRunnerRecord(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision ||
		current.Record.ProvisioningState == RunnerProvisioningProvisioning {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "runner is not available for removal")
	}
	if err := validateRunnerDeletionTask(current, tombstone, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRunnerDeleteMarker(current.Record.Desired, task, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	sourceResult, err := repository.store.Get(ctx, taskKey(current.Record.CreateTaskID))
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
		ownership, decodeErr := decodeRunnerRuntimeOwnership(runtimeEvidence.Values[0].Value)
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
	cleanupLifecycle, err := TakeRunnerRuntimeCleanupOwnership(current.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	lifecycleValue, err := encodeRunnerLifecycleRecord(cleanupLifecycle.RunnerLifecycleRecord)
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
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
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
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
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
		{Key: taskKey(source.ID), ModRevision: sourceResult.Entry.ModRevision},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), current.Record.Desired.ID)},
		{Key: runnerRemovalIntentKey(current.Record.Desired.ID)},
		{Key: hierarchyrecord.TenantKey(current.Record.Desired.TenantID), ModRevision: parents.tenant.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetTenant), current.Record.Desired.TenantID)},
	}
	if current.Record.Desired.OwnerKind == RunnerOwnerProject {
		conditions = append(conditions,
			etcdstore.Condition{Key: hierarchyrecord.ProjectKey(current.Record.Desired.OwnerID), ModRevision: parents.project.Revision},
			etcdstore.Condition{Key: deletionTombstoneKey(string(DeletionTargetProject), current.Record.Desired.OwnerID)},
		)
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type: etcdstore.MutationPut, Key: deletionTombstoneKey(string(DeletionTargetRunner), current.Record.Desired.ID),
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
			activeTaskID, decodeErr := decodeTaskReference(values[2].Value)
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
	initiation, err := newRunnerTaskInitiation(current.Record.Desired, parents, TaskActorOperator)
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
	current Versioned[RunnerRecord],
	tombstone DeletionTombstoneRecord,
	task TaskRecord,
) error {
	evidence, evidenceErr := decodeRunnerRemovalTaskEvidence(task)
	if validateRunnerDeletionTombstone(tombstone) != nil ||
		tombstone.TargetID != current.Record.Desired.ID || tombstone.TargetRevision != current.Revision ||
		tombstone.TaskID != task.ID || !tombstone.CreatedAt.Equal(task.CreatedAt) ||
		!tombstone.UpdatedAt.Equal(tombstone.CreatedAt) ||
		task.Executor != TaskExecutorController || task.Type != TaskRemove ||
		task.Target != current.Record.Desired.ID || task.Status != TaskStatusPending ||
		evidenceErr != nil || !evidence.matchesRecord(current.Record) {
		return errs.New(errs.KindValidationFailed, "runner removal task and tombstone do not match")
	}
	return nil
}

type runnerTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareRunnerTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (runnerTaskChange, error) {
	applies, err := taskOwnsRunner(task)
	if err != nil || !applies {
		return runnerTaskChange{}, err
	}
	if task.Type == TaskCreate {
		return repository.prepareRunnerCreationAcknowledgement(ctx, task, terminalStatus, revision)
	}
	return repository.prepareRunnerRemovalAcknowledgement(ctx, task, terminalStatus, revision)
}

func (repository *TaskRepository) prepareRunnerCreationAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (runnerTaskChange, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerKey(task.Target), runnerLifecycleKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetRunner), task.Target),
			runnerRuntimeOwnershipKey(task.Target), runnerReadinessProofKey(task.ID),
		},
		Revision: revision,
	})
	if err != nil {
		return runnerTaskChange{}, err
	}
	if result == nil || len(result.Values) != 5 || result.Values[0] == nil || result.Values[1] == nil ||
		result.Values[2] != nil {
		return runnerTaskChange{}, errs.New(errs.KindStateConflict, "runner provisioning state does not match its task")
	}
	record, err := decodeRunnerAggregate(result.Values[0], result.Values[1])
	if err != nil {
		return runnerTaskChange{}, err
	}
	proofValue := result.Values[4]
	if terminalStatus == TaskStatusCompleted || proofValue != nil {
		if result.Values[3] == nil || proofValue == nil || record.ContainerID == "" {
			return runnerTaskChange{}, errs.New(errs.KindStateConflict, "exact runner readiness proof is required")
		}
		ownership, ownershipErr := decodeRunnerRuntimeOwnership(result.Values[3].Value)
		proof, proofErr := decodeRunnerReadinessProof(proofValue.Value)
		if ownershipErr != nil || proofErr != nil {
			return runnerTaskChange{}, errs.New(errs.KindInternal, "runner readiness evidence is corrupt")
		}
		if ownership.RunnerID != record.Desired.ID || ownership.RuntimeEpoch != record.RuntimeEpoch ||
			!runnerReadinessProofMatches(proof, task, record, result.Values[3]) {
			return runnerTaskChange{}, errs.New(
				errs.KindStateConflict,
				"runner readiness proof does not match its lifecycle",
			)
		}
	}
	replacement, err := CompleteRunnerProvisioning(record, task.ID, terminalStatus == TaskStatusCompleted)
	if err != nil {
		return runnerTaskChange{}, err
	}
	value, err := encodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
	if err != nil {
		return runnerTaskChange{}, err
	}
	allocation, err := (&RunnerRepository{store: repository.store}).readRunnerAllocationEvidence(
		ctx, record, revision,
	)
	if err != nil {
		clear(value)
		return runnerTaskChange{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: runnerKey(task.Target), ModRevision: result.Values[0].ModRevision},
		{Key: runnerLifecycleKey(task.Target), ModRevision: result.Values[1].ModRevision},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), task.Target)},
		{Key: runnerReadinessProofKey(task.ID), ModRevision: keyValueRevision(proofValue)},
		{
			Key:         runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID),
			ModRevision: allocation.owner.ModRevision,
		},
		{
			Key:         runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug),
			ModRevision: allocation.slug.ModRevision,
		},
		{Key: runnerTenantQuotaKey(record.Desired.TenantID), ModRevision: allocation.quota.ModRevision},
		{Key: runnerHostSlotKey(record.Allocation.Slot), ModRevision: allocation.host.ModRevision},
		{Key: systemPoolRegistryKey, ModRevision: allocation.system.ModRevision},
	}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: runnerLifecycleKey(task.Target), Value: value}}
	if proofValue != nil {
		conditions = append(conditions, etcdstore.Condition{
			Key: runnerRuntimeOwnershipKey(task.Target), ModRevision: result.Values[3].ModRevision,
		})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: runnerReadinessProofKey(task.ID)})
	}
	return runnerTaskChange{
		applies:    true,
		conditions: conditions,
		mutations:  mutations,
		values:     [][]byte{value},
	}, nil
}

func (repository *TaskRepository) prepareRunnerRemovalAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (runnerTaskChange, error) {
	base, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerKey(task.Target),
			runnerLifecycleKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetRunner), task.Target),
			runnerRemovalIntentKey(task.Target),
			runnerObservationKey(task.Target),
			runnerRuntimeOwnershipKey(task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return runnerTaskChange{}, err
	}
	if base == nil || len(base.Values) != 6 || base.Values[0] == nil || base.Values[1] == nil ||
		base.Values[2] == nil || base.Values[3] == nil {
		return runnerTaskChange{}, errs.New(errs.KindInternal, "runner removal state is inconsistent")
	}
	record, err := decodeRunnerAggregate(base.Values[0], base.Values[1])
	if err != nil {
		return runnerTaskChange{}, err
	}
	tombstone, err := decodeRunnerDeletionTombstone(base.Values[2].Value)
	if err != nil || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != base.Values[0].ModRevision || tombstone.TaskID != task.ID {
		return runnerTaskChange{}, errs.New(errs.KindStateConflict, "runner deletion tombstone does not match its task")
	}
	intent, err := decodeRunnerRemovalIntent(base.Values[3].Value)
	evidence, evidenceErr := decodeRunnerRemovalTaskEvidence(task)
	if err != nil || evidenceErr != nil || !runnerIntentMatchesRecord(intent, record, task.ID) ||
		!evidence.matchesRecord(record) || !evidence.matchesIntent(intent) {
		return runnerTaskChange{}, errs.New(errs.KindStateConflict, "runner removal intent does not match its task")
	}
	allocation, err := (&RunnerRepository{store: repository.store}).readRunnerAllocationEvidence(ctx, record, revision)
	if err != nil {
		return runnerTaskChange{}, err
	}
	change := runnerTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: runnerKey(task.Target), ModRevision: base.Values[0].ModRevision},
			{Key: runnerLifecycleKey(task.Target), ModRevision: base.Values[1].ModRevision},
			{
				Key:         deletionTombstoneKey(string(DeletionTargetRunner), task.Target),
				ModRevision: base.Values[2].ModRevision,
			},
			{Key: runnerRemovalIntentKey(task.Target), ModRevision: base.Values[3].ModRevision},
			{Key: runnerObservationKey(task.Target), ModRevision: keyValueRevision(base.Values[4])},
			{Key: runnerRuntimeOwnershipKey(task.Target), ModRevision: keyValueRevision(base.Values[5])},
			{
				Key:         runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, task.Target),
				ModRevision: allocation.owner.ModRevision,
			},
			{
				Key:         runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug),
				ModRevision: allocation.slug.ModRevision,
			},
			{Key: runnerTenantQuotaKey(record.Desired.TenantID), ModRevision: allocation.quota.ModRevision},
			{Key: runnerHostSlotKey(record.Allocation.Slot), ModRevision: allocation.host.ModRevision},
			{Key: systemPoolRegistryKey, ModRevision: allocation.system.ModRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetRunner), task.Target)},
			{Type: etcdstore.MutationDelete, Key: runnerRemovalIntentKey(task.Target)},
		},
	}
	if terminalStatus != TaskStatusCompleted {
		return change, nil
	}
	if base.Values[5] != nil {
		return runnerTaskChange{}, errs.New(errs.KindResourceInUse, "runner runtime cleanup proof is required")
	}
	quota, err := decodeRunnerTenantQuota(allocation.quota.Value)
	if err != nil {
		return runnerTaskChange{}, corruptRunnerTenantQuota()
	}
	quota, err = quota.Release(task.Target)
	if err != nil {
		return runnerTaskChange{}, err
	}
	system, err := decodeSystemPoolRegistry(allocation.system.Value)
	if err != nil {
		return runnerTaskChange{}, corruptSystemPoolRegistry()
	}
	system, err = system.ReleaseRunner(task.Target, record.Allocation.NetworkCIDR)
	if err != nil {
		return runnerTaskChange{}, err
	}
	quotaValue, err := recordcodec.Encode("runner_tenant_quota", quota)
	if err != nil {
		return runnerTaskChange{}, err
	}
	systemValue, err := recordcodec.Encode("system_pool_registry", system)
	if err != nil {
		clear(quotaValue)
		return runnerTaskChange{}, err
	}
	change.values = append(change.values, quotaValue, systemValue)
	change.mutations = append(
		change.mutations,
		etcdstore.Mutation{
			Type: etcdstore.MutationDelete,
			Key:  runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, task.Target),
		},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug)},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: runnerTenantQuotaKey(record.Desired.TenantID), Value: quotaValue},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: runnerHostSlotKey(record.Allocation.Slot)},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: systemPoolRegistryKey, Value: systemValue},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: runnerObservationKey(task.Target)},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: runnerLifecycleKey(task.Target)},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: runnerKey(task.Target)},
	)
	return change, nil
}

func (repository *TaskRepository) prepareRunnerTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (runnerTaskChange, error) {
	applies, err := taskOwnsRunner(source)
	if err != nil || !applies {
		return runnerTaskChange{}, err
	}
	if source.Type == TaskCreate {
		return runnerTaskChange{}, errs.New(
			errs.KindValidationFailed,
			"runner creation retry requires a fresh registration token",
		)
	}
	sourceEvidence, sourceEvidenceErr := decodeRunnerRemovalTaskEvidence(source)
	retryEvidence, retryEvidenceErr := decodeRunnerRemovalTaskEvidence(retry)
	if retry.Type != TaskRemove || retry.Target != source.Target || sourceEvidenceErr != nil ||
		retryEvidenceErr != nil || sourceEvidence != retryEvidence || !runnerStringMapsEqual(source.Params, retry.Params) {
		return runnerTaskChange{}, errs.New(errs.KindInternal, "runner removal retry changed its durable target")
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerKey(source.Target),
			runnerLifecycleKey(source.Target),
			deletionTombstoneKey(string(DeletionTargetRunner), source.Target),
			runnerRemovalIntentKey(source.Target),
			runnerRuntimeOwnershipKey(source.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return runnerTaskChange{}, err
	}
	if result == nil || len(result.Values) != 5 || result.Values[0] == nil || result.Values[1] == nil ||
		result.Values[2] != nil || result.Values[3] != nil {
		return runnerTaskChange{}, errs.New(errs.KindStateConflict, "runner is not available for removal retry")
	}
	record, err := decodeRunnerAggregate(result.Values[0], result.Values[1])
	if err != nil {
		return runnerTaskChange{}, err
	}
	if !sourceEvidence.matchesRecord(record) {
		return runnerTaskChange{}, errs.New(errs.KindStateConflict, "runner removal retry target changed")
	}
	if result.Values[4] != nil {
		ownership, decodeErr := decodeRunnerRuntimeOwnership(result.Values[4].Value)
		if decodeErr != nil || ownership.RunnerID != record.Desired.ID ||
			ownership.RuntimeEpoch >= record.RuntimeEpoch || record.ContainerID == "" {
			return runnerTaskChange{}, errs.New(errs.KindInternal, "runner removal retry ownership is corrupt")
		}
	}
	replacement, err := TakeRunnerRuntimeCleanupOwnership(record)
	if err != nil {
		return runnerTaskChange{}, err
	}
	lifecycleValue, err := encodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
	if err != nil {
		return runnerTaskChange{}, err
	}
	allocation, err := (&RunnerRepository{store: repository.store}).readRunnerAllocationEvidence(
		ctx, record, revision,
	)
	if err != nil {
		return runnerTaskChange{}, err
	}
	parentKeys := []string{
		hierarchyrecord.TenantKey(record.Desired.TenantID),
		deletionTombstoneKey(string(DeletionTargetTenant), record.Desired.TenantID),
	}
	if record.Desired.OwnerKind == RunnerOwnerProject {
		parentKeys = append(parentKeys,
			hierarchyrecord.ProjectKey(record.Desired.OwnerID),
			deletionTombstoneKey(string(DeletionTargetProject), record.Desired.OwnerID),
		)
	}
	parents, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: parentKeys, Revision: revision})
	if err != nil {
		return runnerTaskChange{}, err
	}
	if parents == nil || len(parents.Values) != len(parentKeys) {
		return runnerTaskChange{}, errs.New(errs.KindInternal, "runner owner evidence is incomplete")
	}
	if parents.Values[0] == nil {
		return runnerTaskChange{}, errs.New(errs.KindTenantNotFound, "tenant was not found")
	}
	if parents.Values[1] != nil {
		return runnerTaskChange{}, errs.New(errs.KindResourceInUse, "tenant deletion is in progress")
	}
	tenant, err := hierarchyrecord.DecodeTenant(parents.Values[0].Value)
	if err != nil || tenant.ID != record.Desired.TenantID {
		return runnerTaskChange{}, recordcodec.CorruptRecord()
	}
	if record.Desired.OwnerKind == RunnerOwnerProject {
		if parents.Values[2] == nil {
			return runnerTaskChange{}, errs.New(errs.KindProjectNotFound, "project was not found")
		}
		if parents.Values[3] != nil {
			return runnerTaskChange{}, errs.New(errs.KindResourceInUse, "project deletion is in progress")
		}
		project, decodeErr := hierarchyrecord.DecodeProject(parents.Values[2].Value)
		if decodeErr != nil || project.ID != record.Desired.OwnerID || project.TenantID != tenant.ID ||
			project.Kind != hierarchyrecord.ProjectKindTenant {
			return runnerTaskChange{}, recordcodec.CorruptRecord()
		}
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetRunner, TargetID: source.Target,
		TargetRevision: result.Values[0].ModRevision, TaskID: retry.ID,
		Phase: DeletionPhaseFinalizing, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	intent := RunnerRemovalIntent{
		RunnerID: record.Desired.ID, TaskID: retry.ID,
		OwnerKind: record.Desired.OwnerKind, OwnerID: record.Desired.OwnerID,
		TenantID: record.Desired.TenantID, Allocation: record.Allocation, CreatedAt: retry.CreatedAt,
	}
	if !retryEvidence.matchesIntent(intent) {
		return runnerTaskChange{}, errs.New(errs.KindStateConflict, "runner removal retry intent changed")
	}
	tombstoneValue, err := encodeRunnerDeletionTombstone(tombstone)
	if err != nil {
		return runnerTaskChange{}, err
	}
	intentValue, err := encodeRunnerRemovalIntent(intent)
	if err != nil {
		clear(tombstoneValue)
		return runnerTaskChange{}, err
	}
	change := runnerTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: runnerKey(source.Target), ModRevision: result.Values[0].ModRevision},
			{Key: runnerLifecycleKey(source.Target), ModRevision: result.Values[1].ModRevision},
			{Key: runnerRuntimeOwnershipKey(source.Target), ModRevision: keyValueRevision(result.Values[4])},
			{Key: deletionTombstoneKey(string(DeletionTargetRunner), source.Target)},
			{Key: runnerRemovalIntentKey(source.Target)},
			{
				Key:         runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID),
				ModRevision: allocation.owner.ModRevision,
			},
			{
				Key:         runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug),
				ModRevision: allocation.slug.ModRevision,
			},
			{Key: runnerTenantQuotaKey(record.Desired.TenantID), ModRevision: allocation.quota.ModRevision},
			{Key: runnerHostSlotKey(record.Allocation.Slot), ModRevision: allocation.host.ModRevision},
			{Key: systemPoolRegistryKey, ModRevision: allocation.system.ModRevision},
			{Key: hierarchyrecord.TenantKey(record.Desired.TenantID), ModRevision: parents.Values[0].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetTenant), record.Desired.TenantID)},
		},
		mutations: []etcdstore.Mutation{
			{
				Type: etcdstore.MutationPut, Key: deletionTombstoneKey(string(DeletionTargetRunner), source.Target),
				Value: tombstoneValue,
			},
			{Type: etcdstore.MutationPut, Key: runnerRemovalIntentKey(source.Target), Value: intentValue},
			{Type: etcdstore.MutationPut, Key: runnerLifecycleKey(source.Target), Value: lifecycleValue},
		},
		values: [][]byte{tombstoneValue, intentValue, lifecycleValue},
	}
	if record.Desired.OwnerKind == RunnerOwnerProject {
		change.conditions = append(change.conditions,
			etcdstore.Condition{Key: hierarchyrecord.ProjectKey(record.Desired.OwnerID), ModRevision: parents.Values[2].ModRevision},
			etcdstore.Condition{Key: deletionTombstoneKey(string(DeletionTargetProject), record.Desired.OwnerID)},
		)
	}
	return change, nil
}

func runnerStringMapsEqual(left map[string]string, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func taskOwnsRunner(task TaskRecord) (bool, error) {
	if task.Executor != TaskExecutorController || task.Params[TaskResourceKindParam] != TaskResourceRunner {
		return false, nil
	}
	switch task.Type {
	case TaskCreate:
		return taskOwnsRunnerCreation(task)
	case TaskRemove:
		_, err := decodeRunnerRemovalTaskEvidence(task)
		return err == nil, err
	default:
		return false, errs.New(errs.KindInternal, "runner task has invalid durable input")
	}
}

func (repository *TaskRepository) validateRunnerTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsRunner(task)
	if err != nil || !applies {
		return err
	}
	if task.Status != terminalStatus || !runnerTerminal(terminalStatus) {
		return errs.New(errs.KindInternal, "runner terminal task has invalid durable input")
	}
	if task.Type == TaskCreate {
		return repository.validateRunnerCreationAcknowledgementReplay(ctx, task, terminalStatus, revision)
	}
	return repository.validateRunnerRemovalAcknowledgementReplay(ctx, task, terminalStatus, revision)
}

func (repository *TaskRepository) validateRunnerCreationAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerKey(task.Target),
			runnerLifecycleKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetRunner), task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != 3 || stored.Values[0] == nil || stored.Values[1] == nil ||
		stored.Values[2] != nil {
		return errs.New(errs.KindStateConflict, "runner creation replay evidence is incomplete")
	}
	record, err := decodeRunnerAggregate(stored.Values[0], stored.Values[1])
	if err != nil || record.Desired.ID != task.Target || record.CreateTaskID != task.ID {
		return errs.New(errs.KindStateConflict, "runner creation replay retained corrupt target state")
	}
	wantState := RunnerProvisioningFailed
	if terminalStatus == TaskStatusCompleted {
		wantState = RunnerProvisioningReady
	}
	if record.ProvisioningState != wantState {
		return errs.New(errs.KindStateConflict, "runner creation replay target state changed")
	}
	_, err = (&RunnerRepository{store: repository.store}).readRunnerAllocationEvidence(ctx, record, revision)
	return err
}

func (repository *TaskRepository) validateRunnerRemovalAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	evidence, err := decodeRunnerRemovalTaskEvidence(task)
	if err != nil {
		return err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerKey(task.Target),
			runnerLifecycleKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetRunner), task.Target),
			runnerRemovalIntentKey(task.Target),
			runnerObservationKey(task.Target),
			runnerOwnerKey(evidence.ownerKind, evidence.ownerID, task.Target),
			runnerTenantQuotaKey(evidence.tenantID),
			runnerHostSlotKey(evidence.hostSlot),
			systemPoolRegistryKey,
			runnerRuntimeOwnershipKey(task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != 10 {
		return errs.New(errs.KindInternal, "runner removal replay evidence is incomplete")
	}
	if stored.Values[2] != nil || stored.Values[3] != nil {
		return errs.New(errs.KindStateConflict, "runner removal replay found a live cleanup fence")
	}
	if stored.Values[6] == nil || stored.Values[8] == nil {
		return errs.New(errs.KindStateConflict, "runner removal replay lost allocation registries")
	}
	quota, err := decodeRunnerTenantQuota(stored.Values[6].Value)
	if err != nil || quota.Validate() != nil {
		return corruptRunnerTenantQuota()
	}
	system, err := decodeSystemPoolRegistry(stored.Values[8].Value)
	if err != nil || system.Reservations == nil {
		return corruptSystemPoolRegistry()
	}
	if terminalStatus == TaskStatusCompleted {
		if stored.Values[0] != nil || stored.Values[1] != nil || stored.Values[4] != nil || stored.Values[5] != nil ||
			stored.Values[7] != nil || stored.Values[9] != nil {
			return errs.New(errs.KindStateConflict, "completed runner removal retained target state")
		}
		index := sortSearchRunnerID(quota.RunnerIDs, task.Target)
		if (index < len(quota.RunnerIDs) && quota.RunnerIDs[index] == task.Target) ||
			system.Reservations[runnerallocation.RunnerReservationOwner(task.Target)] != "" {
			return errs.New(errs.KindStateConflict, "completed runner removal retained allocation ownership")
		}
		return nil
	}
	if stored.Values[0] == nil || stored.Values[1] == nil || stored.Values[5] == nil || stored.Values[7] == nil {
		return errs.New(errs.KindStateConflict, "failed runner removal lost target state")
	}
	record, err := decodeRunnerAggregate(stored.Values[0], stored.Values[1])
	if err != nil || record.Desired.ID != task.Target || !evidence.matchesRecord(record) {
		return errs.New(errs.KindStateConflict, "failed runner removal retained corrupt target state")
	}
	slug, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug)},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if slug == nil || len(slug.Values) != 1 {
		return errs.New(errs.KindInternal, "runner removal replay slug evidence is incomplete")
	}
	return runnerAllocationEvidenceOwns(record, runnerAllocationEvidence{
		owner:  stored.Values[5],
		slug:   slug.Values[0],
		quota:  stored.Values[6],
		host:   stored.Values[7],
		system: stored.Values[8],
	})
}

func taskOwnsRunnerCreation(task TaskRecord) (bool, error) {
	if task.Executor != TaskExecutorController || task.Params[TaskResourceKindParam] != TaskResourceRunner {
		return false, nil
	}
	if task.Type != TaskCreate || ids.Validate(ids.KindRunner, task.Target) != nil ||
		len(task.Params) != 2 || task.Params[RunnerRegistrationTokenPresentParam] != "true" {
		return false, errs.New(errs.KindInternal, "runner creation task has invalid durable input")
	}
	return true, nil
}

func runnerIntentMatchesRecord(intent RunnerRemovalIntent, record RunnerRecord, taskID string) bool {
	return intent.RunnerID == record.Desired.ID && intent.TaskID == taskID &&
		intent.OwnerKind == record.Desired.OwnerKind && intent.OwnerID == record.Desired.OwnerID &&
		intent.TenantID == record.Desired.TenantID && intent.Allocation == record.Allocation
}

func runnerRetryableTerminal(status TaskStatus) bool {
	return status == TaskStatusFailed || status == TaskStatusAborted || status == TaskStatusTimedOut
}

func runnerTerminal(status TaskStatus) bool {
	return status == TaskStatusCompleted || runnerRetryableTerminal(status)
}

func clearRunnerTaskChange(change runnerTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
