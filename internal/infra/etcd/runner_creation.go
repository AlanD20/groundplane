package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateRunnerWithTask atomically publishes the provisioning Runner, its one
// owner index, combined Tenant quota, exact host/network allocations, Task,
// queue membership, and idempotency marker before any host effect can start.
func (repository *RunnerRepository) CreateRunnerWithTask(
	ctx context.Context,
	config runnerallocation.RunnerAllocationConfig,
	desired runnerrecord.RunnerDesiredRecord,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	desired, err := runnerrecord.NormalizeRunnerDesired(desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	slotCount, err := config.Validate()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRunnerCreateTask(desired, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRunnerCreateMarker(desired, task, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task = bindRunnerTaskMarker(task, marker)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	existing, err := idempotency.Read(ctx, marker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing != nil {
		return IdempotencyTransactionResult{
			kind: idempotencyTransactionExisting, revision: existing.modRevision,
			marker: cloneIdempotencyMarker(existing.marker),
		}, nil
	}
	parents, err := repository.resolveRunnerParents(ctx, desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	allocationState, err := repository.getRunnerAllocationState(ctx, desired.TenantID, desired.ID, config, slotCount)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	quota, err := allocationState.quota.Record.Claim(desired.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	systemPool, subnet, err := allocationState.system.Record.ReserveRunner(config.RunnerPool, desired.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	allocation, err := config.HostPool.Allocation(allocationState.host.slot, subnet)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	record, err := runnerrecord.NewProvisioningRunner(desired, allocation, task.ID, task.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	values, err := encodeRunnerCreateValues(record, quota, allocationState.host.record, systemPool, task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer values.clear()
	layout := newRunnerCreateEvidence(desired, parents, allocationState, task)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: values.task},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: values.reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: values.reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: values.reference},
		{Type: etcdstore.MutationPut, Key: runnerKey(desired.ID), Value: values.runner},
		{Type: etcdstore.MutationPut, Key: runnerLifecycleKey(desired.ID), Value: values.lifecycle},
		{Type: etcdstore.MutationPut, Key: runnerTenantSlugKey(desired.TenantID, desired.Slug), Value: []byte(desired.ID)},
		{
			Type: etcdstore.MutationPut, Key: runnerOwnerKey(desired.OwnerKind, desired.OwnerID, desired.ID),
			Value: []byte(desired.ID),
		},
		{Type: etcdstore.MutationPut, Key: runnerTenantQuotaKey(desired.TenantID), Value: values.quota},
		{Type: etcdstore.MutationPut, Key: runnerHostSlotKey(allocationState.host.slot), Value: values.host},
		{Type: etcdstore.MutationPut, Key: systemPoolRegistryKey, Value: values.system},
	}
	initiation, err := newRunnerTaskInitiation(desired, parents, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, layout.conditions, mutations, layout.classifier())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

type runnerCreateValues struct {
	runner, lifecycle, quota, host, system, task, reference []byte
}

func encodeRunnerCreateValues(
	record runnerrecord.RunnerRecord,
	quota runnerallocation.RunnerTenantQuota,
	host RunnerHostSlotRecord,
	system runnerallocation.SystemPoolRegistry,
	task TaskRecord,
) (runnerCreateValues, error) {
	var result runnerCreateValues
	var err error
	result.runner, err = runnerrecord.EncodeRunnerDesiredRecord(record.Desired)
	if err != nil {
		return result, err
	}
	result.lifecycle, err = runnerrecord.EncodeRunnerLifecycleRecord(record.RunnerLifecycleRecord)
	if err != nil {
		result.clear()
		return runnerCreateValues{}, err
	}
	result.quota, err = recordcodec.Encode("runner_tenant_quota", quota)
	if err != nil {
		result.clear()
		return runnerCreateValues{}, err
	}
	result.host, err = encodeRunnerHostSlotRecord(host)
	if err != nil {
		result.clear()
		return runnerCreateValues{}, err
	}
	result.system, err = recordcodec.Encode("system_pool_registry", system)
	if err != nil {
		result.clear()
		return runnerCreateValues{}, err
	}
	result.task, err = encodeTaskRecord(task)
	if err != nil {
		result.clear()
		return runnerCreateValues{}, err
	}
	result.reference, err = encodeTaskReference(task.ID)
	if err != nil {
		result.clear()
		return runnerCreateValues{}, err
	}
	return result, nil
}

func (values *runnerCreateValues) clear() {
	clear(values.runner)
	clear(values.lifecycle)
	clear(values.quota)
	clear(values.host)
	clear(values.system)
	clear(values.task)
	clear(values.reference)
}

type runnerCreateEvidence struct {
	conditions      []etcdstore.Condition
	task            int
	operation       int
	active          int
	queue           int
	runner          int
	lifecycle       int
	slug            int
	owner           int
	quota           int
	system          int
	tenant          int
	runnerDeletion  int
	tenantDeletion  int
	project         int
	projectDeletion int
	host            int
	desired         runnerrecord.RunnerDesiredRecord
	parents         runnerParents
	allocation      runnerAllocationState
	operationID     string
}

func newRunnerCreateEvidence(
	desired runnerrecord.RunnerDesiredRecord,
	parents runnerParents,
	allocation runnerAllocationState,
	task TaskRecord,
) runnerCreateEvidence {
	evidence := runnerCreateEvidence{
		project: -1, projectDeletion: -1, desired: desired, parents: parents,
		allocation: allocation, operationID: task.OperationID,
	}
	add := func(condition etcdstore.Condition) int {
		index := len(evidence.conditions)
		evidence.conditions = append(evidence.conditions, condition)
		return index
	}
	evidence.task = add(etcdstore.Condition{Key: taskKey(task.ID)})
	evidence.operation = add(etcdstore.Condition{Key: taskOperationIndexKey(task.OperationID, task.ID)})
	evidence.active = add(etcdstore.Condition{Key: taskActiveOperationKey(task.OperationID)})
	evidence.queue = add(etcdstore.Condition{Key: taskQueueKey(task.Executor, task.ID)})
	evidence.runner = add(etcdstore.Condition{Key: runnerKey(desired.ID)})
	evidence.lifecycle = add(etcdstore.Condition{Key: runnerLifecycleKey(desired.ID)})
	evidence.slug = add(etcdstore.Condition{Key: runnerTenantSlugKey(desired.TenantID, desired.Slug)})
	evidence.owner = add(etcdstore.Condition{Key: runnerOwnerKey(desired.OwnerKind, desired.OwnerID, desired.ID)})
	evidence.quota = add(etcdstore.Condition{Key: runnerTenantQuotaKey(desired.TenantID), ModRevision: allocation.quota.Revision})
	evidence.system = add(etcdstore.Condition{Key: systemPoolRegistryKey, ModRevision: allocation.system.Revision})
	evidence.tenant = add(etcdstore.Condition{Key: hierarchyrecord.TenantKey(desired.TenantID), ModRevision: parents.tenant.Revision})
	evidence.runnerDeletion = add(etcdstore.Condition{Key: deletionTombstoneKey(string(DeletionTargetRunner), desired.ID)})
	evidence.tenantDeletion = add(etcdstore.Condition{Key: deletionTombstoneKey(string(DeletionTargetTenant), desired.TenantID)})
	if desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		evidence.project = add(etcdstore.Condition{Key: hierarchyrecord.ProjectKey(desired.OwnerID), ModRevision: parents.project.Revision})
		evidence.projectDeletion = add(
			etcdstore.Condition{Key: deletionTombstoneKey(string(DeletionTargetProject), desired.OwnerID)},
		)
	}
	evidence.host = add(allocation.host.condition)
	return evidence
}

func (evidence runnerCreateEvidence) classifier() idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(evidence.conditions) {
			return errs.New(errs.KindInternal, "runner creation compare evidence is incomplete")
		}
		if values[evidence.active] != nil {
			activeTaskID, err := decodeTaskReference(values[evidence.active].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				evidence.operationID,
				activeTaskID,
			)
		}
		for _, index := range []int{evidence.task, evidence.operation, evidence.queue} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "runner creation collided with durable task state")
			}
		}
		if values[evidence.runner] != nil || values[evidence.lifecycle] != nil || values[evidence.owner] != nil {
			return errs.New(errs.KindStateConflict, "runner stable identity is already in use")
		}
		if values[evidence.slug] != nil {
			return errs.New(errs.KindRunnerSlugConflict, "runner slug is already in use")
		}
		if revisionChanged(values[evidence.quota], evidence.allocation.quota.Revision) {
			return stateConflict("runner tenant quota", evidence.desired.TenantID)
		}
		if revisionChanged(values[evidence.system], evidence.allocation.system.Revision) {
			return stateConflict("system pool registry", "global")
		}
		if values[evidence.tenant] == nil {
			return errs.New(errs.KindTenantNotFound, "tenant was not found")
		}
		if values[evidence.tenant].ModRevision != evidence.parents.tenant.Revision {
			return stateConflict("tenant", evidence.desired.TenantID)
		}
		if values[evidence.runnerDeletion] != nil || values[evidence.tenantDeletion] != nil {
			return errs.New(errs.KindResourceInUse, "runner hierarchy deletion is in progress")
		}
		if evidence.project >= 0 {
			if values[evidence.project] == nil {
				return errs.New(errs.KindProjectNotFound, "project was not found")
			}
			if values[evidence.project].ModRevision != evidence.parents.project.Revision {
				return stateConflict("project", evidence.desired.OwnerID)
			}
			if values[evidence.projectDeletion] != nil {
				return errs.New(errs.KindResourceInUse, "runner hierarchy deletion is in progress")
			}
		}
		if values[evidence.host] != nil {
			return stateConflict("runner host slot", evidence.desired.ID)
		}
		return stateConflict("runner", evidence.desired.ID)
	}
}
