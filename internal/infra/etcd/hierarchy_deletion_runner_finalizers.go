package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionReservationFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		hierarchyrecord.EnvironmentKey(action.TargetID), environmentPoolRegistryKey,
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if result == nil || len(result.Values) != 2 || result.Values[0] == nil || result.Values[1] == nil {
		if result != nil {
			etcdstore.ClearValues(result.Values)
		}
		return hierarchyDeletionControllerEffects{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion reservation authority changed",
		)
	}
	defer etcdstore.ClearValues(result.Values)
	environment, err := hierarchyrecord.DecodeEnvironment(result.Values[0].Value)
	if err != nil || environment.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	fixedInputDigest := hierarchyDeletionBytesDigest(result.Values[0].Value)
	if result.Values[0].ModRevision != action.TargetRevision {
		if environment.DeletionTaskID == "" {
			return hierarchyDeletionControllerEffects{}, errs.New(
				errs.KindStateConflict,
				"hierarchy deletion reservation authority changed",
			)
		}
		frozen := environment
		frozen.DeletionTaskID = ""
		frozenValue, encodeErr := hierarchyrecord.EncodeEnvironment(frozen)
		if encodeErr != nil {
			return hierarchyDeletionControllerEffects{}, encodeErr
		}
		fixedInputDigest = hierarchyDeletionBytesDigest(frozenValue)
		clear(frozenValue)
	}
	registry, err := recordcodec.Decode[EnvironmentPoolRegistry](result.Values[1].Value, "environment_pool_registry")
	if err != nil || validateEnvironmentPoolRegistry(registry) != nil {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	next, err := registry.Release(environment.ID, environment.NetworkPool)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	mutation := etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: environmentPoolRegistryKey}
	values := [][]byte(nil)
	if len(next.Reservations) != 0 {
		value, encodeErr := recordcodec.Encode("environment_pool_registry", next)
		if encodeErr != nil {
			return hierarchyDeletionControllerEffects{}, encodeErr
		}
		values = append(values, value)
		mutation = etcdstore.Mutation{Type: etcdstore.MutationPut, Key: environmentPoolRegistryKey, Value: value}
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: fixedInputDigest,
		conditions: []etcdstore.Condition{
			{Key: hierarchyrecord.EnvironmentKey(environment.ID), ModRevision: result.Values[0].ModRevision},
			{Key: environmentPoolRegistryKey, ModRevision: result.Values[1].ModRevision},
		},
		mutations: []etcdstore.Mutation{mutation}, values: values,
	}, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionRunnerFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	base, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		runnerKey(action.TargetID), runnerLifecycleKey(action.TargetID),
		runnerObservationKey(action.TargetID), runnerRuntimeOwnershipKey(action.TargetID),
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if base == nil || len(base.Values) != 4 || base.Values[0] == nil || base.Values[1] == nil ||
		base.Values[0].ModRevision != action.TargetRevision || base.Values[3] != nil {
		if base != nil {
			etcdstore.ClearValues(base.Values)
		}
		return hierarchyDeletionControllerEffects{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion Runner cleanup authority changed",
		)
	}
	defer etcdstore.ClearValues(base.Values)
	record, err := decodeRunnerAggregate(base.Values[0], base.Values[1])
	if err != nil || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	allocation, err := (&RunnerRepository{store: repository.store}).readRunnerAllocationEvidence(ctx, record, 0)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clearRunnerAllocationEvidence(allocation)
	quota, err := decodeRunnerTenantQuota(allocation.quota.Value)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	quota, err = quota.Release(action.TargetID)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	system, err := decodeSystemPoolRegistry(allocation.system.Value)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	system, err = system.ReleaseRunner(action.TargetID, record.Allocation.NetworkCIDR)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	quotaValue, err := recordcodec.Encode("runner_tenant_quota", quota)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	systemValue, err := recordcodec.Encode("system_pool_registry", system)
	if err != nil {
		clear(quotaValue)
		return hierarchyDeletionControllerEffects{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: runnerKey(action.TargetID), ModRevision: base.Values[0].ModRevision},
		{Key: runnerLifecycleKey(action.TargetID), ModRevision: base.Values[1].ModRevision},
		{Key: runnerObservationKey(action.TargetID), ModRevision: keyValueRevision(base.Values[2])},
		{Key: runnerRuntimeOwnershipKey(action.TargetID)},
		{
			Key:         runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, action.TargetID),
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
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, action.TargetID)},
		{Type: etcdstore.MutationDelete, Key: runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug)},
		{Type: etcdstore.MutationPut, Key: runnerTenantQuotaKey(record.Desired.TenantID), Value: quotaValue},
		{Type: etcdstore.MutationDelete, Key: runnerHostSlotKey(record.Allocation.Slot)},
		{Type: etcdstore.MutationPut, Key: systemPoolRegistryKey, Value: systemValue},
		{Type: etcdstore.MutationDelete, Key: runnerObservationKey(action.TargetID)},
		{Type: etcdstore.MutationDelete, Key: runnerLifecycleKey(action.TargetID)},
		{Type: etcdstore.MutationDelete, Key: runnerKey(action.TargetID)},
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(base.Values[0].Value), conditions: conditions,
		mutations: mutations, values: [][]byte{quotaValue, systemValue},
	}, nil
}
