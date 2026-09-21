package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionReservationFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		hierarchyrecord.EnvironmentKey(action.TargetID), networkreservations.EnvironmentPoolRegistryKey,
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
	registry, err := recordcodec.Decode[networkreservations.EnvironmentPoolRegistry](result.Values[1].Value, "environment_pool_registry")
	if err != nil || networkreservations.ValidateEnvironmentPoolRegistry(registry) != nil {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	next, err := registry.Release(environment.ID, environment.NetworkPool)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	mutation := etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: networkreservations.EnvironmentPoolRegistryKey}
	values := [][]byte(nil)
	if len(next.Reservations) != 0 {
		value, encodeErr := recordcodec.Encode("environment_pool_registry", next)
		if encodeErr != nil {
			return hierarchyDeletionControllerEffects{}, encodeErr
		}
		values = append(values, value)
		mutation = etcdstore.Mutation{Type: etcdstore.MutationPut, Key: networkreservations.EnvironmentPoolRegistryKey, Value: value}
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: fixedInputDigest,
		conditions: []etcdstore.Condition{
			{Key: hierarchyrecord.EnvironmentKey(environment.ID), ModRevision: result.Values[0].ModRevision},
			{Key: networkreservations.EnvironmentPoolRegistryKey, ModRevision: result.Values[1].ModRevision},
		},
		mutations: []etcdstore.Mutation{mutation}, values: values,
	}, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionRunnerFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	base, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		runnerrecord.RunnerKey(action.TargetID), runnerrecord.RunnerLifecycleKey(action.TargetID),
		runnerrecord.RunnerObservationKey(action.TargetID), runnerrecord.RunnerRuntimeOwnershipKey(action.TargetID),
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
	record, err := runnerrecord.DecodeRunnerAggregate(base.Values[0], base.Values[1])
	if err != nil || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	allocation, err := (composeRunnerRepository(repository.store)).readRunnerAllocationEvidence(ctx, record, 0)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clearRunnerAllocationEvidence(allocation)
	quota, err := runnerrecord.DecodeRunnerTenantQuota(allocation.quota.Value)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	quota, err = quota.Release(action.TargetID)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	system, err := runnerrecord.DecodeSystemPoolRegistry(allocation.system.Value)
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
		{Key: runnerrecord.RunnerKey(action.TargetID), ModRevision: base.Values[0].ModRevision},
		{Key: runnerrecord.RunnerLifecycleKey(action.TargetID), ModRevision: base.Values[1].ModRevision},
		{Key: runnerrecord.RunnerObservationKey(action.TargetID), ModRevision: etcdstore.RevisionOf(base.Values[2])},
		{Key: runnerrecord.RunnerRuntimeOwnershipKey(action.TargetID)},
		{
			Key:         runnerrecord.RunnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, action.TargetID),
			ModRevision: allocation.owner.ModRevision,
		},
		{
			Key:         runnerrecord.RunnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug),
			ModRevision: allocation.slug.ModRevision,
		},
		{Key: runnerrecord.RunnerTenantQuotaKey(record.Desired.TenantID), ModRevision: allocation.quota.ModRevision},
		{Key: runnerrecord.RunnerHostSlotKey(record.Allocation.Slot), ModRevision: allocation.host.ModRevision},
		{Key: runnerrecord.SystemPoolRegistryKey, ModRevision: allocation.system.ModRevision},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: runnerrecord.RunnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, action.TargetID)},
		{Type: etcdstore.MutationDelete, Key: runnerrecord.RunnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug)},
		{Type: etcdstore.MutationPut, Key: runnerrecord.RunnerTenantQuotaKey(record.Desired.TenantID), Value: quotaValue},
		{Type: etcdstore.MutationDelete, Key: runnerrecord.RunnerHostSlotKey(record.Allocation.Slot)},
		{Type: etcdstore.MutationPut, Key: runnerrecord.SystemPoolRegistryKey, Value: systemValue},
		{Type: etcdstore.MutationDelete, Key: runnerrecord.RunnerObservationKey(action.TargetID)},
		{Type: etcdstore.MutationDelete, Key: runnerrecord.RunnerLifecycleKey(action.TargetID)},
		{Type: etcdstore.MutationDelete, Key: runnerrecord.RunnerKey(action.TargetID)},
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(base.Values[0].Value), conditions: conditions,
		mutations: mutations, values: [][]byte{quotaValue, systemValue},
	}, nil
}
