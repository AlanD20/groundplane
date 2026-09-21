package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type runnerTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareRunnerTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) (runnerTaskChange, error) {
	applies, err := taskOwnsRunner(task)
	if err != nil || !applies {
		return runnerTaskChange{}, err
	}
	if task.Type == taskjournal.TaskCreate {
		return repository.prepareRunnerCreationAcknowledgement(ctx, task, terminalStatus, revision)
	}
	return repository.prepareRunnerRemovalAcknowledgement(ctx, task, terminalStatus, revision)
}

func (repository *TaskRepository) prepareRunnerCreationAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) (runnerTaskChange, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerKey(task.Target), runnerLifecycleKey(task.Target),
			deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), task.Target),
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
	if terminalStatus == taskjournal.TaskStatusCompleted || proofValue != nil {
		if result.Values[3] == nil || proofValue == nil || record.ContainerID == "" {
			return runnerTaskChange{}, errs.New(errs.KindStateConflict, "exact runner readiness proof is required")
		}
		ownership, ownershipErr := runnerrecord.DecodeRunnerRuntimeOwnership(result.Values[3].Value)
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
	replacement, err := runnerrecord.CompleteRunnerProvisioning(record, task.ID, terminalStatus == taskjournal.TaskStatusCompleted)
	if err != nil {
		return runnerTaskChange{}, err
	}
	value, err := runnerrecord.EncodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
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
		{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), task.Target)},
		{Key: runnerReadinessProofKey(task.ID), ModRevision: keyValueRevision(proofValue)},
		{
			Key:         runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID),
			ModRevision: allocation.owner.ModRevision,
		},
		{
			Key:         runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug),
			ModRevision: allocation.slug.ModRevision,
		},
		{Key: runnerrecord.RunnerTenantQuotaKey(record.Desired.TenantID), ModRevision: allocation.quota.ModRevision},
		{Key: runnerrecord.RunnerHostSlotKey(record.Allocation.Slot), ModRevision: allocation.host.ModRevision},
		{Key: runnerrecord.SystemPoolRegistryKey, ModRevision: allocation.system.ModRevision},
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
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) (runnerTaskChange, error) {
	base, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerKey(task.Target),
			runnerLifecycleKey(task.Target),
			deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), task.Target),
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
				Key:         deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), task.Target),
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
			{Key: runnerrecord.RunnerTenantQuotaKey(record.Desired.TenantID), ModRevision: allocation.quota.ModRevision},
			{Key: runnerrecord.RunnerHostSlotKey(record.Allocation.Slot), ModRevision: allocation.host.ModRevision},
			{Key: runnerrecord.SystemPoolRegistryKey, ModRevision: allocation.system.ModRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationDelete, Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetRunner), task.Target)},
			{Type: etcdstore.MutationDelete, Key: runnerRemovalIntentKey(task.Target)},
		},
	}
	if terminalStatus != taskjournal.TaskStatusCompleted {
		return change, nil
	}
	if base.Values[5] != nil {
		return runnerTaskChange{}, errs.New(errs.KindResourceInUse, "runner runtime cleanup proof is required")
	}
	quota, err := runnerrecord.DecodeRunnerTenantQuota(allocation.quota.Value)
	if err != nil {
		return runnerTaskChange{}, runnerrecord.CorruptRunnerTenantQuota()
	}
	quota, err = quota.Release(task.Target)
	if err != nil {
		return runnerTaskChange{}, err
	}
	system, err := runnerrecord.DecodeSystemPoolRegistry(allocation.system.Value)
	if err != nil {
		return runnerTaskChange{}, runnerrecord.CorruptSystemPoolRegistry()
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
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: runnerrecord.RunnerTenantQuotaKey(record.Desired.TenantID), Value: quotaValue},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: runnerrecord.RunnerHostSlotKey(record.Allocation.Slot)},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: runnerrecord.SystemPoolRegistryKey, Value: systemValue},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: runnerObservationKey(task.Target)},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: runnerLifecycleKey(task.Target)},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: runnerKey(task.Target)},
	)
	return change, nil
}
