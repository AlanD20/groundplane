package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
	if source.Type == taskjournal.TaskCreate {
		return runnerTaskChange{}, errs.New(
			errs.KindValidationFailed,
			"runner creation retry requires a fresh registration token",
		)
	}
	sourceEvidence, sourceEvidenceErr := decodeRunnerRemovalTaskEvidence(source)
	retryEvidence, retryEvidenceErr := decodeRunnerRemovalTaskEvidence(retry)
	if retry.Type != taskjournal.TaskRemove || retry.Target != source.Target || sourceEvidenceErr != nil ||
		retryEvidenceErr != nil || sourceEvidence != retryEvidence || !runnerStringMapsEqual(source.Params, retry.Params) {
		return runnerTaskChange{}, errs.New(errs.KindInternal, "runner removal retry changed its durable target")
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runnerKey(source.Target),
			runnerLifecycleKey(source.Target),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRunner), source.Target),
			runnerrecord.RunnerRemovalIntentKey(source.Target),
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
		ownership, decodeErr := runnerrecord.DecodeRunnerRuntimeOwnership(result.Values[4].Value)
		if decodeErr != nil || ownership.RunnerID != record.Desired.ID ||
			ownership.RuntimeEpoch >= record.RuntimeEpoch || record.ContainerID == "" {
			return runnerTaskChange{}, errs.New(errs.KindInternal, "runner removal retry ownership is corrupt")
		}
	}
	replacement, err := runnerrecord.TakeRunnerRuntimeCleanupOwnership(record)
	if err != nil {
		return runnerTaskChange{}, err
	}
	lifecycleValue, err := runnerrecord.EncodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
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
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), record.Desired.TenantID),
	}
	if record.Desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		parentKeys = append(parentKeys,
			hierarchyrecord.ProjectKey(record.Desired.OwnerID),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), record.Desired.OwnerID),
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
	if record.Desired.OwnerKind == runnerrecord.RunnerOwnerProject {
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
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetRunner, TargetID: source.Target,
		TargetRevision: result.Values[0].ModRevision, TaskID: retry.ID,
		Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	intent := runnerrecord.RunnerRemovalIntent{
		RunnerID: record.Desired.ID, TaskID: retry.ID,
		OwnerKind: record.Desired.OwnerKind, OwnerID: record.Desired.OwnerID,
		TenantID: record.Desired.TenantID, Allocation: record.Allocation, CreatedAt: retry.CreatedAt,
	}
	if !retryEvidence.matchesIntent(intent) {
		return runnerTaskChange{}, errs.New(errs.KindStateConflict, "runner removal retry intent changed")
	}
	tombstoneValue, err := runnerrecord.EncodeRunnerDeletionTombstone(tombstone)
	if err != nil {
		return runnerTaskChange{}, err
	}
	intentValue, err := runnerrecord.EncodeRunnerRemovalIntent(intent)
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
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRunner), source.Target)},
			{Key: runnerrecord.RunnerRemovalIntentKey(source.Target)},
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
			{Key: hierarchyrecord.TenantKey(record.Desired.TenantID), ModRevision: parents.Values[0].ModRevision},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), record.Desired.TenantID)},
		},
		mutations: []etcdstore.Mutation{
			{
				Type: etcdstore.MutationPut, Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRunner), source.Target),
				Value: tombstoneValue,
			},
			{Type: etcdstore.MutationPut, Key: runnerrecord.RunnerRemovalIntentKey(source.Target), Value: intentValue},
			{Type: etcdstore.MutationPut, Key: runnerLifecycleKey(source.Target), Value: lifecycleValue},
		},
		values: [][]byte{tombstoneValue, intentValue, lifecycleValue},
	}
	if record.Desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		change.conditions = append(change.conditions,
			etcdstore.Condition{Key: hierarchyrecord.ProjectKey(record.Desired.OwnerID), ModRevision: parents.Values[2].ModRevision},
			etcdstore.Condition{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), record.Desired.OwnerID)},
		)
	}
	return change, nil
}
