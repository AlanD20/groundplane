package etcd

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	runnerPrefix                 = "/v1/records/runners/"
	runnerLifecyclePrefix        = "/v1/runtime/runner-lifecycles/"
	runnerRuntimeOwnershipPrefix = "/v1/runtime/runner-ownership/"
	runnerObservationPrefix      = "/v1/runtime/observations/runners/"
)

type RunnerFilter struct {
	TenantID  string
	ProjectID string
}

type RunnerRepository struct {
	store hierarchyStore
}

func NewRunnerRepository(store Store) (*RunnerRepository, error) {
	return newRunnerRepository(store)
}

func newRunnerRepository(store hierarchyStore) (*RunnerRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "runner store is required")
	}
	return &RunnerRepository{store: store}, nil
}

func (repository *RunnerRepository) EnsureRunnerNetworkPool(
	ctx context.Context,
	config RunnerAllocationConfig,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if _, err := config.Validate(); err != nil {
		return err
	}
	result, err := repository.store.Get(ctx, systemPoolRegistryKey)
	if err != nil {
		return err
	}
	if result == nil {
		return errs.New(errs.KindInternal, "system pool registry read is missing")
	}
	if result.Entry != nil {
		return validateReservedRunnerNetworkPool(config, result.Entry.Value)
	}
	registry := SystemPoolRegistry{
		RunnerNetworkPool: config.RunnerPool.String(), Reservations: map[string]string{},
	}
	value, err := encodeEnvelope("system_pool_registry", registry)
	if err != nil {
		return err
	}
	defer clear(value)
	transaction, err := repository.store.Transact(ctx,
		[]Condition{{Key: systemPoolRegistryKey}},
		[]Mutation{{Type: MutationPut, Key: systemPoolRegistryKey, Value: value}},
	)
	if err != nil {
		return err
	}
	if transaction.Succeeded {
		return nil
	}
	if len(transaction.FailureReads) != 1 || transaction.FailureReads[0] == nil {
		return errs.New(errs.KindInternal, "system pool bootstrap compare evidence is incomplete")
	}
	return validateReservedRunnerNetworkPool(config, transaction.FailureReads[0].Value)
}

func validateReservedRunnerNetworkPool(config RunnerAllocationConfig, value []byte) error {
	if len(value) > maximumRunnerPersistenceBytes {
		return corruptSystemPoolRegistry()
	}
	registry, err := decodeSystemPoolRegistry(value)
	if err != nil || validateSystemPoolRegistry(config.SystemPool, registry) != nil {
		return corruptSystemPoolRegistry()
	}
	if registry.RunnerNetworkPool != config.RunnerPool.String() {
		return errs.New(errs.KindStateConflict, "configured runner network pool does not match its bootstrap reservation")
	}
	return nil
}

func (repository *RunnerRepository) AttestRunnerRuntimeOwnership(
	ctx context.Context,
	current Versioned[RunnerRecord],
	containerID string,
	ownership RunnerRuntimeOwnershipRecord,
) (Versioned[RunnerRuntimeOwnershipRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, err
	}
	if current.Revision <= 0 || current.Record.LifecycleRevision <= 0 || validateRunnerRecord(current.Record) != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, errs.New(errs.KindValidationFailed, "runner attestation target is invalid")
	}
	replacement, err := BindRunnerContainerID(current.Record, current.Record.CreateTaskID, containerID)
	if err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, err
	}
	if validateRunnerRuntimeOwnership(ownership) != nil || ownership.RunnerID != current.Record.Desired.ID ||
		ownership.RuntimeEpoch != replacement.RuntimeEpoch {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, errs.New(errs.KindValidationFailed, "runner runtime ownership does not match its lifecycle")
	}
	lifecycleValue, err := encodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
	if err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, err
	}
	defer clear(lifecycleValue)
	ownershipValue, err := encodeRunnerRuntimeOwnership(ownership)
	if err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, err
	}
	defer clear(ownershipValue)
	conditions := []Condition{
		{Key: runnerKey(ownership.RunnerID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(ownership.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerRuntimeOwnershipKey(ownership.RunnerID)},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), ownership.RunnerID)},
	}
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: runnerLifecycleKey(ownership.RunnerID), Value: lifecycleValue},
		{Type: MutationPut, Key: runnerRuntimeOwnershipKey(ownership.RunnerID), Value: ownershipValue},
	})
	if err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, err
	}
	if result.Succeeded {
		return Versioned[RunnerRuntimeOwnershipRecord]{
			Record: ownership, Revision: result.Revision, ReadRevision: result.Revision,
		}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, errs.New(errs.KindInternal, "runner attestation compare evidence is incomplete")
	}
	if result.FailureReads[3] != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, errs.New(errs.KindResourceInUse, "runner deletion is in progress")
	}
	if result.FailureReads[1] != nil && result.FailureReads[2] != nil &&
		sameRunnerRuntimeOwnershipBytes(result.FailureReads[1].Value, lifecycleValue) &&
		sameRunnerRuntimeOwnershipBytes(result.FailureReads[2].Value, ownershipValue) {
		stored, decodeErr := decodeRunnerRuntimeOwnership(result.FailureReads[2].Value)
		if decodeErr != nil {
			return Versioned[RunnerRuntimeOwnershipRecord]{}, decodeErr
		}
		return Versioned[RunnerRuntimeOwnershipRecord]{
			Record: stored, Revision: result.FailureReads[2].ModRevision, ReadRevision: result.Revision,
		}, nil
	}
	return Versioned[RunnerRuntimeOwnershipRecord]{}, stateConflict("runner runtime ownership", ownership.RunnerID)
}

func (repository *RunnerRepository) GetRunnerRuntimeOwnership(
	ctx context.Context,
	runnerID string,
) (Versioned[RunnerRuntimeOwnershipRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, false, err
	}
	if err := validateID(ids.KindRunner, runnerID); err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, runnerRuntimeOwnershipKey(runnerID))
	if err != nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, false, err
	}
	if result == nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, false, errs.New(errs.KindInternal, "runner runtime ownership read is missing")
	}
	if result.Entry == nil {
		return Versioned[RunnerRuntimeOwnershipRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := decodeRunnerRuntimeOwnership(result.Entry.Value)
	if err != nil || record.RunnerID != runnerID {
		return Versioned[RunnerRuntimeOwnershipRecord]{}, false, errs.New(errs.KindInternal, "runner runtime ownership is corrupt")
	}
	return Versioned[RunnerRuntimeOwnershipRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func (repository *RunnerRepository) BeginFailedRunnerRuntimeCleanup(
	ctx context.Context,
	current Versioned[RunnerRecord],
) (Versioned[RunnerRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if current.Revision <= 0 || current.Record.LifecycleRevision <= 0 || validateRunnerRecord(current.Record) != nil ||
		current.Record.ProvisioningState != RunnerProvisioningFailed || current.Record.ContainerID == "" {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindValidationFailed, "failed runner cleanup target is invalid")
	}
	evidence, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		runnerRuntimeOwnershipKey(current.Record.Desired.ID),
		deletionTombstoneKey(string(DeletionTargetRunner), current.Record.Desired.ID),
	}, Revision: current.ReadRevision})
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if evidence == nil || len(evidence.Values) != 2 || evidence.Values[0] == nil || evidence.Values[1] != nil {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindStateConflict, "failed runner cleanup ownership is unavailable")
	}
	ownership, err := decodeRunnerRuntimeOwnership(evidence.Values[0].Value)
	if err != nil || ownership.RunnerID != current.Record.Desired.ID ||
		ownership.RuntimeEpoch != current.Record.RuntimeEpoch {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindStateConflict, "failed runner cleanup ownership changed")
	}
	replacement, err := TakeRunnerRuntimeCleanupOwnership(current.Record)
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	lifecycleValue, err := encodeRunnerLifecycleRecord(replacement.RunnerLifecycleRecord)
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	defer clear(lifecycleValue)
	conditions := []Condition{
		{Key: runnerKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(current.Record.Desired.ID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerRuntimeOwnershipKey(current.Record.Desired.ID), ModRevision: evidence.Values[0].ModRevision},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), current.Record.Desired.ID)},
	}
	result, err := repository.store.Transact(ctx, conditions, []Mutation{{
		Type: MutationPut, Key: runnerLifecycleKey(current.Record.Desired.ID), Value: lifecycleValue,
	}})
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if result.Succeeded {
		replacement.LifecycleRevision = result.Revision
		return Versioned[RunnerRecord]{
			Record: replacement, Revision: current.Revision, ReadRevision: result.Revision,
		}, nil
	}
	if len(result.FailureReads) != len(conditions) {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "failed runner cleanup compare evidence is incomplete")
	}
	if result.FailureReads[1] != nil && result.FailureReads[2] != nil && result.FailureReads[3] == nil &&
		sameRunnerRuntimeOwnershipBytes(result.FailureReads[1].Value, lifecycleValue) &&
		sameRunnerRuntimeOwnershipBytes(result.FailureReads[2].Value, evidence.Values[0].Value) {
		replacement.LifecycleRevision = result.FailureReads[1].ModRevision
		return Versioned[RunnerRecord]{
			Record: replacement, Revision: current.Revision, ReadRevision: result.Revision,
		}, nil
	}
	return Versioned[RunnerRecord]{}, stateConflict("failed runner cleanup ownership", current.Record.Desired.ID)
}

func (repository *RunnerRepository) DeleteRunnerRuntimeOwnershipAfterCleanup(
	ctx context.Context,
	current Versioned[RunnerRecord],
	expected RunnerRuntimeOwnershipRecord,
) (int64, error) {
	if err := validateContext(ctx); err != nil {
		return 0, err
	}
	if current.Revision <= 0 || current.Record.LifecycleRevision <= 0 || validateRunnerRecord(current.Record) != nil ||
		validateRunnerRuntimeOwnership(expected) != nil || expected.RunnerID != current.Record.Desired.ID ||
		current.Record.ContainerID == "" || current.Record.RuntimeEpoch <= expected.RuntimeEpoch {
		return 0, errs.New(errs.KindValidationFailed, "runner runtime cleanup proof does not match its lifecycle")
	}
	expectedValue, err := encodeRunnerRuntimeOwnership(expected)
	if err != nil {
		return 0, err
	}
	defer clear(expectedValue)
	lifecycleValue, err := encodeRunnerLifecycleRecord(current.Record.RunnerLifecycleRecord)
	if err != nil {
		return 0, err
	}
	defer clear(lifecycleValue)
	evidence, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		runnerLifecycleKey(expected.RunnerID),
		runnerRuntimeOwnershipKey(expected.RunnerID),
		deletionTombstoneKey(string(DeletionTargetRunner), expected.RunnerID),
	}, Revision: current.ReadRevision})
	if err != nil {
		return 0, err
	}
	if evidence == nil || len(evidence.Values) != 3 || evidence.Values[0] == nil || evidence.Values[1] == nil ||
		evidence.Values[0].ModRevision != current.Record.LifecycleRevision ||
		!sameRunnerRuntimeOwnershipBytes(evidence.Values[0].Value, lifecycleValue) ||
		!sameRunnerRuntimeOwnershipBytes(evidence.Values[1].Value, expectedValue) {
		return 0, errs.New(errs.KindStateConflict, "runner runtime cleanup ownership changed")
	}
	if evidence.Values[2] != nil {
		tombstone, decodeErr := decodeRunnerDeletionTombstone(evidence.Values[2].Value)
		if decodeErr != nil || tombstone.TargetID != expected.RunnerID {
			return 0, errs.New(errs.KindStateConflict, "runner runtime cleanup tombstone changed")
		}
	} else if current.Record.ProvisioningState != RunnerProvisioningFailed {
		return 0, errs.New(errs.KindStateConflict, "runner runtime cleanup lacks deletion ownership")
	}
	result, err := repository.store.Transact(ctx, []Condition{
		{Key: runnerKey(expected.RunnerID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(expected.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerRuntimeOwnershipKey(expected.RunnerID), ModRevision: evidence.Values[1].ModRevision},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), expected.RunnerID), ModRevision: keyValueRevision(evidence.Values[2])},
	}, []Mutation{{Type: MutationDelete, Key: runnerRuntimeOwnershipKey(expected.RunnerID)}})
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, stateConflict("runner runtime cleanup ownership", expected.RunnerID)
	}
	return result.Revision, nil
}

func runnerKey(id string) string { return runnerPrefix + id }

func runnerLifecycleKey(id string) string { return runnerLifecyclePrefix + id }

func runnerRuntimeOwnershipKey(id string) string { return runnerRuntimeOwnershipPrefix + id }

func runnerObservationKey(id string) string { return runnerObservationPrefix + id }

func runnerOwnerPrefix(kind RunnerOwnerKind, ownerID string) string {
	return "/v1/indexes/runners/by-owner/" + string(kind) + "/" + ownerID + "/"
}

func runnerOwnerKey(kind RunnerOwnerKind, ownerID string, runnerID string) string {
	return runnerOwnerPrefix(kind, ownerID) + runnerID
}

func runnerTenantSlugKey(tenantID string, slug string) string {
	return "/v1/indexes/runners/by-slug/tenant/" + tenantID + "/" + encodeDynamicSegment(slug)
}

func runnerTenantCursorPrefix(tenantID string) string {
	return "/v1/cursors/runners/by-tenant/" + tenantID + "/"
}

func (repository *RunnerRepository) ResolveRunner(
	ctx context.Context,
	tenantID string,
	reference string,
) (Versioned[RunnerRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if err := validateID(ids.KindTenant, tenantID); err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if ids.Validate(ids.KindRunner, reference) == nil {
		current, err := repository.GetRunner(ctx, reference)
		if err != nil {
			return Versioned[RunnerRecord]{}, err
		}
		if current.Record.Desired.TenantID != tenantID {
			return Versioned[RunnerRecord]{}, errs.New(errs.KindScopeUnauthorized, "Runner is outside the Tenant scope")
		}
		return current, nil
	}
	if reference == "" {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindRunnerNotFound, "Runner was not found")
	}
	index, err := repository.store.Get(ctx, runnerTenantSlugKey(tenantID, reference))
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if index == nil || index.Entry == nil {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindRunnerNotFound, "Runner was not found")
	}
	id := string(index.Entry.Value)
	if ids.Validate(ids.KindRunner, id) != nil {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "runner slug index is corrupt")
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{runnerKey(id), runnerLifecycleKey(id)}, Revision: index.ReadRevision,
	})
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if result == nil || len(result.Values) != 2 || result.Values[0] == nil || result.Values[1] == nil {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "runner slug index is corrupt")
	}
	record, err := decodeRunnerAggregate(result.Values[0], result.Values[1])
	if err != nil || record.Desired.ID != id || record.Desired.TenantID != tenantID || record.Desired.Slug != reference {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "runner slug index is corrupt")
	}
	return Versioned[RunnerRecord]{Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision}, nil
}

type runnerParents struct {
	tenant  Versioned[TenantRecord]
	project Versioned[ProjectRecord]
}

func (repository *RunnerRepository) resolveRunnerParents(
	ctx context.Context,
	desired RunnerDesiredRecord,
) (runnerParents, error) {
	if err := validateRunnerDesired(desired); err != nil {
		return runnerParents{}, err
	}
	hierarchy, err := newHierarchyRepository(repository.store)
	if err != nil {
		return runnerParents{}, err
	}
	tenant, err := hierarchy.GetTenant(ctx, desired.TenantID)
	if err != nil {
		return runnerParents{}, err
	}
	parents := runnerParents{tenant: tenant}
	if desired.OwnerKind == RunnerOwnerProject {
		parents.project, err = hierarchy.GetProject(ctx, desired.OwnerID)
		if err != nil {
			return runnerParents{}, err
		}
		if parents.project.Record.Kind != ProjectKindTenant ||
			parents.project.Record.TenantID != desired.TenantID {
			return runnerParents{}, errs.New(
				errs.KindValidationFailed,
				"runner project owner does not belong to its tenant",
			)
		}
	}
	return parents, nil
}

func (repository *RunnerRepository) ReplaceRunnerSlugIdempotent(
	ctx context.Context,
	runnerID string,
	slug string,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if ids.Validate(ids.KindRunner, runnerID) != nil || validateRunnerSlug(slug) != nil ||
		marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.Method != http.MethodPatch || marker.Locator.Route != "/runners/{id}" ||
		marker.ReplayTarget == nil || marker.ReplayTarget.Kind != IdempotencyReplayTargetRunner ||
		marker.ReplayTarget.ID != runnerID || validateIdempotencyMarker(marker) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "runner slug mutation is invalid")
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
	current, err := repository.GetRunner(ctx, runnerID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRunnerMarkerScope(current.Record.Desired, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	secondaryKeys := []string{
		runnerTenantSlugKey(current.Record.Desired.TenantID, current.Record.Desired.Slug),
		deletionTombstoneKey(string(DeletionTargetRunner), runnerID),
	}
	renaming := current.Record.Desired.Slug != slug
	if renaming {
		secondaryKeys = append(secondaryKeys, runnerTenantSlugKey(current.Record.Desired.TenantID, slug))
	}
	secondary, err := repository.store.GetMany(ctx, GetManyRequest{Keys: secondaryKeys, Revision: current.ReadRevision})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if secondary == nil || len(secondary.Values) != len(secondaryKeys) || secondary.Values[0] == nil ||
		string(secondary.Values[0].Value) != runnerID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "runner slug index is missing or mismatched")
	}
	if secondary.Values[1] != nil || current.Record.ProvisioningState == RunnerProvisioningProvisioning {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "runner mutation is in progress")
	}
	if renaming && secondary.Values[2] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindSlugConflict, "runner slug is already in use")
	}
	replacement := cloneRunnerRecord(current.Record)
	replacement.Desired.Slug = slug
	value, err := encodeRunnerDesiredRecord(replacement.Desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	conditions := []Condition{
		{Key: runnerKey(runnerID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(runnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: runnerTenantSlugKey(current.Record.Desired.TenantID, current.Record.Desired.Slug), ModRevision: secondary.Values[0].ModRevision},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), runnerID)},
	}
	mutations := []Mutation{{Type: MutationPut, Key: runnerKey(runnerID), Value: value}}
	if renaming {
		conditions = append(conditions, Condition{Key: runnerTenantSlugKey(current.Record.Desired.TenantID, slug)})
		mutations = append(mutations,
			Mutation{Type: MutationDelete, Key: runnerTenantSlugKey(current.Record.Desired.TenantID, current.Record.Desired.Slug)},
			Mutation{Type: MutationPut, Key: runnerTenantSlugKey(current.Record.Desired.TenantID, slug), Value: []byte(runnerID)},
		)
	}
	plan, err := newIdempotencyMutationPlan(conditions, mutations, func(_ int64, values []*KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "runner slug mutation compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.New(errs.KindRunnerNotFound, "Runner was not found")
		}
		if values[3] != nil {
			return errs.New(errs.KindResourceInUse, "runner mutation is in progress")
		}
		if renaming && values[4] != nil {
			return errs.New(errs.KindSlugConflict, "runner slug is already in use")
		}
		return stateConflict("runner", runnerID)
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

// CreateRunnerWithTask atomically publishes the provisioning Runner, its one
// owner index, combined Tenant quota, exact host/network allocations, Task,
// queue membership, and idempotency marker before any host effect can start.
func (repository *RunnerRepository) CreateRunnerWithTask(
	ctx context.Context,
	config RunnerAllocationConfig,
	desired RunnerDesiredRecord,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	desired, err := NormalizeRunnerDesired(desired)
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
	record, err := NewProvisioningRunner(desired, allocation, task.ID, task.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	values, err := encodeRunnerCreateValues(record, quota, allocationState.host.record, systemPool, task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer values.clear()
	layout := newRunnerCreateEvidence(desired, parents, allocationState, task)
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: values.task},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: values.reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: values.reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: values.reference},
		{Type: MutationPut, Key: runnerKey(desired.ID), Value: values.runner},
		{Type: MutationPut, Key: runnerLifecycleKey(desired.ID), Value: values.lifecycle},
		{Type: MutationPut, Key: runnerTenantSlugKey(desired.TenantID, desired.Slug), Value: []byte(desired.ID)},
		{
			Type: MutationPut, Key: runnerOwnerKey(desired.OwnerKind, desired.OwnerID, desired.ID),
			Value: []byte(desired.ID),
		},
		{Type: MutationPut, Key: runnerTenantQuotaKey(desired.TenantID), Value: values.quota},
		{Type: MutationPut, Key: runnerHostSlotKey(allocationState.host.slot), Value: values.host},
		{Type: MutationPut, Key: systemPoolRegistryKey, Value: values.system},
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

func newRunnerTaskInitiation(
	desired RunnerDesiredRecord,
	parents runnerParents,
	actor TaskActor,
) (TaskInitiation, error) {
	owner, err := runnerTaskOwner(desired)
	if err != nil {
		return TaskInitiation{}, err
	}
	fences := []Condition{{Key: tenantKey(desired.TenantID), ModRevision: parents.tenant.Revision}}
	if desired.OwnerKind == RunnerOwnerProject {
		fences = append(fences, Condition{Key: projectKey(desired.OwnerID), ModRevision: parents.project.Revision})
	}
	return newTaskInitiation(owner, actor, fences...)
}

type runnerCreateValues struct {
	runner, lifecycle, quota, host, system, task, reference []byte
}

func encodeRunnerCreateValues(
	record RunnerRecord,
	quota RunnerTenantQuota,
	host RunnerHostSlotRecord,
	system SystemPoolRegistry,
	task TaskRecord,
) (runnerCreateValues, error) {
	var result runnerCreateValues
	var err error
	result.runner, err = encodeRunnerDesiredRecord(record.Desired)
	if err != nil {
		return result, err
	}
	result.lifecycle, err = encodeRunnerLifecycleRecord(record.RunnerLifecycleRecord)
	if err != nil {
		result.clear()
		return runnerCreateValues{}, err
	}
	result.quota, err = encodeEnvelope("runner_tenant_quota", quota)
	if err != nil {
		result.clear()
		return runnerCreateValues{}, err
	}
	result.host, err = encodeRunnerHostSlotRecord(host)
	if err != nil {
		result.clear()
		return runnerCreateValues{}, err
	}
	result.system, err = encodeEnvelope("system_pool_registry", system)
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
	conditions      []Condition
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
	desired         RunnerDesiredRecord
	parents         runnerParents
	allocation      runnerAllocationState
	operationID     string
}

func newRunnerCreateEvidence(
	desired RunnerDesiredRecord,
	parents runnerParents,
	allocation runnerAllocationState,
	task TaskRecord,
) runnerCreateEvidence {
	evidence := runnerCreateEvidence{
		project: -1, projectDeletion: -1, desired: desired, parents: parents,
		allocation: allocation, operationID: task.OperationID,
	}
	add := func(condition Condition) int {
		index := len(evidence.conditions)
		evidence.conditions = append(evidence.conditions, condition)
		return index
	}
	evidence.task = add(Condition{Key: taskKey(task.ID)})
	evidence.operation = add(Condition{Key: taskOperationIndexKey(task.OperationID, task.ID)})
	evidence.active = add(Condition{Key: taskActiveOperationKey(task.OperationID)})
	evidence.queue = add(Condition{Key: taskQueueKey(task.Executor, task.ID)})
	evidence.runner = add(Condition{Key: runnerKey(desired.ID)})
	evidence.lifecycle = add(Condition{Key: runnerLifecycleKey(desired.ID)})
	evidence.slug = add(Condition{Key: runnerTenantSlugKey(desired.TenantID, desired.Slug)})
	evidence.owner = add(Condition{Key: runnerOwnerKey(desired.OwnerKind, desired.OwnerID, desired.ID)})
	evidence.quota = add(Condition{Key: runnerTenantQuotaKey(desired.TenantID), ModRevision: allocation.quota.Revision})
	evidence.system = add(Condition{Key: systemPoolRegistryKey, ModRevision: allocation.system.Revision})
	evidence.tenant = add(Condition{Key: tenantKey(desired.TenantID), ModRevision: parents.tenant.Revision})
	evidence.runnerDeletion = add(Condition{Key: deletionTombstoneKey(string(DeletionTargetRunner), desired.ID)})
	evidence.tenantDeletion = add(Condition{Key: deletionTombstoneKey(string(DeletionTargetTenant), desired.TenantID)})
	if desired.OwnerKind == RunnerOwnerProject {
		evidence.project = add(Condition{Key: projectKey(desired.OwnerID), ModRevision: parents.project.Revision})
		evidence.projectDeletion = add(
			Condition{Key: deletionTombstoneKey(string(DeletionTargetProject), desired.OwnerID)},
		)
	}
	evidence.host = add(allocation.host.condition)
	return evidence
}

func (evidence runnerCreateEvidence) classifier() idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
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
			return errs.New(errs.KindSlugConflict, "runner slug is already in use")
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

func validateRunnerCreateTask(desired RunnerDesiredRecord, task TaskRecord) error {
	owner, err := runnerTaskOwner(desired)
	if err != nil {
		return err
	}
	if task.Executor != TaskExecutorController || task.Type != TaskCreate || task.Target != desired.ID ||
		task.Owner != owner ||
		task.Status != TaskStatusPending || task.IdempotencyKey == "" || len(task.Params) != 2 ||
		task.Params[TaskResourceKindParam] != TaskResourceRunner ||
		task.Params[RunnerRegistrationTokenPresentParam] != "true" {
		return errs.New(errs.KindValidationFailed, "runner creation task has invalid durable input")
	}
	return nil
}

func runnerTaskOwner(desired RunnerDesiredRecord) (TaskOwner, error) {
	if err := validateRunnerOwnership(desired); err != nil {
		return TaskOwner{}, err
	}
	if desired.OwnerKind == RunnerOwnerTenant {
		return TenantTaskOwner(desired.TenantID)
	}
	return TenantProjectTaskOwner(desired.TenantID, desired.OwnerID)
}

func validateRunnerCreateMarker(desired RunnerDesiredRecord, task TaskRecord, marker IdempotencyMarker) error {
	return validateRunnerOperationMarker(desired, task, marker, http.MethodPost, "/runners", nil, true)
}

func validateRunnerRetryMarker(desired RunnerDesiredRecord, task TaskRecord, marker IdempotencyMarker) error {
	if err := validateRunnerRetryMarkerEnvelope(task, marker); err != nil {
		return err
	}
	return validateRunnerMarkerScope(desired, marker)
}

func validateRunnerDeleteMarker(desired RunnerDesiredRecord, task TaskRecord, marker IdempotencyMarker) error {
	target := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetRunner, ID: desired.ID}
	return validateRunnerOperationMarker(desired, task, marker, http.MethodDelete, "/runners/{id}", &target, true)
}

func validateRunnerOperationMarker(
	desired RunnerDesiredRecord,
	task TaskRecord,
	marker IdempotencyMarker,
	method string,
	route string,
	replayTarget *IdempotencyReplayTarget,
	requireOperationKey bool,
) error {
	if err := validateRunnerMarkerEnvelope(task, marker, method, route, replayTarget, requireOperationKey); err != nil {
		return err
	}
	return validateRunnerMarkerScope(desired, marker)
}

func validateRunnerRetryMarkerEnvelope(task TaskRecord, marker IdempotencyMarker) error {
	if marker.Locator.ScopeKind != IdempotencyScopeTenant && marker.Locator.ScopeKind != IdempotencyScopeProject {
		return errs.New(errs.KindValidationFailed, "runner retry marker scope is invalid")
	}
	return validateRunnerMarkerEnvelope(task, marker, http.MethodPost, "/runners/{id}/retry", nil, false)
}

func validateRunnerMarkerEnvelope(
	task TaskRecord,
	marker IdempotencyMarker,
	method string,
	route string,
	replayTarget *IdempotencyReplayTarget,
	requireOperationKey bool,
) error {
	if task.IdempotencyKey == "" || (requireOperationKey && task.IdempotencyKey != marker.Locator.Key) ||
		marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.Method != method || marker.Locator.Route != route ||
		!validTaskResponse(marker.Response, task.ID) ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) ||
		!marker.TerminalAt.IsZero() || !marker.RetainUntil.IsZero() ||
		!runnerReplayTargetsEqual(marker.ReplayTarget, replayTarget) {
		return errs.New(errs.KindValidationFailed, "runner task marker does not match its task")
	}
	return validateIdempotencyMarker(marker)
}

func validateRunnerMarkerScope(desired RunnerDesiredRecord, marker IdempotencyMarker) error {
	scopeKind := IdempotencyScopeTenant
	if desired.OwnerKind == RunnerOwnerProject {
		scopeKind = IdempotencyScopeProject
	}
	if marker.Locator.ScopeKind != scopeKind || marker.Locator.ScopeID != desired.OwnerID {
		return errs.New(errs.KindValidationFailed, "runner task marker does not match its owner-scoped task")
	}
	return nil
}

func runnerReplayTargetsEqual(left *IdempotencyReplayTarget, right *IdempotencyReplayTarget) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func bindRunnerTaskMarker(task TaskRecord, marker IdempotencyMarker) TaskRecord {
	task = cloneTaskRecord(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	return task
}

func (repository *RunnerRepository) GetRunner(ctx context.Context, id string) (Versioned[RunnerRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if err := validateID(ids.KindRunner, id); err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{runnerKey(id), runnerLifecycleKey(id)},
	})
	if err != nil {
		return Versioned[RunnerRecord]{}, err
	}
	if result == nil || len(result.Values) != 2 {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "runner read is incomplete")
	}
	if result.Values[0] == nil && result.Values[1] == nil {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindRunnerNotFound, "Runner was not found")
	}
	if result.Values[0] == nil || result.Values[1] == nil {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "runner desired/lifecycle pair is incomplete")
	}
	record, err := decodeRunnerAggregate(result.Values[0], result.Values[1])
	if err != nil || record.Desired.ID != id {
		return Versioned[RunnerRecord]{}, errs.New(errs.KindInternal, "runner desired/lifecycle pair is corrupt")
	}
	return Versioned[RunnerRecord]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func decodeRunnerAggregate(desiredValue *KeyValue, lifecycleValue *KeyValue) (RunnerRecord, error) {
	if desiredValue == nil || lifecycleValue == nil || desiredValue.ModRevision <= 0 || lifecycleValue.ModRevision <= 0 {
		return RunnerRecord{}, errs.New(errs.KindInternal, "runner desired/lifecycle pair is incomplete")
	}
	desired, err := decodeRunnerDesiredRecord(desiredValue.Value)
	if err != nil {
		return RunnerRecord{}, err
	}
	lifecycle, err := decodeRunnerLifecycleRecord(lifecycleValue.Value)
	if err != nil || lifecycle.RunnerID != desired.ID {
		return RunnerRecord{}, errs.New(errs.KindInternal, "runner desired/lifecycle pair is corrupt")
	}
	return RunnerRecord{
		Desired: desired, RunnerLifecycleRecord: lifecycle, LifecycleRevision: lifecycleValue.ModRevision,
	}, nil
}

func (repository *RunnerRepository) ListRunners(
	ctx context.Context,
	filter RunnerFilter,
	request PageRequest,
) (Page[RunnerRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Page[RunnerRecord]{}, err
	}
	if (filter.TenantID == "") == (filter.ProjectID == "") {
		return Page[RunnerRecord]{}, errs.New(
			errs.KindValidationFailed,
			"runner list requires exactly one tenant or project filter",
		)
	}
	if filter.ProjectID != "" {
		if err := validateID(ids.KindProject, filter.ProjectID); err != nil {
			return Page[RunnerRecord]{}, err
		}
		page, err := listIndexPage(
			ctx, repository.store, "runners", "project", filter.ProjectID,
			runnerOwnerPrefix(RunnerOwnerProject, filter.ProjectID), runnerKey, ids.KindRunner, request,
			decodeRunnerDesiredAggregate,
			func(record RunnerRecord) string { return record.Desired.ID },
			func(record RunnerRecord) bool {
				return record.Desired.OwnerKind == RunnerOwnerProject && record.Desired.OwnerID == filter.ProjectID
			},
		)
		if err != nil {
			return Page[RunnerRecord]{}, err
		}
		return repository.hydrateRunnerPage(ctx, page)
	}
	return repository.listTenantRunners(ctx, filter.TenantID, request)
}

func (repository *RunnerRepository) listTenantRunners(
	ctx context.Context,
	tenantID string,
	request PageRequest,
) (Page[RunnerRecord], error) {
	if err := validateID(ids.KindTenant, tenantID); err != nil {
		return Page[RunnerRecord]{}, err
	}
	prefix := runnerTenantCursorPrefix(tenantID)
	limit, revision, startKey, query, err := normalizePageRequest(
		request, "runners", "tenant", tenantID, prefix, ids.KindRunner,
	)
	if err != nil {
		return Page[RunnerRecord]{}, err
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{runnerTenantQuotaKey(tenantID)}, Revision: revision,
	})
	if err != nil {
		return Page[RunnerRecord]{}, err
	}
	if read == nil || len(read.Values) != 1 {
		return Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner tenant quota read is incomplete")
	}
	quota := RunnerTenantQuota{RunnerIDs: []string{}}
	if read.Values[0] != nil {
		quota, err = decodeRunnerTenantQuota(read.Values[0].Value)
		if err != nil || validateRunnerTenantQuota(quota) != nil {
			return Page[RunnerRecord]{}, corruptRunnerTenantQuota()
		}
	}
	startID := strings.TrimPrefix(startKey, prefix)
	start := 0
	if startID != "" {
		start = sort.SearchStrings(quota.RunnerIDs, startID)
		if start < len(quota.RunnerIDs) && quota.RunnerIDs[start] == startID {
			start++
		}
	}
	end := start + limit
	if end > len(quota.RunnerIDs) {
		end = len(quota.RunnerIDs)
	}
	idsPage := quota.RunnerIDs[start:end]
	keys := make([]string, len(idsPage))
	for index, runnerID := range idsPage {
		keys[index] = runnerKey(runnerID)
	}
	page := Page[RunnerRecord]{Items: []Versioned[RunnerRecord]{}, Revision: read.ReadRevision}
	if len(keys) != 0 {
		records, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: read.ReadRevision})
		if err != nil {
			return Page[RunnerRecord]{}, err
		}
		if records == nil || len(records.Values) != len(keys) {
			return Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner tenant membership is corrupt")
		}
		for index, value := range records.Values {
			if value == nil {
				return Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner tenant membership is corrupt")
			}
			record, err := decodeRunnerDesiredAggregate(value.Value)
			if err != nil || record.Desired.ID != idsPage[index] || record.Desired.TenantID != tenantID {
				return Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner tenant membership is corrupt")
			}
			page.Items = append(page.Items, Versioned[RunnerRecord]{
				Record: record, Revision: value.ModRevision, ReadRevision: records.ReadRevision,
			})
		}
	}
	if end < len(quota.RunnerIDs) {
		page.NextCursor, err = encodeCursor(cursorPayload{
			Version: cursorVersion, Revision: read.ReadRevision, LastID: quota.RunnerIDs[end-1], Query: query,
		})
		if err != nil {
			return Page[RunnerRecord]{}, err
		}
	}
	return repository.hydrateRunnerPage(ctx, page)
}

func (repository *RunnerRepository) hydrateRunnerPage(
	ctx context.Context,
	page Page[RunnerRecord],
) (Page[RunnerRecord], error) {
	if len(page.Items) == 0 {
		return page, nil
	}
	keys := make([]string, len(page.Items))
	for index := range page.Items {
		keys[index] = runnerLifecycleKey(page.Items[index].Record.Desired.ID)
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: page.Revision})
	if err != nil {
		return Page[RunnerRecord]{}, err
	}
	if result == nil || len(result.Values) != len(keys) {
		return Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner lifecycle page is incomplete")
	}
	for index, value := range result.Values {
		if value == nil {
			return Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner lifecycle page is incomplete")
		}
		lifecycle, err := decodeRunnerLifecycleRecord(value.Value)
		if err != nil || lifecycle.RunnerID != page.Items[index].Record.Desired.ID {
			return Page[RunnerRecord]{}, errs.New(errs.KindInternal, "runner lifecycle page is corrupt")
		}
		page.Items[index].Record.RunnerLifecycleRecord = lifecycle
		page.Items[index].Record.LifecycleRevision = value.ModRevision
	}
	return page, nil
}

func (repository *RunnerRepository) PutRunnerObservation(
	ctx context.Context,
	record RunnerObservationRecord,
	expectedRevision int64,
) (Versioned[RunnerObservationRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerObservationRecord]{}, err
	}
	if err := validateRunnerObservation(record); err != nil {
		return Versioned[RunnerObservationRecord]{}, err
	}
	current, err := repository.GetRunner(ctx, record.RunnerID)
	if err != nil {
		return Versioned[RunnerObservationRecord]{}, err
	}
	parents, err := repository.resolveRunnerParents(ctx, current.Record.Desired)
	if err != nil {
		return Versioned[RunnerObservationRecord]{}, err
	}
	observation, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{runnerObservationKey(record.RunnerID)},
	})
	if err != nil {
		return Versioned[RunnerObservationRecord]{}, err
	}
	if observation == nil || len(observation.Values) != 1 ||
		revisionChanged(observation.Values[0], expectedRevision) {
		return Versioned[RunnerObservationRecord]{}, stateConflict("runner observation", record.RunnerID)
	}
	value, err := encodeRunnerObservation(record)
	if err != nil {
		return Versioned[RunnerObservationRecord]{}, err
	}
	defer clear(value)
	conditions := []Condition{
		{Key: runnerObservationKey(record.RunnerID), ModRevision: expectedRevision},
		{Key: runnerKey(record.RunnerID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(record.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: tenantKey(current.Record.Desired.TenantID), ModRevision: parents.tenant.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), record.RunnerID)},
		{Key: deletionTombstoneKey(string(DeletionTargetTenant), current.Record.Desired.TenantID)},
	}
	if current.Record.Desired.OwnerKind == RunnerOwnerProject {
		conditions = append(conditions,
			Condition{Key: projectKey(current.Record.Desired.OwnerID), ModRevision: parents.project.Revision},
			Condition{Key: deletionTombstoneKey(string(DeletionTargetProject), current.Record.Desired.OwnerID)},
		)
	}
	result, err := repository.store.Transact(ctx, conditions, []Mutation{{
		Type: MutationPut, Key: runnerObservationKey(record.RunnerID), Value: value,
	}})
	if err != nil {
		return Versioned[RunnerObservationRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[RunnerObservationRecord]{}, stateConflict("runner observation", record.RunnerID)
	}
	return Versioned[RunnerObservationRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *RunnerRepository) GetRunnerObservation(
	ctx context.Context,
	runnerID string,
) (Versioned[RunnerObservationRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RunnerObservationRecord]{}, false, err
	}
	if err := validateID(ids.KindRunner, runnerID); err != nil {
		return Versioned[RunnerObservationRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, runnerObservationKey(runnerID))
	if err != nil {
		return Versioned[RunnerObservationRecord]{}, false, err
	}
	if result == nil {
		return Versioned[RunnerObservationRecord]{}, false, errs.New(
			errs.KindInternal,
			"runner observation read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[RunnerObservationRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := decodeRunnerObservation(result.Entry.Value)
	if err != nil || record.RunnerID != runnerID {
		return Versioned[RunnerObservationRecord]{}, false, errs.New(errs.KindInternal, "runner observation is corrupt")
	}
	return Versioned[RunnerObservationRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

type runnerAllocationState struct {
	quota  Versioned[RunnerTenantQuota]
	host   runnerHostSlotState
	system Versioned[SystemPoolRegistry]
}

type runnerHostSlotState struct {
	slot      uint32
	record    RunnerHostSlotRecord
	condition Condition
}

const (
	runnerAllocationQuotaIndex = iota
	runnerAllocationSystemIndex
	runnerAllocationHostStartIndex
)

func (repository *RunnerRepository) getRunnerAllocationState(
	ctx context.Context,
	tenantID string,
	runnerID string,
	config RunnerAllocationConfig,
	slotCount uint32,
) (runnerAllocationState, error) {
	keys := make([]string, 0, runnerAllocationHostStartIndex+int(slotCount))
	keys = append(keys, runnerTenantQuotaKey(tenantID), systemPoolRegistryKey)
	for slot := uint32(0); slot < slotCount; slot++ {
		keys = append(keys, runnerHostSlotKey(slot))
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: keys,
	})
	if err != nil {
		return runnerAllocationState{}, err
	}
	if result == nil || len(result.Values) != len(keys) {
		return runnerAllocationState{}, errs.New(errs.KindInternal, "runner allocation read is incomplete")
	}
	state := runnerAllocationState{
		quota: Versioned[RunnerTenantQuota]{
			Record: RunnerTenantQuota{RunnerIDs: []string{}}, ReadRevision: result.ReadRevision,
		},
	}
	if result.Values[runnerAllocationQuotaIndex] != nil {
		state.quota.Record, err = decodeRunnerTenantQuota(result.Values[runnerAllocationQuotaIndex].Value)
		if err != nil || validateRunnerTenantQuota(state.quota.Record) != nil {
			return runnerAllocationState{}, corruptRunnerTenantQuota()
		}
		state.quota.Revision = result.Values[runnerAllocationQuotaIndex].ModRevision
	}
	if result.Values[runnerAllocationSystemIndex] != nil {
		if len(result.Values[runnerAllocationSystemIndex].Value) > maximumRunnerPersistenceBytes {
			return runnerAllocationState{}, corruptSystemPoolRegistry()
		}
		state.system.Record, err = decodeSystemPoolRegistry(result.Values[runnerAllocationSystemIndex].Value)
		if err != nil || validateSystemPoolRegistry(config.SystemPool, state.system.Record) != nil ||
			state.system.Record.RunnerNetworkPool != config.RunnerPool.String() {
			return runnerAllocationState{}, corruptSystemPoolRegistry()
		}
		state.system.Revision = result.Values[runnerAllocationSystemIndex].ModRevision
	} else {
		return runnerAllocationState{}, errs.New(errs.KindStateConflict, "runner network pool is not reserved at bootstrap")
	}
	firstFree := int64(-1)
	owners := make(map[string]struct{}, slotCount)
	for slot := uint32(0); slot < slotCount; slot++ {
		value := result.Values[runnerAllocationHostStartIndex+int(slot)]
		if value == nil {
			if firstFree < 0 {
				firstFree = int64(slot)
			}
			continue
		}
		record, decodeErr := decodeRunnerHostSlotRecord(value.Value)
		if decodeErr != nil || record.Slot != slot {
			return runnerAllocationState{}, corruptRunnerHostSlotRecord()
		}
		if _, duplicate := owners[record.RunnerID]; duplicate {
			return runnerAllocationState{}, corruptRunnerHostSlotRecord()
		}
		owners[record.RunnerID] = struct{}{}
		if record.RunnerID == runnerID {
			return runnerAllocationState{}, stateConflict("runner", runnerID)
		}
	}
	selected := firstFree
	if selected < 0 {
		return runnerAllocationState{}, errs.New(errs.KindResourceInUse, "runner host allocation pool is exhausted")
	}
	state.host.slot = uint32(selected)
	state.host.record = RunnerHostSlotRecord{Slot: uint32(selected), RunnerID: runnerID}
	state.host.condition = Condition{Key: runnerHostSlotKey(uint32(selected))}
	return state, nil
}

func revisionChanged(value *KeyValue, expected int64) bool {
	return (expected == 0 && value != nil) ||
		(expected > 0 && (value == nil || value.ModRevision != expected))
}
