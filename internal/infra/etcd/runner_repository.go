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
	runnerPrefix            = "/v1/records/runners/"
	runnerObservationPrefix = "/v1/runtime/observations/runners/"
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

func runnerKey(id string) string { return runnerPrefix + id }

func runnerObservationKey(id string) string { return runnerObservationPrefix + id }

func runnerOwnerPrefix(kind RunnerOwnerKind, ownerID string) string {
	return "/v1/indexes/runners/by-owner/" + string(kind) + "/" + ownerID + "/"
}

func runnerOwnerKey(kind RunnerOwnerKind, ownerID string, runnerID string) string {
	return runnerOwnerPrefix(kind, ownerID) + runnerID
}

func runnerTenantCursorPrefix(tenantID string) string {
	return "/v1/cursors/runners/by-tenant/" + tenantID + "/"
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
			return runnerParents{}, errs.New(errs.KindValidationFailed, "runner project owner does not belong to its tenant")
		}
	}
	return parents, nil
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
	systemPool, subnet, err := allocationState.system.Record.ReserveRunner(config.SystemPool, desired.ID)
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
		{
			Type: MutationPut, Key: runnerOwnerKey(desired.OwnerKind, desired.OwnerID, desired.ID),
			Value: []byte(desired.ID),
		},
		{Type: MutationPut, Key: runnerTenantQuotaKey(desired.TenantID), Value: values.quota},
		{Type: MutationPut, Key: runnerHostSlotKey(allocationState.host.slot), Value: values.host},
		{Type: MutationPut, Key: systemPoolRegistryKey, Value: values.system},
	}
	plan, err := newTaskIdempotencyMutationPlan(layout.conditions, mutations, layout.classifier())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

type runnerCreateValues struct {
	runner, quota, host, system, task, reference []byte
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
	result.runner, err = encodeRunnerRecord(record)
	if err != nil {
		return result, err
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
	evidence.owner = add(Condition{Key: runnerOwnerKey(desired.OwnerKind, desired.OwnerID, desired.ID)})
	evidence.quota = add(Condition{Key: runnerTenantQuotaKey(desired.TenantID), ModRevision: allocation.quota.Revision})
	evidence.system = add(Condition{Key: systemPoolRegistryKey, ModRevision: allocation.system.Revision})
	evidence.tenant = add(Condition{Key: tenantKey(desired.TenantID), ModRevision: parents.tenant.Revision})
	evidence.runnerDeletion = add(Condition{Key: deletionTombstoneKey(string(DeletionTargetRunner), desired.ID)})
	evidence.tenantDeletion = add(Condition{Key: deletionTombstoneKey(string(DeletionTargetTenant), desired.TenantID)})
	if desired.OwnerKind == RunnerOwnerProject {
		evidence.project = add(Condition{Key: projectKey(desired.OwnerID), ModRevision: parents.project.Revision})
		evidence.projectDeletion = add(Condition{Key: deletionTombstoneKey(string(DeletionTargetProject), desired.OwnerID)})
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
			return errs.Newf(errs.KindStateConflict, "operation %s already has active task %s", evidence.operationID, activeTaskID)
		}
		for _, index := range []int{evidence.task, evidence.operation, evidence.queue} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "runner creation collided with durable task state")
			}
		}
		if values[evidence.runner] != nil || values[evidence.owner] != nil {
			return errs.New(errs.KindStateConflict, "runner stable identity is already in use")
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
	if task.Executor != TaskExecutorController || task.Type != TaskCreate || task.Target != desired.ID ||
		task.Status != TaskStatusPending || task.IdempotencyKey == "" || len(task.Params) != 2 ||
		task.Params[TaskResourceKindParam] != TaskResourceRunner ||
		task.Params[RunnerRegistrationTokenPresentParam] != "true" {
		return errs.New(errs.KindValidationFailed, "runner creation task has invalid durable input")
	}
	return nil
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
	return getRecord(
		ctx, repository.store, runnerKey(id), id, errs.KindRunnerNotFound,
		decodeRunnerRecord,
		func(record RunnerRecord) string { return record.Desired.ID },
	)
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
		return Page[RunnerRecord]{}, errs.New(errs.KindValidationFailed, "runner list requires exactly one tenant or project filter")
	}
	if filter.ProjectID != "" {
		if err := validateID(ids.KindProject, filter.ProjectID); err != nil {
			return Page[RunnerRecord]{}, err
		}
		return listIndexPage(
			ctx, repository.store, "runners", "project", filter.ProjectID,
			runnerOwnerPrefix(RunnerOwnerProject, filter.ProjectID), runnerKey, ids.KindRunner, request,
			decodeRunnerRecord,
			func(record RunnerRecord) string { return record.Desired.ID },
			func(record RunnerRecord) bool {
				return record.Desired.OwnerKind == RunnerOwnerProject && record.Desired.OwnerID == filter.ProjectID
			},
		)
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
		quota, err = decodeEnvelope[RunnerTenantQuota](read.Values[0].Value, "runner_tenant_quota")
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
			record, err := decodeRunnerRecord(value.Value)
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
		return Versioned[RunnerObservationRecord]{}, false, errs.New(errs.KindInternal, "runner observation read is empty")
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
		system: Versioned[SystemPoolRegistry]{
			Record: SystemPoolRegistry{Reservations: map[string]string{}}, ReadRevision: result.ReadRevision,
		},
	}
	if result.Values[runnerAllocationQuotaIndex] != nil {
		state.quota.Record, err = decodeEnvelope[RunnerTenantQuota](
			result.Values[runnerAllocationQuotaIndex].Value, "runner_tenant_quota",
		)
		if err != nil || validateRunnerTenantQuota(state.quota.Record) != nil {
			return runnerAllocationState{}, corruptRunnerTenantQuota()
		}
		state.quota.Revision = result.Values[runnerAllocationQuotaIndex].ModRevision
	}
	if result.Values[runnerAllocationSystemIndex] != nil {
		state.system.Record, err = decodeEnvelope[SystemPoolRegistry](
			result.Values[runnerAllocationSystemIndex].Value, "system_pool_registry",
		)
		if err != nil || validateSystemPoolRegistry(config.SystemPool, state.system.Record) != nil {
			return runnerAllocationState{}, corruptSystemPoolRegistry()
		}
		state.system.Revision = result.Values[runnerAllocationSystemIndex].ModRevision
	} else if err := validateSystemPoolRegistry(config.SystemPool, state.system.Record); err != nil {
		return runnerAllocationState{}, err
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
