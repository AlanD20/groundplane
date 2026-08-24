package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskOwnerIndexesPublishRawIdentityAtTheTaskRevision(t *testing.T) {
	// Rationale: every publisher shares the Task idempotency planner, so the
	// primary and both immutable owner memberships must appear at one revision.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime()
	tenantID := ids.NewAt(ids.KindTenant, now, 2101)
	projectID := ids.NewAt(ids.KindProject, now, 2102)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2103)
	owner, err := TenantProjectTaskOwner(tenantID, projectID)
	if err != nil {
		t.Fatalf("TenantProjectTaskOwner() error = %v", err)
	}
	owner.EnvironmentID = environmentID
	task := validTaskRecord(now)
	task.Owner = owner
	createOwnedLifecycleTask(t, repository, task)

	keys := []string{
		taskKey(task.ID),
		taskWorkspaceTenantIndexKey(tenantID, task.ID),
		taskEnvironmentIndexKey(environmentID, task.ID),
	}
	result, err := store.GetMany(ctx, GetManyRequest{Keys: keys})
	if err != nil || len(result.Values) != len(keys) {
		t.Fatalf("GetMany(owner indexes) = %#v, %v", result, err)
	}
	for index, value := range result.Values {
		if value == nil || value.ModRevision != result.Values[0].ModRevision {
			t.Fatalf("owner publication[%d] = %#v", index, value)
		}
		if index > 0 && string(value.Value) != task.ID {
			t.Fatalf("owner index[%d] value = %q, want raw Task id", index, value.Value)
		}
	}
}

func TestTaskPruningRejectsMissingOwnerIndex(t *testing.T) {
	// Rationale: pruning must not erase a primary when its immutable owner
	// journal membership is already missing, because that split state is corrupt.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(time.Minute)
	task := validTaskRecord(now)
	createLifecycleTask(t, repository, task)
	finishedAt := now.Add(time.Second)
	if _, err := repository.AbortPendingTask(ctx, task.ID, finishedAt); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	pruneAt := finishedAt.Add(TaskRetention).Add(time.Nanosecond)
	if count, err := idempotency.PruneExpired(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpired(marker) = %d, %v", count, err)
	}
	key := taskWorkspacePlatformIndexKey(task.ID)
	indexed, err := store.Get(ctx, key)
	if err != nil || indexed.Entry == nil {
		t.Fatalf("Get(owner index) = %#v, %v", indexed, err)
	}
	transaction, err := store.Transact(ctx, []Condition{{
		Key: key, ModRevision: indexed.Entry.ModRevision,
	}}, []Mutation{{Type: MutationDelete, Key: key}})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("delete owner index = %#v, %v", transaction, err)
	}
	if _, err := repository.PruneExpiredTasks(ctx, pruneAt); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("PruneExpiredTasks(missing owner) error = %v, want internal", err)
	}
}

func TestGetTaskRejectsMissingTenantAndEnvironmentMemberships(t *testing.T) {
	// Rationale: a Task primary without either exact immutable owner membership is corrupt at the same MVCC view.
	ctx := context.Background()
	for _, missingEnvironment := range []bool{false, true} {
		t.Run(map[bool]string{false: "tenant", true: "environment"}[missingEnvironment], func(t *testing.T) {
			store := newMemoryTaskStore()
			repository, err := newTaskRepository(store)
			if err != nil {
				t.Fatalf("newTaskRepository() error = %v", err)
			}
			now := taskJournalTime()
			owner, err := TenantProjectTaskOwner(
				ids.NewAt(ids.KindTenant, now, 2201),
				ids.NewAt(ids.KindProject, now, 2202),
			)
			if err != nil {
				t.Fatalf("TenantProjectTaskOwner() error = %v", err)
			}
			owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 2203)
			task := validTaskRecord(now)
			task.Owner = owner
			createOwnedLifecycleTask(t, repository, task)
			key := taskWorkspaceTenantIndexKey(owner.TenantID, task.ID)
			if missingEnvironment {
				key = taskEnvironmentIndexKey(owner.EnvironmentID, task.ID)
			}
			membership, err := store.Get(ctx, key)
			if err != nil || membership.Entry == nil {
				t.Fatalf("Get(membership) = %#v, %v", membership, err)
			}
			result, err := store.Transact(ctx, []Condition{{
				Key: key, ModRevision: membership.Entry.ModRevision,
			}}, []Mutation{{Type: MutationDelete, Key: key}})
			if err != nil || !result.Succeeded {
				t.Fatalf("delete membership = %#v, %v", result, err)
			}
			if _, err := repository.GetTask(ctx, task.ID); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("GetTask(missing membership) error = %v, want internal", err)
			}
		})
	}
}

func TestTaskPruningDeletesTenantAndEnvironmentMemberships(t *testing.T) {
	// Rationale: pruning must remove the primary and every immutable owner membership in the same durable cleanup.
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime()
	owner, err := TenantProjectTaskOwner(
		ids.NewAt(ids.KindTenant, now, 2251),
		ids.NewAt(ids.KindProject, now, 2252),
	)
	if err != nil {
		t.Fatalf("TenantProjectTaskOwner() error = %v", err)
	}
	owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 2253)
	task := validTaskRecord(now)
	task.Owner = owner
	createOwnedLifecycleTask(t, repository, task)
	finishedAt := now.Add(time.Second)
	if _, err := repository.AbortPendingTask(ctx, task.ID, finishedAt); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	pruneAt := finishedAt.Add(TaskRetention).Add(time.Nanosecond)
	if count, err := idempotency.PruneExpired(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpired(marker) = %d, %v", count, err)
	}
	if count, err := repository.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks() = %d, %v", count, err)
	}
	for _, key := range []string{
		taskKey(task.ID),
		taskWorkspaceTenantIndexKey(owner.TenantID, task.ID),
		taskEnvironmentIndexKey(owner.EnvironmentID, task.ID),
	} {
		result, err := store.Get(ctx, key)
		if err != nil || result.Entry != nil {
			t.Fatalf("Get(pruned key %q) = %#v, %v", key, result, err)
		}
	}
}

func TestSystemInitiatedRetryPreservesSourceOwner(t *testing.T) {
	// Rationale: cascade authority changes only the retry actor; ADR 0039
	// requires the immediate source Task owner to persist.
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime()
	parent := validTaskRecord(now)
	parent.Executor = TaskExecutorController
	createLifecycleTask(t, repository, parent)
	if _, found, err := repository.ClaimNextControllerTask(ctx, now); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(parent) found/error = %v/%v", found, err)
	}
	initiation, err := repository.GetSystemTaskInitiation(ctx, parent.ID)
	if err != nil {
		t.Fatalf("GetSystemTaskInitiation() error = %v", err)
	}

	owner, err := TenantProjectTaskOwner(
		ids.NewAt(ids.KindTenant, now, 2301),
		ids.NewAt(ids.KindProject, now, 2302),
	)
	if err != nil {
		t.Fatalf("TenantProjectTaskOwner() error = %v", err)
	}
	owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 2303)
	source := validTaskRecord(now.Add(time.Second))
	source.Owner = owner
	createOwnedLifecycleTask(t, repository, source)
	terminalAt := source.CreatedAt.Add(time.Second)
	if _, err := repository.AbortPendingTask(ctx, source.ID, terminalAt); err != nil {
		t.Fatalf("AbortPendingTask(source) error = %v", err)
	}

	retryID := ids.NewAt(ids.KindTask, terminalAt.Add(time.Second), 2304)
	marker := pendingRetryMarker(source, retryID, terminalAt.Add(time.Second), "system-retry-key-0001")
	if _, err := repository.RetryTaskWithInitiation(ctx, source.ID, retryID, initiation, marker); err != nil {
		t.Fatalf("RetryTaskWithInitiation() error = %v", err)
	}
	retry, err := repository.GetTask(ctx, retryID)
	if err != nil {
		t.Fatalf("GetTask(retry) error = %v", err)
	}
	if retry.Record.Owner != source.Owner || retry.Record.Actor != TaskActorSystem {
		t.Fatalf(
			"system retry owner/actor = %#v/%q, want %#v/%q",
			retry.Record.Owner,
			retry.Record.Actor,
			source.Owner,
			TaskActorSystem,
		)
	}
}

func TestSystemInitiatedRetryRejectsChangedControllerParent(t *testing.T) {
	// Rationale: every variable parent-authority fence must remain
	// classifiable when a cascade retry loses its running parent CAS.
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime()
	parent := validTaskRecord(now)
	parent.Executor = TaskExecutorController
	createLifecycleTask(t, repository, parent)
	if _, found, err := repository.ClaimNextControllerTask(ctx, now); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(parent) found/error = %v/%v", found, err)
	}
	initiation, err := repository.GetSystemTaskInitiation(ctx, parent.ID)
	if err != nil {
		t.Fatalf("GetSystemTaskInitiation() error = %v", err)
	}
	source := validTaskRecord(now.Add(time.Second))
	createLifecycleTask(t, repository, source)
	finishedAt := source.CreatedAt.Add(time.Second)
	if _, err := repository.AbortPendingTask(ctx, source.ID, finishedAt); err != nil {
		t.Fatalf("AbortPendingTask(source) error = %v", err)
	}
	if _, err := repository.AcknowledgeControllerTask(
		ctx,
		parent.ID,
		TaskStatusCompleted,
		now.Add(3*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(parent) error = %v", err)
	}
	retryID := ids.NewAt(ids.KindTask, now.Add(4*time.Second), 2310)
	marker := pendingRetryMarker(source, retryID, now.Add(4*time.Second), "parent-fence-key-0001")
	result, err := repository.RetryTaskWithInitiation(ctx, source.ID, retryID, initiation, marker)
	if err != nil {
		t.Fatalf("RetryTaskWithInitiation() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || outcome != IdempotencyKnownConflict ||
		!errors.Is(conflict, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("RetryTaskWithInitiation(parent changed) = %v/%v/%v", outcome, conflict, classifyErr)
	}
}

func TestSystemTaskInitiationRejectsAgentExecutor(t *testing.T) {
	// Rationale: only a running Controller Task can authorize system children;
	// an Agent assignment is never cascade authority.
	ctx := context.Background()
	repository, err := newTaskRepository(newMemoryTaskStore())
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime()
	task := validTaskRecord(now)
	createLifecycleTask(t, repository, task)
	agentID := ids.NewAt(ids.KindAgent, now, 2311)
	if _, found, err := repository.ClaimNextTask(ctx, agentID, 1, now); err != nil || !found {
		t.Fatalf("ClaimNextTask(agent) found/error = %v/%v", found, err)
	}
	if _, err := repository.GetSystemTaskInitiation(ctx, task.ID); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("GetSystemTaskInitiation(agent) error = %v, want state_conflict", err)
	}
}

func TestTaskRetryScopeUsesDurableOwnerInsteadOfOriginalMarker(t *testing.T) {
	// Rationale: retry idempotency is domain-separated by the accepted durable
	// owner even when the original marker used stale scope.
	ctx := context.Background()
	repository, err := newTaskRepository(newMemoryTaskStore())
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime()
	owner, err := TenantProjectTaskOwner(
		ids.NewAt(ids.KindTenant, now, 2321),
		ids.NewAt(ids.KindProject, now, 2322),
	)
	if err != nil {
		t.Fatalf("TenantProjectTaskOwner() error = %v", err)
	}
	owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, now, 2323)
	task := validTaskRecord(now)
	task.Owner = owner
	createOwnedLifecycleTask(t, repository, task)
	scope, err := repository.GetTaskRetryScope(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTaskRetryScope() error = %v", err)
	}
	if scope != (TaskRetryScope{Kind: IdempotencyScopeEnvironment, ID: owner.EnvironmentID}) {
		t.Fatalf("GetTaskRetryScope() = %#v", scope)
	}
}

func TestEnvironmentTaskInitiationRejectsMissingAndChangedTenant(t *testing.T) {
	// Rationale: a tenant-owned Task may publish only while the complete
	// Tenant-to-Project-to-Environment ancestry remains current.
	ctx := context.Background()
	store := newMemoryTaskStore()
	now := taskJournalTime()
	tenantID := ids.NewAt(ids.KindTenant, now, 2331)
	project := Versioned[ProjectRecord]{
		Record: ProjectRecord{
			ID: ids.NewAt(ids.KindProject, now, 2332), TenantID: tenantID, Kind: ProjectKindTenant,
		},
	}
	environment := Versioned[EnvironmentRecord]{Record: EnvironmentRecord{
		ID: ids.NewAt(ids.KindEnvironment, now, 2333), ProjectID: project.Record.ID,
	}}
	if _, err := loadTaskInitiationTenant(ctx, store, project); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("loadTaskInitiationTenant(missing) error = %v, want state_conflict", err)
	}

	seedTaskRepositoryValue(t, store, tenantKey(tenantID), []byte("tenant-v1"))
	seedTaskRepositoryValue(t, store, projectKey(project.Record.ID), []byte("project-v1"))
	seedTaskRepositoryValue(t, store, environmentKey(environment.Record.ID), []byte("environment-v1"))
	tenantRead, err := store.Get(ctx, tenantKey(tenantID))
	if err != nil || tenantRead.Entry == nil {
		t.Fatalf("Get(Tenant) = %#v, %v", tenantRead, err)
	}
	projectRead, err := store.Get(ctx, projectKey(project.Record.ID))
	if err != nil || projectRead.Entry == nil {
		t.Fatalf("Get(Project) = %#v, %v", projectRead, err)
	}
	environmentRead, err := store.Get(ctx, environmentKey(environment.Record.ID))
	if err != nil || environmentRead.Entry == nil {
		t.Fatalf("Get(Environment) = %#v, %v", environmentRead, err)
	}
	tenant := &Versioned[TenantRecord]{
		Record: TenantRecord{ID: tenantID}, Revision: tenantRead.Entry.ModRevision,
	}
	project.Revision = projectRead.Entry.ModRevision
	environment.Revision = environmentRead.Entry.ModRevision
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, TaskActorOperator)
	if err != nil {
		t.Fatalf("newEnvironmentTaskInitiation() error = %v", err)
	}
	task := validTaskRecord(now.Add(time.Second))
	task.Owner, err = EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	marker := pendingTaskMarker(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		t.Fatalf("encodeTaskRecord() error = %v", err)
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	defer clear(reference)
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		[]Condition{
			{Key: taskKey(task.ID)},
			{Key: taskOperationIndexKey(task.OperationID, task.ID)},
			{Key: taskActiveOperationKey(task.OperationID)},
			{Key: taskQueueKey(task.Executor, task.ID)},
			{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
			{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		},
		[]Mutation{
			{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
			{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
			{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
			{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		},
		func(int64, []*KeyValue) error {
			return errs.New(errs.KindStateConflict, "base task publication changed")
		},
	)
	if err != nil {
		t.Fatalf("newTaskIdempotencyMutationPlan() error = %v", err)
	}
	changed, err := store.Transact(
		ctx,
		[]Condition{{Key: tenantKey(tenantID), ModRevision: tenant.Revision}},
		[]Mutation{{Type: MutationPut, Key: tenantKey(tenantID), Value: []byte("tenant-v2")}},
	)
	if err != nil || !changed.Succeeded {
		t.Fatalf("change Tenant = %#v, %v", changed, err)
	}
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	result, err := idempotency.Apply(ctx, marker, plan)
	if err != nil {
		t.Fatalf("Apply(changed Tenant) error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || outcome != IdempotencyKnownConflict ||
		!errors.Is(conflict, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Apply(changed Tenant) = %v/%v/%v", outcome, conflict, classifyErr)
	}
}

func mustEnvironmentTaskOwner(
	t *testing.T,
	project ProjectRecord,
	environment EnvironmentRecord,
) TaskOwner {
	t.Helper()
	owner, err := EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	return owner
}

func mustProjectTaskOwner(t *testing.T, project ProjectRecord) TaskOwner {
	t.Helper()
	owner, err := ProjectTaskOwner(project)
	if err != nil {
		t.Fatalf("ProjectTaskOwner() error = %v", err)
	}
	return owner
}

func createOwnedLifecycleTask(t *testing.T, repository *TaskRepository, task TaskRecord) {
	t.Helper()
	marker := pendingTaskMarker(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		t.Fatalf("encodeTaskRecord() error = %v", err)
	}
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
	}
	initiation, err := newTaskInitiation(task.Owner, task.Actor)
	if err != nil {
		t.Fatalf("newTaskInitiation() error = %v", err)
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		func(int64, []*KeyValue) error { return errs.New(errs.KindStateConflict, "test task exists") },
	)
	if err != nil {
		t.Fatalf("newTaskIdempotencyMutationPlan() error = %v", err)
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	result, err := idempotency.Apply(context.Background(), marker, plan)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("Apply() outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
}
