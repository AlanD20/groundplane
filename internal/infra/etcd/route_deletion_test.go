package etcd

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestRouteRepositoryBeginsRemovalInOneTransaction(t *testing.T) {
	// Rationale: public Route visibility, its immutable suppression candidate,
	// Task journal, deletion fence, and reconciliation ownership must never
	// describe different removal attempts after a Controller crash.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1100, "/api/*")
	current, err := repository.CreateRoute(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	projection := routeDeletionTestProjection(t, store, current)
	task, marker, tombstone, intent := routeDeletionTestRecords(t, project, environment, current, projection)
	result, err := repository.BeginRouteDeletionWithTask(
		ctx, environment, project, target, current, &projection, tombstone, intent, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginRouteDeletionWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("BeginRouteDeletionWithTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	visible, err := repository.GetRoute(ctx, current.Record.Desired.ID)
	if err != nil || visible.Record != current.Record {
		t.Fatalf("GetRoute(fenced) = %#v, %v", visible, err)
	}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	storedIntent, found, err := hierarchy.GetRouteRemovalIntent(ctx, task.ID)
	if err != nil || !found || !sameRouteRemovalProjection(
		*storedIntent.Record.CandidateProjection,
		*intent.CandidateProjection,
	) {
		t.Fatalf("GetRouteRemovalIntent() = %#v/%v/%v", storedIntent, found, err)
	}
	storedTombstone, found, err := hierarchy.GetDeletionTombstone(ctx, DeletionTargetRoute, task.Target)
	if err != nil || !found || storedTombstone.Record.TaskID != task.ID {
		t.Fatalf("GetDeletionTombstone() = %#v/%v/%v", storedTombstone, found, err)
	}
	companions, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		taskKey(task.ID),
		taskOperationIndexKey(task.OperationID, task.ID),
		taskActiveOperationKey(task.OperationID),
		taskQueueKey(task.Executor, task.ID),
		componentTaskActiveEnvironmentKey(environment.Record.ID),
	}})
	if err != nil || companions == nil || len(companions.Values) != 5 {
		t.Fatalf("GetMany(Task companions) = %#v, %v", companions, err)
	}
	for index, companion := range companions.Values {
		if companion == nil {
			t.Fatalf("Task companion %d is missing", index)
		}
	}
	if string(companions.Values[4].Value) != task.ID {
		t.Fatalf("Environment reconciliation owner = %q, want %q", companions.Values[4].Value, task.ID)
	}
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	locator, found, err := idempotency.ResolveReplayLocator(
		ctx,
		*marker.ReplayTarget,
		marker.Locator.Method,
		marker.Locator.Route,
		marker.Locator.Key,
	)
	if err != nil || !found || locator != marker.Locator {
		t.Fatalf("ResolveReplayLocator() = %#v/%v/%v", locator, found, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != task.ID {
		t.Fatalf("ClaimNextControllerTask() = %#v/%v/%v", claim, found, err)
	}
	terminalAt := task.CreatedAt.Add(2 * time.Second)
	terminal, err := tasks.AcknowledgeControllerTask(ctx, task.ID, TaskStatusCompleted, terminalAt)
	if err != nil || terminal.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask() = %#v/%v", terminal, err)
	}
	for _, key := range []string{
		routeKey(task.Target),
		routeOwnerKey(current.Record.EnvironmentID, task.Target),
		routeMatchKey(current.Record.EnvironmentID, current.Record.Desired.Host, current.Record.Desired.Path),
		deletionTombstoneKey(string(DeletionTargetRoute), task.Target),
		componentTaskActiveEnvironmentKey(environment.Record.ID),
	} {
		stored, getErr := store.Get(ctx, key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("finalized key %s = %#v/%v", key, stored, getErr)
		}
	}
	projectionRead, err := store.Get(ctx, environmentComposeProjectionKey(environment.Record.ID))
	if err != nil || projectionRead.Entry == nil {
		t.Fatalf("Get(promoted projection) = %#v/%v", projectionRead, err)
	}
	promoted, err := decodeEnvironmentComposeProjection(projectionRead.Entry.Value)
	if err != nil || !sameRouteRemovalProjection(promoted, *intent.CandidateProjection) {
		t.Fatalf("promoted projection = %#v/%v", promoted, err)
	}
	terminalIntent, found, err := hierarchy.GetRouteRemovalIntent(ctx, task.ID)
	if err != nil || !found || terminalIntent.Record.Status != TaskStatusCompleted ||
		terminalIntent.Record.TerminalAt == nil || !terminalIntent.Record.TerminalAt.Equal(terminalAt) {
		t.Fatalf("GetRouteRemovalIntent(terminal) = %#v/%v/%v", terminalIntent, found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, TaskStatusCompleted, terminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(replay) error = %v", err)
	}
}

func TestRouteRepositoryRejectsRemovalDuringEnvironmentReconciliation(t *testing.T) {
	// Rationale: a Route suppression candidate must never race another writer
	// that can replace the same applied Environment projection.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1110, "/admin/*")
	current, err := repository.CreateRoute(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	projection := routeDeletionTestProjection(t, store, current)
	fenceResult, err := store.Transact(ctx, []Condition{{
		Key: componentTaskActiveEnvironmentKey(environment.Record.ID),
	}}, []Mutation{{
		Type:  MutationPut,
		Key:   componentTaskActiveEnvironmentKey(environment.Record.ID),
		Value: []byte(ids.NewAt(ids.KindTask, serviceRecordTestTime(), 1111)),
	}})
	if err != nil || !fenceResult.Succeeded {
		t.Fatalf("seed Environment reconciliation fence = %#v/%v", fenceResult, err)
	}
	task, marker, tombstone, intent := routeDeletionTestRecords(t, project, environment, current, projection)
	result, err := repository.BeginRouteDeletionWithTask(
		ctx, environment, project, target, current, &projection, tombstone, intent, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginRouteDeletionWithTask() error = %v", err)
	}
	_, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || !isKind(conflict, errs.KindResourceInUse) {
		t.Fatalf("BeginRouteDeletionWithTask() conflict/error = %v/%v", conflict, classifyErr)
	}
	storedTask, err := store.Get(ctx, taskKey(task.ID))
	if err != nil || storedTask.Entry != nil {
		t.Fatalf("Get(Task after conflict) = %#v, %v", storedTask, err)
	}
	visible, err := repository.GetRoute(ctx, current.Record.Desired.ID)
	if err != nil || visible.Record != current.Record {
		t.Fatalf("GetRoute(after conflict) = %#v, %v", visible, err)
	}
}

func TestRouteRemovalFailureRetryAndAbortPreserveAppliedState(t *testing.T) {
	// Rationale: every non-success attempt must retain both the public Route and
	// current applied projection, while retry atomically reacquires ownership of
	// the exact same immutable suppression candidate.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1140, "/retry/*")
	current, err := repository.CreateRoute(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateRoute() error = %v", err)
	}
	projection := routeDeletionTestProjection(t, store, current)
	task, marker, tombstone, intent := routeDeletionTestRecords(t, project, environment, current, projection)
	if _, err := repository.BeginRouteDeletionWithTask(
		ctx, environment, project, target, current, &projection, tombstone, intent, task, marker,
	); err != nil {
		t.Fatalf("BeginRouteDeletionWithTask() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(
		ctx, task.CreatedAt.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
	}
	failed, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, TaskStatusFailed, task.CreatedAt.Add(2*time.Second),
	)
	if err != nil || failed.Record.Status != TaskStatusFailed {
		t.Fatalf("AcknowledgeControllerTask(failed) = %#v/%v", failed, err)
	}
	assertRouteRemovalRetained(t, repository, store, current, projection)

	retryID := ids.NewAt(ids.KindTask, task.CreatedAt.Add(3*time.Second), 1141)
	retryMarker := pendingRetryMarker(
		failed.Record,
		retryID,
		task.CreatedAt.Add(3*time.Second),
		"route-retry-key-0001",
	)
	result, err := tasks.RetryTask(ctx, task.ID, retryID, TaskActorOperator, retryMarker)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	fence, err := store.Get(ctx, componentTaskActiveEnvironmentKey(environment.Record.ID))
	if err != nil || fence.Entry == nil || string(fence.Entry.Value) != retryID {
		t.Fatalf("Get(retry reconciliation fence) = %#v/%v", fence, err)
	}
	if _, err := tasks.AbortPendingTask(ctx, retryID, task.CreatedAt.Add(4*time.Second)); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	assertRouteRemovalRetained(t, repository, store, current, projection)
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	retryIntent, found, err := hierarchy.GetRouteRemovalIntent(ctx, retryID)
	if err != nil || !found || retryIntent.Record.Status != TaskStatusAborted || retryIntent.Record.TerminalAt == nil {
		t.Fatalf("GetRouteRemovalIntent(aborted retry) = %#v/%v/%v", retryIntent, found, err)
	}
}

func assertRouteRemovalRetained(
	t *testing.T,
	repository *RouteRepository,
	store *memoryHierarchyStore,
	route Versioned[RouteRecord],
	projection Versioned[EnvironmentComposeProjection],
) {
	t.Helper()
	visible, err := repository.GetRoute(context.Background(), route.Record.Desired.ID)
	if err != nil || visible.Record != route.Record {
		t.Fatalf("GetRoute(retained) = %#v/%v", visible, err)
	}
	projectionRead, err := store.Get(
		context.Background(),
		environmentComposeProjectionKey(route.Record.EnvironmentID),
	)
	if err != nil || projectionRead.Entry == nil {
		t.Fatalf("Get(retained projection) = %#v/%v", projectionRead, err)
	}
	retained, err := decodeEnvironmentComposeProjection(projectionRead.Entry.Value)
	if err != nil || !sameRouteRemovalProjection(retained, projection.Record) {
		t.Fatalf("retained projection = %#v/%v", retained, err)
	}
	fence, err := store.Get(context.Background(), componentTaskActiveEnvironmentKey(route.Record.EnvironmentID))
	if err != nil || fence.Entry != nil {
		t.Fatalf("Get(released reconciliation fence) = %#v/%v", fence, err)
	}
}

func routeDeletionTestProjection(
	t *testing.T,
	store *memoryHierarchyStore,
	route Versioned[RouteRecord],
) Versioned[EnvironmentComposeProjection] {
	t.Helper()
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID:    route.Record.EnvironmentID,
		RevisionID:       ids.NewAt(ids.KindTask, serviceRecordTestTime().Add(time.Hour), 1120),
		RenderGeneration: 2,
		Routes: []EnvironmentRouteIdentity{{
			ID: route.Record.Desired.ID, Host: route.Record.Desired.Host, Path: route.Record.Desired.Path,
		}},
	})
	value, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	result, err := store.Transact(context.Background(), []Condition{{
		Key: environmentComposeProjectionKey(route.Record.EnvironmentID),
	}}, []Mutation{{
		Type: MutationPut, Key: environmentComposeProjectionKey(route.Record.EnvironmentID), Value: value,
	}})
	clear(value)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Environment projection = %#v/%v", result, err)
	}
	return Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: result.Revision, ReadRevision: result.Revision,
	}
}

func routeDeletionTestRecords(
	t *testing.T,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	route Versioned[RouteRecord],
	projection Versioned[EnvironmentComposeProjection],
) (TaskRecord, IdempotencyMarker, DeletionTombstoneRecord, RouteRemovalIntent) {
	t.Helper()
	createdAt := serviceRecordTestTime().Add(2 * time.Hour)
	task := validTaskRecord(createdAt)
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.ID = ids.NewAt(ids.KindTask, createdAt, 1130)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 1131)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 1132)
	task.Executor = TaskExecutorController
	task.Type = TaskRemove
	task.Target = route.Record.Desired.ID
	task.Params = map[string]string{
		TaskResourceKindParam:     TaskResourceRoute,
		TaskRouteEnvironmentParam: route.Record.EnvironmentID,
	}
	task.TimeoutSeconds = 30
	task.IdempotencyKey = "route-remove-key-0001"
	marker := pendingTaskMarker(task)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment,
		ScopeID:   route.Record.EnvironmentID,
		Method:    http.MethodDelete,
		Route:     "/routes/{id}",
		Key:       task.IdempotencyKey,
	}
	replayTarget := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetRoute, ID: route.Record.Desired.ID}
	marker.ReplayTarget = &replayTarget
	tombstone := DeletionTombstoneRecord{
		TargetKind:     DeletionTargetRoute,
		TargetID:       route.Record.Desired.ID,
		TargetRevision: route.Revision,
		TaskID:         task.ID,
		Phase:          DeletionPhaseFinalizing,
		CreatedAt:      createdAt,
		UpdatedAt:      createdAt,
	}
	intent, err := NewRouteRemovalIntent(
		task.ID,
		route.Record.EnvironmentID,
		route.Record.Desired.ID,
		route.Revision,
		&projection,
		createdAt,
	)
	if err != nil {
		t.Fatalf("NewRouteRemovalIntent() error = %v", err)
	}
	return task, marker, tombstone, intent
}
