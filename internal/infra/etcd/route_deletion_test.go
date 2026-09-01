package etcd

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestRouteRemovalCompletionPromotesStagedCandidate(t *testing.T) {
	// Rationale: public Route visibility, its immutable desired-state candidate,
	// Task journal, deletion fence, and reconciliation ownership must never
	// describe different removal attempts after a Controller crash.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1100, "/api/*")
	projection := routeDeletionTestProjection(t, store, environment, project, target, Versioned[RouteRecord]{Record: record})
	current, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil {
		t.Fatalf("GetRoute() error = %v", err)
	}
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
	descriptorRead, err := store.Get(
		ctx,
		environmentBlueprintDescriptorKeyByID(strings.TrimPrefix(task.ID, string(ids.KindTask)+"_")),
	)
	if err != nil || descriptorRead.Entry == nil {
		t.Fatalf("Get(Route desired descriptor) = %#v/%v", descriptorRead, err)
	}
	descriptor, err := decodeEnvironmentBlueprintStageDescriptor(descriptorRead.Entry.Value)
	if err != nil || descriptor.ProjectionResources != 1 {
		t.Fatalf("Route desired descriptor resources = %d/%v, want 1", descriptor.ProjectionResources, err)
	}
	desired, found, err := hierarchy.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || !sameRouteRemovalProjection(desired.Record, projection.Record) {
		t.Fatalf("GetEnvironmentComposeProjection(pending) = %#v/%v/%v", desired, found, err)
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
		routeObservationKey(task.Target),
		deletionTombstoneKey(string(DeletionTargetRoute), task.Target),
		componentTaskActiveEnvironmentKey(environment.Record.ID),
	} {
		stored, getErr := store.Get(ctx, key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("finalized key %s = %#v/%v", key, stored, getErr)
		}
	}
	desired, found, err = hierarchy.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || !sameRouteRemovalProjection(desired.Record, *intent.CandidateProjection) {
		t.Fatalf("GetEnvironmentComposeProjection(completed) = %#v/%v/%v", desired, found, err)
	}
	_, found, err = hierarchy.GetRouteRemovalIntent(ctx, task.ID)
	if err != nil || found {
		t.Fatalf("GetRouteRemovalIntent(completed) found/error = %v/%v", found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, TaskStatusCompleted, terminalAt); err != nil {
		t.Fatalf("AcknowledgeControllerTask(replay) error = %v", err)
	}
	currentHead, err := encodeTaskReference(projection.Record.RevisionID)
	if err != nil {
		t.Fatalf("encodeTaskReference(current replay head) error = %v", err)
	}
	corruptReplay, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: environmentBlueprintHeadKey(environment.Record.ID), Value: currentHead,
	}})
	clear(currentHead)
	if err != nil || !corruptReplay.Succeeded {
		t.Fatalf("replace completed replay head = %#v/%v", corruptReplay, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(ctx, task.ID, TaskStatusCompleted, terminalAt); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("AcknowledgeControllerTask(changed replay head) error = %v", err)
	}
}

func TestRouteRepositoryRejectsRemovalDuringEnvironmentReconciliation(t *testing.T) {
	// Rationale: a Route desired-state candidate must never race another writer
	// that can replace the same applied Environment projection.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1110, "/admin/*")
	projection := routeDeletionTestProjection(t, store, environment, project, target, Versioned[RouteRecord]{Record: record})
	current, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil {
		t.Fatalf("GetRoute() error = %v", err)
	}
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
	// the exact same immutable desired-state candidate.
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project, target := routeRepositoryTestHierarchy(t)
	record := routeRepositoryTestRecord(t, environment.Record.ID, target.Record.Desired.ID, 1140, "/retry/*")
	projection := routeDeletionTestProjection(t, store, environment, project, target, Versioned[RouteRecord]{Record: record})
	current, err := repository.GetRoute(ctx, record.Desired.ID)
	if err != nil {
		t.Fatalf("GetRoute() error = %v", err)
	}
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
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, TaskStatusFailed, task.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(failed replay) error = %v", err)
	}

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
	observationRead, err := store.Get(context.Background(), routeObservationKey(route.Record.Desired.ID))
	if err != nil || observationRead.Entry == nil {
		t.Fatalf("Get(retained Route observation) = %#v/%v", observationRead, err)
	}
	observation, err := decodeRouteObservation(observationRead.Entry.Value)
	if err != nil || observation.EnvironmentID != route.Record.EnvironmentID ||
		observation.RouteID != route.Record.Desired.ID ||
		observation.DesiredGeneration != route.Record.DesiredGeneration ||
		observation.Observation != route.Record.Observed {
		t.Fatalf("retained Route observation = %#v/%v", observation, err)
	}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	desired, found, err := hierarchy.GetEnvironmentComposeProjection(
		context.Background(), route.Record.EnvironmentID,
	)
	if err != nil || !found || !sameRouteRemovalProjection(desired.Record, projection.Record) {
		t.Fatalf("GetEnvironmentComposeProjection(retained) = %#v/%v/%v", desired, found, err)
	}
	fence, err := store.Get(context.Background(), componentTaskActiveEnvironmentKey(route.Record.EnvironmentID))
	if err != nil || fence.Entry != nil {
		t.Fatalf("Get(released reconciliation fence) = %#v/%v", fence, err)
	}
}

func routeDeletionTestProjection(
	t *testing.T,
	store *memoryHierarchyStore,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	route Versioned[RouteRecord],
) Versioned[EnvironmentComposeProjection] {
	t.Helper()
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 1120)
	selected, found, err := currentEnvironmentProjectionAtRevision(
		context.Background(), store, environment.Record.ID, 0,
	)
	if err != nil || !found {
		t.Fatalf("read current Environment projection = %#v/%v/%v", selected, found, err)
	}
	projection, err := ApplyEnvironmentRoute(selected.Record, route.Record)
	if err != nil {
		t.Fatalf("ApplyEnvironmentRoute() error = %v", err)
	}
	projection.RevisionID = task.ID
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n")
	marker := environmentBlueprintTestMarker(task, environment.Record.ID)
	claim := stageEnvironmentBlueprintForPublicationTest(t, hierarchy, selected.Revision, revision, projection, marker)
	descriptorRead, err := store.Get(context.Background(), environmentBlueprintDescriptorKeyByID(claim.DescriptorID))
	if err != nil || descriptorRead.Entry == nil {
		t.Fatalf("read Environment descriptor = %#v/%v", descriptorRead, err)
	}
	descriptor, err := decodeEnvironmentBlueprintStageDescriptor(descriptorRead.Entry.Value)
	if err != nil {
		t.Fatalf("decode Environment descriptor = %v", err)
	}
	descriptor.State = EnvironmentBlueprintStagePublished
	descriptor.UpdatedAt = nextBlueprintProgressTime(descriptor.UpdatedAt)
	descriptorValue, err := encodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		t.Fatalf("encode Environment descriptor = %v", err)
	}
	headValue, err := encodeTaskReference(projection.RevisionID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	observation, err := NewRouteObservationRecord(
		route.Record.EnvironmentID,
		route.Record.Desired.ID,
		route.Record.DesiredGeneration,
		route.Record.Observed,
	)
	if err != nil {
		t.Fatalf("NewRouteObservationRecord() error = %v", err)
	}
	observationValue, err := encodeRouteObservation(observation)
	if err != nil {
		t.Fatalf("encodeRouteObservation() error = %v", err)
	}
	result, err := store.Transact(context.Background(), []Condition{{
		Key: environmentBlueprintDescriptorKeyByID(claim.DescriptorID), ModRevision: descriptorRead.Entry.ModRevision,
	}, {
		Key: environmentBlueprintHeadKey(route.Record.EnvironmentID), ModRevision: selected.Revision,
	}, {
		Key: routeObservationKey(route.Record.Desired.ID),
	}}, []Mutation{
		{Type: MutationPut, Key: environmentBlueprintDescriptorKeyByID(claim.DescriptorID), Value: descriptorValue},
		{Type: MutationPut, Key: environmentBlueprintHeadKey(route.Record.EnvironmentID), Value: headValue},
		{Type: MutationPut, Key: routeObservationKey(route.Record.Desired.ID), Value: observationValue},
	})
	clear(descriptorValue)
	clear(headValue)
	clear(observationValue)
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
