package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestServiceLifecycleRenderInputPinsAppliedProjection(t *testing.T) {
	// Rationale: queued and retried lifecycle work must reproduce the exact
	// Blueprint projection instead of consulting mutable desired state.
	t.Parallel()
	at := time.Date(2026, time.August, 23, 13, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 1)
	desired := core.Service{
		ID: serviceID, Name: "api", Image: "api:1",
		Strategy: core.StrategyRecreate, OnFailure: core.OnFailureSwitchBack, Replicas: 1,
	}
	projection := serviceRecordTestProjection(t, ids.NewAt(ids.KindEnvironment, at, 5), desired)
	projection.RenderGeneration = 9
	input := ServiceLifecycleRenderInput{
		PlanID: ids.NewAt(ids.KindPlan, at, 2), ServiceID: serviceID,
		TenantID: ids.NewAt(ids.KindTenant, at, 3), TenantSlug: "acme",
		ProjectID: ids.NewAt(ids.KindProject, at, 4), ProjectSlug: "shop",
		EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 5), EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/test",
		ArtifactID:          ids.NewAt(ids.KindConfig, at, 6),
		Projection:          projection,
	}
	value, err := encodeServiceLifecycleRenderInput(input)
	if err != nil {
		t.Fatalf("encodeServiceLifecycleRenderInput() error = %v", err)
	}
	decoded, err := decodeServiceLifecycleRenderInput(value)
	if err != nil || decoded.PlanID != input.PlanID || decoded.Projection.DesiredServices[0].Desired.ID != serviceID {
		t.Fatalf("decodeServiceLifecycleRenderInput() = %#v, %v", decoded, err)
	}
	input.Projection.DesiredServices[0].Desired.Name = "changed"
	if decoded.Projection.DesiredServices[0].Desired.Name != "api" {
		t.Fatalf("decoded render input aliases caller = %#v", decoded)
	}
}

func TestServiceLifecycleProjectionFenceUsesDesiredBlueprintHead(t *testing.T) {
	// Rationale: lifecycle render input comes from the desired Blueprint head,
	// whose revision cannot be compared with the independently written applied projection.
	t.Parallel()
	environmentID := ids.NewAt(
		ids.KindEnvironment,
		time.Date(2026, time.August, 29, 1, 15, 0, 0, time.UTC),
		1,
	)
	got := serviceLifecycleProjectionFenceKey(environmentID)
	if want := environmentBlueprintHeadKey(environmentID); got != want {
		t.Fatalf("serviceLifecycleProjectionFenceKey() = %q, want %q", got, want)
	}
	if got == environmentComposeProjectionKey(environmentID) {
		t.Fatalf("Service lifecycle fence still selects the applied projection: %q", got)
	}
}

func TestServiceLifecycleAbortReleasesActiveFence(t *testing.T) {
	// Rationale: every terminal path must release the per-Service serializer or
	// a failed/no-op lifecycle action would permanently block later recovery.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	services, err := newServiceRepository(store)
	if err != nil {
		t.Fatalf("newServiceRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	at := time.Date(2026, time.August, 23, 13, 30, 0, 0, time.UTC)
	tenant, err := hierarchy.CreateTenant(ctx, TenantRecord{
		ID: ids.NewAt(ids.KindTenant, at, 1), Slug: "acme", Name: "Acme",
	})
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(ctx, ProjectRecord{
		ID: ids.NewAt(ids.KindProject, at, 2), TenantID: tenant.Record.ID,
		Slug: "shop", Name: "Shop", Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	environment, err := hierarchy.CreateEnvironment(ctx, EnvironmentRecord{
		ID:                environmentID,
		ProjectID:         project.Record.ID,
		Name:              "production",
		NetworkPool:       "10.70.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenant.Record.ID + "/" + project.Record.ID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, at, 4),
		CreatedAt:         at,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	serviceID := ids.NewAt(ids.KindService, at, 5)
	desired := core.Service{
		ID: serviceID, Name: "api", Image: "api:1", Zones: []string{"frontend"},
		Strategy: core.StrategyRecreate, OnFailure: core.OnFailureSwitchBack, Replicas: 1,
	}
	projection := serviceRecordTestProjection(t, environment.Record.ID, desired)
	seedServiceRepositoryTestDesiredProjection(t, store, projection)
	seedServiceRepositoryTestRuntime(t, store, ServiceRuntimeRecord{
		EnvironmentID: environment.Record.ID, ServiceID: serviceID,
		Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
	})
	current, err := services.GetService(ctx, serviceID)
	if err != nil {
		t.Fatalf("GetService() error = %v", err)
	}
	replacement := current.Record
	replacement.Runtime.RuntimeIntent = core.ServiceRuntimeIntentStopped
	task := validTaskRecord(at.Add(time.Second))
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.Executor = TaskExecutorController
	task.Type = TaskStop
	task.Target = serviceID
	task.Params = map[string]string{
		TaskResourceKindParam: TaskResourceService, TaskServiceEnvironmentParam: environment.Record.ID,
	}
	task.Steps = []TaskStepRecord{{Kind: TaskStepOperation, ID: ids.NewAt(ids.KindStep, at, 6)}}
	task.RenderGeneration = 1
	task.TimeoutSeconds = 30
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = environment.Record.ID
	marker.Locator.Route = "/services/{id}/stop"
	marker.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetService, ID: serviceID}
	result, err := services.BeginServiceLifecycleWithTask(
		ctx, tenant, project, environment, current, replacement, nil, nil, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginServiceLifecycleWithTask() error = %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("Service lifecycle outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	runtimeRead, err := store.Get(ctx, serviceRuntimeKey(serviceID))
	if err != nil || runtimeRead.Entry == nil || runtimeRead.Entry.ModRevision != result.revision {
		t.Fatalf("Service runtime sidecar after lifecycle = %#v, %v", runtimeRead, err)
	}
	runtimeRecord, err := decodeServiceRuntimeRecord(runtimeRead.Entry.Value)
	if err != nil || runtimeRecord.Runtime.RuntimeIntent != core.ServiceRuntimeIntentStopped ||
		runtimeRecord.ServiceID != serviceID || runtimeRecord.EnvironmentID != environment.Record.ID {
		t.Fatalf("decoded Service runtime sidecar = %#v, %v", runtimeRecord, err)
	}
	epoch, err := store.Get(ctx, environmentMutationEpochKey(environment.Record.ID))
	if err != nil || epoch.Entry == nil || epoch.Entry.ModRevision != result.revision {
		t.Fatalf("Service lifecycle mutation epoch = %#v, %v", epoch, err)
	}
	aborted, err := tasks.AbortPendingTask(ctx, task.ID, task.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	epoch, err = store.Get(ctx, environmentMutationEpochKey(environment.Record.ID))
	if err != nil || epoch.Entry == nil || epoch.Entry.ModRevision != aborted.Revision {
		t.Fatalf("terminal Service lifecycle mutation epoch = %#v, %v", epoch, err)
	}
	active, err := store.Get(ctx, serviceLifecycleActiveKey(serviceID))
	if err != nil || active.Entry != nil {
		t.Fatalf("active Service lifecycle fence after abort = %#v, %v", active, err)
	}
	staleAt := task.CreatedAt.Add(3 * time.Second)
	staleTask := task
	staleTask.ID = ids.NewAt(ids.KindTask, staleAt, 8)
	staleTask.OperationID = ids.NewAt(ids.KindOperation, staleAt, 9)
	staleTask.PlanID = ids.NewAt(ids.KindPlan, staleAt, 10)
	staleTask.IdempotencyKey = "service-stale-runtime-key-0001"
	staleTask.CreatedAt = staleAt
	staleTask.UpdatedAt = staleAt
	staleTask.Steps = []TaskStepRecord{{Kind: TaskStepOperation, ID: ids.NewAt(ids.KindStep, staleAt, 11)}}
	staleMarker := pendingTaskMarker(staleTask)
	staleMarker.Locator.ScopeID = environment.Record.ID
	staleMarker.Locator.Route = "/services/{id}/stop"
	staleMarker.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetService, ID: serviceID}
	staleReplacement := current.Record
	staleReplacement.Runtime.RuntimeIntent = core.ServiceRuntimeIntentStopped
	if _, err := services.BeginServiceLifecycleWithTask(
		ctx, tenant, project, environment, current, staleReplacement, nil, nil, staleTask, staleMarker,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("stale Service runtime CAS error = %v", err)
	}
	retryAt := task.CreatedAt.Add(2 * time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 7)
	retryMarker := pendingRetryMarker(aborted.Record, retryID, retryAt, "service-retry-key-0001")
	retryResult, err := tasks.RetryTask(ctx, task.ID, retryID, TaskActorOperator, retryMarker)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	if epochRevision := mustEnvironmentMutationEpochRevision(
		t,
		store,
		environment.Record.ID,
	); epochRevision != retryResult.revision {
		t.Fatalf("Service lifecycle retry epoch = %d, want %d", epochRevision, retryResult.revision)
	}
	replay, err := tasks.RetryTask(ctx, task.ID, retryID, TaskActorOperator, retryMarker)
	if err != nil {
		t.Fatalf("RetryTask(replay) error = %v", err)
	}
	outcome, _, conflict, classifyErr := replay.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting {
		t.Fatalf("RetryTask(replay) outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	retryActive, err := store.Get(ctx, serviceLifecycleActiveKey(serviceID))
	if err != nil || retryActive.Entry == nil || retryActive.Entry.ModRevision != retryResult.revision {
		t.Fatalf("active Service lifecycle retry fence = %#v, %v", retryActive, err)
	}
}
