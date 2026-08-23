package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestServiceLifecycleRenderInputPinsAppliedProjection(t *testing.T) {
	// Rationale: queued and retried lifecycle work must reproduce the exact
	// Blueprint projection instead of consulting mutable desired state.
	t.Parallel()
	at := time.Date(2026, time.August, 23, 13, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 1)
	input := ServiceLifecycleRenderInput{
		PlanID: ids.NewAt(ids.KindPlan, at, 2), ServiceID: serviceID,
		TenantID: ids.NewAt(ids.KindTenant, at, 3), TenantSlug: "acme",
		ProjectID: ids.NewAt(ids.KindProject, at, 4), ProjectSlug: "shop",
		EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 5), EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/test",
		ArtifactID:          ids.NewAt(ids.KindConfig, at, 6),
		Projection: EnvironmentComposeProjection{
			EnvironmentID:       ids.NewAt(ids.KindEnvironment, at, 5),
			BlueprintRevisionID: ids.NewAt(ids.KindTask, at, 7), RenderGeneration: 9,
			Services: []EnvironmentComposeIdentity{{ID: serviceID, Name: "api"}},
		},
	}
	value, err := encodeServiceLifecycleRenderInput(input)
	if err != nil {
		t.Fatalf("encodeServiceLifecycleRenderInput() error = %v", err)
	}
	decoded, err := decodeServiceLifecycleRenderInput(value)
	if err != nil || decoded.PlanID != input.PlanID || decoded.Projection.Services[0].ID != serviceID {
		t.Fatalf("decodeServiceLifecycleRenderInput() = %#v, %v", decoded, err)
	}
	input.Projection.Services[0].Name = "changed"
	if decoded.Projection.Services[0].Name != "api" {
		t.Fatalf("decoded render input aliases caller = %#v", decoded)
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
	record, err := NewServiceRecord(environment.Record.ID, core.Service{
		ID: serviceID, Name: "api", Image: "api:1", Zones: []string{"frontend"},
		Strategy: core.StrategyRecreate, OnFailure: core.OnFailureSwitchBack, Replicas: 1,
	}, "")
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	current, err := services.CreateService(ctx, environment, project, record)
	if err != nil {
		t.Fatalf("CreateService() error = %v", err)
	}
	replacement, err := SetServiceRuntimeIntent(current.Record, core.ServiceRuntimeIntentStopped)
	if err != nil {
		t.Fatalf("SetServiceRuntimeIntent() error = %v", err)
	}
	task := validTaskRecord(at.Add(time.Second))
	task.Executor = TaskExecutorController
	task.Type = TaskStop
	task.Target = serviceID
	task.Params = map[string]string{
		TaskResourceKindParam: TaskResourceService, TaskServiceEnvironmentParam: environment.Record.ID,
	}
	task.Steps = []TaskStepRecord{{ID: ids.NewAt(ids.KindStep, at, 6)}}
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
	if _, err := tasks.AbortPendingTask(ctx, task.ID, task.CreatedAt.Add(time.Second)); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	active, err := store.Get(ctx, serviceLifecycleActiveKey(serviceID))
	if err != nil || active.Entry != nil {
		t.Fatalf("active Service lifecycle fence after abort = %#v, %v", active, err)
	}
}
