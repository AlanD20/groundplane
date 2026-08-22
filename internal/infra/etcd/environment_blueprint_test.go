package etcd

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestEnvironmentBlueprintApplyPublishesImmutableRevisionAndTaskAtomically(t *testing.T) {
	// Rationale: desired bytes, current pointer, Task indexes, queue membership,
	// and idempotency evidence must become visible at one etcd revision.
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	task := environmentBlueprintTestTask(environment.Record.ID, 20)
	revision := environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n")
	marker := environmentBlueprintTestMarker(task, environment.Record.ID)

	result, err := repository.ApplyEnvironmentBlueprintWithTask(
		ctx, project, environment, 0, revision,
		environmentBlueprintTestProjection(environment.Record.ID, task, 1), task, marker,
	)
	if err != nil {
		t.Fatalf("ApplyEnvironmentBlueprintWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("apply outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}

	head, found, err := repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil || !found || head.Record.RevisionID != task.ID {
		t.Fatalf("GetEnvironmentBlueprintHead() = %#v, %v, %v", head, found, err)
	}
	stored, found, err := repository.GetEnvironmentBlueprintRevision(ctx, environment.Record.ID, task.ID)
	if err != nil || !found || len(stored.Record.Files) != 1 ||
		string(stored.Record.Files[0].Content) != "services: {}\n" {
		t.Fatalf("GetEnvironmentBlueprintRevision() = %#v, %v, %v", stored, found, err)
	}
	if head.Revision != stored.Revision || head.ReadRevision != stored.ReadRevision {
		t.Fatalf("head and revision were not published at one MVCC revision: %#v / %#v", head, stored)
	}
	projection, found, err := repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found || projection.Record.BlueprintRevisionID != task.ID ||
		projection.Record.RenderGeneration != 1 || projection.Revision != head.Revision {
		t.Fatalf("GetEnvironmentComposeProjection() = %#v, %v, %v", projection, found, err)
	}
	assertEnvironmentBlueprintValue(t, store, taskKey(task.ID))
	assertEnvironmentBlueprintValue(t, store, taskQueueKey(task.Executor, task.ID))
}

func TestEnvironmentBlueprintApplyPreservesOldRevisionWhenHeadAdvances(t *testing.T) {
	// Rationale: a queued Task must reconstruct its sealed input after restart
	// even when a later successful apply changes current desired state.
	ctx := context.Background()
	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	firstTask := environmentBlueprintTestTask(environment.Record.ID, 30)
	first := environmentBlueprintTestRevision(environment.Record.ID, firstTask, "services: {old: {}}\n")
	firstProjection := environmentBlueprintTestProjection(environment.Record.ID, firstTask, 1)
	firstResult, err := repository.ApplyEnvironmentBlueprintWithTask(
		ctx, project, environment, 0, first, firstProjection, firstTask,
		environmentBlueprintTestMarker(firstTask, environment.Record.ID),
	)
	if err != nil {
		t.Fatalf("first apply error = %v", err)
	}
	if outcome, _, conflict, classifyErr := firstResult.Classify(); classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("first apply outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	head, found, err := repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("first head = %#v, %v, %v", head, found, err)
	}

	secondTask := environmentBlueprintTestTask(environment.Record.ID, 40)
	second := environmentBlueprintTestRevision(environment.Record.ID, secondTask, "services: {new: {}}\n")
	secondProjection := firstProjection
	secondProjection.BlueprintRevisionID = secondTask.ID
	secondProjection.RenderGeneration = 2
	secondResult, err := repository.ApplyEnvironmentBlueprintWithTask(
		ctx, project, environment, head.Revision, second, secondProjection, secondTask,
		environmentBlueprintTestMarker(secondTask, environment.Record.ID),
	)
	if err != nil {
		t.Fatalf("second apply error = %v", err)
	}
	if outcome, _, conflict, classifyErr := secondResult.Classify(); classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("second apply outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}

	old, found, err := repository.GetEnvironmentBlueprintRevision(ctx, environment.Record.ID, firstTask.ID)
	if err != nil || !found || string(old.Record.Files[0].Content) != "services: {old: {}}\n" {
		t.Fatalf("old immutable revision = %#v, %v, %v", old, found, err)
	}
	current, found, err := repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil || !found || current.Record.RevisionID != secondTask.ID {
		t.Fatalf("current head = %#v, %v, %v", current, found, err)
	}
}

// Rationale: Blueprint replacement must never turn omission into an implicit
// destructive service or stateful-volume operation.
func TestEnvironmentBlueprintApplyRejectsOwnedResourceOmission(t *testing.T) {
	ctx := context.Background()
	repository, err := newHierarchyRepository(newMemoryHierarchyStore())
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	firstTask := environmentBlueprintTestTask(environment.Record.ID, 50)
	first := environmentBlueprintTestRevision(environment.Record.ID, firstTask, "services: {api: {}}\n")
	result, err := repository.ApplyEnvironmentBlueprintWithTask(
		ctx, project, environment, 0, first,
		environmentBlueprintTestProjection(environment.Record.ID, firstTask, 1), firstTask,
		environmentBlueprintTestMarker(firstTask, environment.Record.ID),
	)
	if err != nil {
		t.Fatalf("first apply error = %v", err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("first apply outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	head, found, err := repository.GetEnvironmentBlueprintHead(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("head = %#v, %v, %v", head, found, err)
	}
	secondTask := environmentBlueprintTestTask(environment.Record.ID, 60)
	second := environmentBlueprintTestRevision(environment.Record.ID, secondTask, "services: {}\n")
	omitted := environmentBlueprintTestProjection(environment.Record.ID, secondTask, 2)
	omitted.Services = nil
	_, err = repository.ApplyEnvironmentBlueprintWithTask(
		ctx, project, environment, head.Revision, second, omitted, secondTask,
		environmentBlueprintTestMarker(secondTask, environment.Record.ID),
	)
	if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("omitting apply error = %v, want resource.in_use", err)
	}
}

func createEnvironmentBlueprintOwners(
	t *testing.T,
	repository *HierarchyRepository,
) (Versioned[ProjectRecord], Versioned[EnvironmentRecord]) {
	t.Helper()
	ctx := context.Background()
	at := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	if _, err := repository.CreateTenant(ctx, TenantRecord{ID: tenantID, Slug: "acme", Name: "Acme"}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := repository.CreateProject(ctx, ProjectRecord{
		ID: projectID, TenantID: tenantID, Slug: "console", Name: "Console", Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environment, err := repository.CreateEnvironment(ctx, EnvironmentRecord{
		ID: environmentID, ProjectID: projectID, Name: "production",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, at, 4),
		CreatedAt:         at,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	return project, environment
}

func environmentBlueprintTestTask(environmentID string, seed int64) TaskRecord {
	at := time.Date(2026, 8, 22, 19, 0, int(seed), 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, at, seed)
	return TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, at, seed+1),
		IdempotencyKey: ids.NewAt(ids.KindOperation, at, seed+2)[3:],
		Executor:       etcdTaskExecutorAgent(), PlanID: ids.NewAt(ids.KindPlan, at, seed+3),
		PlanHash: strings.Repeat("a", 64), RenderGeneration: 1,
		Type: TaskUpdate, Target: environmentID,
		Params: map[string]string{
			EnvironmentBlueprintRevisionParam:   taskID,
			TaskMaterializationEnvironmentParam: environmentID,
		},
		Steps:          []TaskStepRecord{{ID: ids.NewAt(ids.KindStep, at, seed+4)}},
		TimeoutSeconds: 120, Status: TaskStatusPending, NextEventSequence: 1, CreatedAt: at,
	}
}

func etcdTaskExecutorAgent() TaskExecutor { return TaskExecutorAgent }

func environmentBlueprintTestRevision(
	environmentID string,
	task TaskRecord,
	content string,
) EnvironmentBlueprintRevision {
	return EnvironmentBlueprintRevision{
		EnvironmentID: environmentID, RevisionID: task.ID,
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Interpolation: map[string]string{"TAG": "v1"},
		Files:         []EnvironmentBlueprintFile{{Path: "blueprint.yaml", Content: []byte(content)}},
		CreatedAt:     task.CreatedAt,
	}
}

func environmentBlueprintTestProjection(
	environmentID string,
	task TaskRecord,
	generation uint64,
) EnvironmentComposeProjection {
	return EnvironmentComposeProjection{
		EnvironmentID: environmentID, BlueprintRevisionID: task.ID, RenderGeneration: generation,
		Services: []EnvironmentComposeIdentity{{
			ID: ids.NewAt(ids.KindService, task.CreatedAt, 70), Name: "api",
		}},
		Networks: []EnvironmentComposeIdentity{{
			ID: ids.NewAt(ids.KindNetwork, task.CreatedAt, 71), Name: "default",
		}},
		Volumes: []EnvironmentComposeIdentity{{
			ID: ids.NewAt(ids.KindVolume, task.CreatedAt, 72), Name: "app-data",
		}},
	}
}

func environmentBlueprintTestMarker(task TaskRecord, environmentID string) IdempotencyMarker {
	marker := pendingTaskMarker(task)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPut, Route: "/environments/{id}/blueprint", Key: task.IdempotencyKey,
	}
	return marker
}

func assertEnvironmentBlueprintValue(t *testing.T, store *memoryHierarchyStore, key string) {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil || result.Entry == nil {
		t.Fatalf("durable key %q = %#v, %v", key, result, err)
	}
}
