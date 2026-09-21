package app

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: publication must select an already sealed desired revision and
// advance the head, Task queue, and Environment mutation epoch atomically.

// Only final publication is rejected; source staging is not public authority.

// BP-04: final publication stays atomic and bounded independently of topology size.

// Rationale: the sealed 6-Zone/13-Service/6-Route topology must publish by
// Environment head without consuming one transaction operation per resource.

// The configuration-head fence adds one comparison, not one per resource.

// Rationale: a queued Task must reconstruct its sealed input after restart
// even when a later successful apply changes current desired state.

func createEnvironmentBlueprintOwners(
	t *testing.T,
	repository *etcd.HierarchyRepository,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]) {
	t.Helper()
	ctx := context.Background()
	at := time.Date(2026, 8, 22, 18, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, at, 1)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	if _, err := repository.CreateTenant(ctx, testhierarchy.TenantRecord{ID: tenantID, Slug: "acme", Name: "Acme"}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := repository.CreateProject(ctx, testhierarchy.ProjectRecord{
		ID: projectID, TenantID: tenantID, Slug: "console", Name: "Console", Kind: testhierarchy.ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environment, err := repository.CreateEnvironment(ctx, testhierarchy.EnvironmentRecord{NetworkPool: "10.40.0.0/16",
		ID: environmentID, ProjectID: projectID, Name: "production",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, at, 4),
		CreatedAt:         at,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	return project, environment
}

func environmentBlueprintTestTask(
	t *testing.T,
	project testhierarchy.ProjectRecord,
	environment testhierarchy.EnvironmentRecord,
	seed int64,
) etcd.TaskRecord {
	t.Helper()
	at := time.Date(2026, 8, 22, 19, 0, int(seed), 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, at, seed)
	owner, err := testtaskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	return etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, at, seed+1),
		Owner: owner, Actor: testtaskjournal.TaskActorOperator,
		IdempotencyKey: ids.NewAt(ids.KindOperation, at, seed+2)[3:],
		Executor:       testtaskjournal.TaskExecutorAgent, PlanID: ids.NewAt(ids.KindPlan, at, seed+3),
		PlanHash: strings.Repeat("a", 64), RenderGeneration: 1,
		Type: testtaskjournal.TaskUpdate, Target: environment.ID,
		Params: map[string]string{
			testblueprints.EnvironmentDesiredRevisionParam:      taskID,
			testtaskjournal.TaskMaterializationEnvironmentParam: environment.ID,
		},
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, at, seed+4)},
		},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: at, UpdatedAt: at,
	}
}

func environmentBlueprintTestRevision(
	environmentID string,
	task etcd.TaskRecord,
	content string,
) testblueprints.EnvironmentBlueprintRevision {
	return testblueprints.EnvironmentBlueprintRevision{
		EnvironmentID: environmentID, RevisionID: task.ID,
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Interpolation: map[string]string{"TAG": "v1"},
		Files:         []testblueprints.EnvironmentBlueprintFile{{Path: "blueprint.yaml", Content: []byte(content)}},
		CreatedAt:     task.CreatedAt,
	}
}

func environmentBlueprintTestMarker(task etcd.TaskRecord, environmentID string) testidempotency.IdempotencyMarker {
	marker := pendingTaskMarker(task)
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPut, Route: "/environments/{id}/blueprint", Key: task.IdempotencyKey,
	}
	return marker
}
