package app

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testenvironmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CreateManualScript uses ordinary persisted Script creation after the fixture
// has published and acknowledged a real generated Release plan.
func (fixture *ExecutedArtifactFixture) CreateManualScript(
	t *testing.T,
	serviceID string,
) (*etcd.ScriptRepository, etcd.TaskRecord) {
	t.Helper()
	ctx := context.Background()
	environment, err := fixture.Hierarchy.GetEnvironment(ctx, fixture.Environment.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	project, err := fixture.Hierarchy.GetProject(ctx, fixture.Project.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	services, err := etcd.NewServiceRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	service, err := services.GetService(ctx, serviceID)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.store.valueAt(testservices.ServiceRuntimeKey(serviceID), fixture.store.revision) != nil {
		t.Fatal("manual source journey must exercise a Service without an optional runtime sidecar")
	}
	scripts, err := etcd.NewScriptRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	at := fixture.Environment.Record.CreatedAt.Add(time.Hour)
	record, err := testscripts.NewRecord(fixture.Environment.Record.ID, serviceID, core.Script{
		ID: ids.NewAt(ids.KindScript, at, 1), Slug: "manual-source-journey", ServiceName: service.Record.Desired.Name,
		When: core.ScriptManual, Body: "exit 0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scripts.CreateScript(ctx, environment, project, service, record); err != nil {
		t.Fatal(err)
	}
	task := validTaskRecord(at)
	task.Owner = mustEnvironmentTaskOwner(t, fixture.Project.Record, fixture.Environment.Record)
	task.Actor, task.Executor, task.Type, task.Target = testtaskjournal.TaskActorOperator, testtaskjournal.TaskExecutorAgent, testtaskjournal.TaskScript, record.Desired.ID
	task.Params = map[string]string{
		testscriptexecutions.ScriptExecutionIDParam: ids.NewULID(),
		testscriptexecutions.ScriptGenerationParam:  "1",
	}
	task.Steps = []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}}
	task.TimeoutSeconds = executionplan.ScriptExecutionTimeoutSeconds
	return scripts, task
}

func (fixture *ExecutedArtifactFixture) PublishManualScript(
	t *testing.T,
	scripts *etcd.ScriptRepository,
	sources testscriptsourcequeries.ScriptExecutionSources,
	task etcd.TaskRecord,
	plan *agentpb.ExecutionPlan,
) {
	t.Helper()
	execution, err := etcd.NewScriptExecutionRecord(task, plan, task.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID, marker.Locator.Route = task.Owner.EnvironmentID, "/scripts/{id}/run"
	result, err := scripts.PublishExecutionWithTask(context.Background(), sources, execution, task, marker)
	if err != nil || !integrationTransactionApplied(result) {
		t.Fatalf("manual publication = %v", err)
	}
}

func (fixture *ExecutedArtifactFixture) RetryTimedOutManualScript(
	t *testing.T,
	assignment etcd.TaskAssignment,
	afterTimeout ...func(),
) etcd.TaskAssignment {
	t.Helper()
	ctx := context.Background()
	deadline := assignment.Assignment.Record.Deadline
	count, err := fixture.Tasks.ExpireTimedOutTasks(ctx, deadline)
	if err != nil || count != 1 {
		t.Fatalf("generated manual timeout = %d, %v", count, err)
	}
	terminal, err := fixture.Tasks.GetTask(ctx, assignment.Task.Record.ID)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusTimedOut {
		t.Fatalf("generated manual timeout state = %v", err)
	}
	for _, check := range afterTimeout {
		check()
	}
	retryAt := deadline.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 121)
	result, err := fixture.Tasks.RetryTask(
		ctx,
		terminal.Record.ID,
		retryID,
		testtaskjournal.TaskActorOperator,
		pendingRetryMarker(terminal.Record, retryID, retryAt, "manual-source-journey-retry"),
	)
	if err != nil || !integrationTransactionApplied(result) {
		t.Fatalf("generated manual retry = %v", err)
	}
	claim, found, err := fixture.Tasks.ClaimNextTask(
		ctx,
		assignment.Assignment.Record.AgentID,
		1,
		retryAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != retryID ||
		claim.Assignment.Record.AssignmentID == assignment.Assignment.Record.AssignmentID {
		t.Fatalf("generated manual retry assignment = %t, %v", found, err)
	}
	return claim
}

func (fixture *ExecutedArtifactFixture) RemoveCompletedManualScript(
	t *testing.T,
	scripts *etcd.ScriptRepository,
	scriptID string,
) {
	t.Helper()
	ctx := context.Background()
	environment, err := fixture.Hierarchy.GetEnvironment(ctx, fixture.Environment.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	project, err := fixture.Hierarchy.GetProject(ctx, fixture.Project.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	script, err := scripts.GetScript(ctx, scriptID)
	if err != nil {
		t.Fatal(err)
	}
	service, err := testenvironmentqueries.FindServiceAtRevision(
		ctx,
		fixture.store,
		script.Record.ServiceID,
		script.ReadRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	task, marker, tombstone := scriptDeletionTestRecords(t, project, environment, script)
	// This journey's runtime timestamps are later than the standalone deletion
	// fixture's baseline. Keep removal after the completed retry.
	at := fixture.Environment.Record.CreatedAt.Add(4 * time.Hour)
	task.CreatedAt, task.UpdatedAt = at, at
	marker.CreatedAt, marker.UpdatedAt = at, at
	tombstone.CreatedAt, tombstone.UpdatedAt = at, at
	result, err := scripts.BeginScriptDeletionWithTask(
		ctx,
		environment,
		project,
		service,
		script,
		tombstone,
		task,
		marker,
	)
	if err != nil || !integrationTransactionApplied(result) {
		t.Fatalf("normal Script removal = %v", err)
	}
	claim, found, err := fixture.Tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != task.ID {
		t.Fatalf("Script removal claim = %t, %v", found, err)
	}
	if _, err := fixture.Tasks.AcknowledgeControllerTask(ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := scripts.GetScript(ctx, scriptID); !isKind(err, errs.KindScriptNotFound) {
		t.Fatalf("removed Script still readable: %v", err)
	}
}

func (fixture *ExecutedArtifactFixture) CompleteManualScript(
	t *testing.T, scripts *etcd.ScriptRepository, assignment etcd.TaskAssignment,
) {
	t.Helper()
	ctx := context.Background()
	execution, err := scripts.GetScriptExecution(
		ctx,
		assignment.Task.Record.Params[testscriptexecutions.ScriptExecutionIDParam],
	)
	if err != nil {
		t.Fatal(err)
	}
	manualScriptCleanupCheckpoints(t, scripts, assignment, execution.Record, testtaskjournal.TaskStatusCompleted)
	terminal, err := fixture.Tasks.AcknowledgeTask(
		ctx,
		assignment.Assignment.Record.AgentID,
		1,
		assignment.Task.Record.ID,
		assignment.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, manualScriptTerminalResult(
			assignment, testtaskjournal.TaskStatusCompleted,
		),
		assignment.Task.Record.CreatedAt.Add(20*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("manual terminal = %v", err)
	}
	if fixture.store.valueAt(
		testscriptsourceevidence.ScriptSourceRootKey(assignment.Task.Record.OperationID),
		fixture.store.revision,
	) != nil {
		t.Fatal("completed generated manual plan retained source root")
	}
}
