package dispatch

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const controllerTaskTestAgentImage = "ghcr.io/example/groundplane-agent:test"

func TestLocalAgentControllerTaskHandlerEnrollsFromImmutableTaskInput(t *testing.T) {
	t.Parallel()

	task := testAgentEnrollmentTask()
	agents := &fakeControllerTaskLocalAgents{healthErr: errs.New(errs.KindAgentNotFound, "missing")}
	handler, err := newTestResourceHandler(
		agents,
		&fakeControllerTaskRunners{},
		&recordingControllerTaskExecutor{})

	if err != nil {
		t.Fatalf("NewResourceHandler() error = %v", err)
	}
	if err := handler.Execute(context.Background(), task, testControllerProgress(context.Background(), task)); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := localagent.EnrollRequest{
		AgentID: task.Target, EnrollmentTaskID: task.ID, Image: controllerTaskTestAgentImage,
		Config: localagent.Config{
			PullIntervalSeconds: 2, MaxConcurrentTasks: 3,
			Labels: map[string]string{"arch": "arm64"},
		},
	}
	if agents.enrollCalls != 1 || !reflect.DeepEqual(agents.request, want) {
		t.Fatalf("Enroll() calls/request = %d/%#v", agents.enrollCalls, agents.request)
	}
}

func TestLocalAgentControllerTaskHandlerReconcilesMatchingReplay(t *testing.T) {
	t.Parallel()

	task := testAgentEnrollmentTask()
	agents := &fakeControllerTaskLocalAgents{health: localagent.Health{Agent: localagent.Agent{
		ID: task.Target, EnrollmentTaskID: task.ID, Phase: localagent.PhaseReady,
	}}}
	handler, err := newTestResourceHandler(
		agents,
		&fakeControllerTaskRunners{},
		&recordingControllerTaskExecutor{})

	if err != nil {
		t.Fatalf("NewResourceHandler() error = %v", err)
	}
	if err := handler.Execute(context.Background(), task, testControllerProgress(context.Background(), task)); err != nil {
		t.Fatalf("Execute(replay) error = %v", err)
	}
	if agents.enrollCalls != 0 || agents.reconcileCalls != 1 || agents.healthCalls != 2 {
		t.Fatalf(
			"replay calls = enroll %d, reconcile %d, health %d",
			agents.enrollCalls,
			agents.reconcileCalls,
			agents.healthCalls,
		)
	}
}

func TestLocalAgentControllerTaskHandlerReconcilesEnrollmentRetry(t *testing.T) {
	t.Parallel()

	original := testAgentEnrollmentTask()
	retry := original
	retry.ID = ids.NewAt(ids.KindTask, original.CreatedAt.Add(time.Second), 23)
	retry.RetryOf = original.ID
	agents := &fakeControllerTaskLocalAgents{health: localagent.Health{Agent: localagent.Agent{
		ID: retry.Target, EnrollmentTaskID: original.ID, Phase: localagent.PhaseReady,
	}}}
	handler, err := newTestResourceHandler(
		agents,
		&fakeControllerTaskRunners{}, &recordingControllerTaskExecutor{})

	if err != nil {
		t.Fatalf("NewResourceHandler() error = %v", err)
	}
	if err := handler.Execute(context.Background(), retry, testControllerProgress(context.Background(), retry)); err != nil {
		t.Fatalf("Execute(retry) error = %v", err)
	}
	if agents.enrollCalls != 0 || agents.reconcileCalls != 1 || agents.healthCalls != 2 {
		t.Fatalf(
			"retry calls = enroll %d, reconcile %d, health %d",
			agents.enrollCalls,
			agents.reconcileCalls,
			agents.healthCalls,
		)
	}
}

func TestLocalAgentControllerTaskHandlerRejectsAnotherEnrollmentOwner(t *testing.T) {
	t.Parallel()

	task := testAgentEnrollmentTask()
	agents := &fakeControllerTaskLocalAgents{health: localagent.Health{Agent: localagent.Agent{
		ID:               task.Target,
		EnrollmentTaskID: ids.NewAt(ids.KindTask, task.CreatedAt.Add(time.Second), 22),
		Phase:            localagent.PhaseReady,
	}}}
	handler, err := newTestResourceHandler(
		agents,
		&fakeControllerTaskRunners{},
		&recordingControllerTaskExecutor{})

	if err != nil {
		t.Fatalf("NewResourceHandler() error = %v", err)
	}
	err = handler.Execute(context.Background(), task, testControllerProgress(context.Background(), task))
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || agents.reconcileCalls != 0 {
		t.Fatalf("Execute(mismatch) error/calls = %v/%d", err, agents.reconcileCalls)
	}
}

func TestLocalAgentControllerTaskHandlerRejectsUnclosedParams(t *testing.T) {
	t.Parallel()

	task := testAgentEnrollmentTask()
	task.Params["surprise"] = "value"
	handler, err := newTestResourceHandler(
		&fakeControllerTaskLocalAgents{},
		&fakeControllerTaskRunners{}, &recordingControllerTaskExecutor{})

	if err != nil {
		t.Fatalf("NewResourceHandler() error = %v", err)
	}
	if err := handler.Execute(context.Background(), task, testControllerProgress(context.Background(), task)); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Execute(unclosed params) error = %v, want validation.failed", err)
	}
}

func testAgentEnrollmentTask() etcd.TaskRecord {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, now, 1)
	return etcd.TaskRecord{
		ID: taskID, Executor: testtaskjournal.TaskExecutorController,
		Type: testtaskjournal.TaskCreate, Target: ids.NewAt(ids.KindAgent, now, 2),
		Params: map[string]string{
			testtaskjournal.TaskResourceKindParam: "agent",
			"image":                               controllerTaskTestAgentImage,
			"enrollment_task_id":                  taskID,
			"pull_interval_seconds":               "2",
			"max_concurrent_tasks":                "3",
			"label:arch":                          "arm64",
		},
		CreatedAt: now,
	}
}

type fakeControllerTaskLocalAgents struct {
	health         localagent.Health
	healthErr      error
	healthCalls    int
	enrollCalls    int
	reconcileCalls int
	request        localagent.EnrollRequest
}

type fakeControllerTaskRunners struct{}

func (*fakeControllerTaskRunners) ExecuteCreate(context.Context, etcd.TaskRecord) error {
	return nil
}

func (*fakeControllerTaskRunners) ExecuteRemove(context.Context, etcd.TaskRecord) error {
	return nil
}

func (agents *fakeControllerTaskLocalAgents) Enroll(
	_ context.Context,
	request localagent.EnrollRequest,
) (localagent.Agent, error) {
	agents.enrollCalls++
	agents.request = request
	return localagent.Agent{}, nil
}

func (agents *fakeControllerTaskLocalAgents) Reconcile(context.Context) error {
	agents.reconcileCalls++
	return nil
}

func (agents *fakeControllerTaskLocalAgents) Health(
	context.Context,
	string,
) (localagent.Health, error) {
	agents.healthCalls++
	return agents.health, agents.healthErr
}

func (agents *fakeControllerTaskLocalAgents) Remove(context.Context, string) error {
	return nil
}
