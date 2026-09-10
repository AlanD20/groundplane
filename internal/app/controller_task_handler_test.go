package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestLocalAgentControllerTaskHandlerEnrollsFromImmutableTaskInput(t *testing.T) {
	t.Parallel()

	task := testAgentEnrollmentTask()
	agents := &fakeControllerTaskLocalAgents{healthErr: errs.New(errs.KindAgentNotFound, "missing")}
	handler, err := newControllerTaskHandler(agents, testBackingZoneCascade(t), &fakeControllerTaskRunners{})
	if err != nil {
		t.Fatalf("newControllerTaskHandler() error = %v", err)
	}
	if err := handler.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := localagent.EnrollRequest{
		AgentID: task.Target, EnrollmentTaskID: task.ID, Image: testAppAgentImage,
		Config: localagent.Config{
			PullIntervalSeconds: 2, MaxConcurrentTasks: 3,
			Labels: map[string]string{"arch": "arm64"},
		},
	}
	if agents.enrollCalls != 1 || !reflect.DeepEqual(agents.request, want) {
		t.Fatalf("Enroll() calls/request = %d/%#v", agents.enrollCalls, agents.request)
	}
}

func TestControllerTaskHandlerFinalizesOwnershipFreeFailedRunner(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	runnerID := ids.NewAt(ids.KindRunner, now, 30)
	tenantID := ids.NewAt(ids.KindTenant, now, 31)
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, now, 32), Executor: etcd.TaskExecutorController,
		Type: etcd.TaskRemove, Target: runnerID,
		Params: map[string]string{
			etcd.TaskResourceKindParam: etcd.TaskResourceRunner,
			etcd.RunnerTenantIDParam:   tenantID, etcd.RunnerOwnerKindParam: string(etcd.RunnerOwnerTenant),
			etcd.RunnerOwnerIDParam: tenantID, etcd.RunnerHostSlotParam: "1",
			etcd.RunnerNetworkCIDRParam: "10.0.0.0/29",
		},
	}
	runners := &fakeControllerTaskRunners{runner: etcd.Versioned[etcd.RunnerRecord]{Record: etcd.RunnerRecord{
		RunnerLifecycleRecord: etcd.RunnerLifecycleRecord{
			RunnerID: runnerID, ProvisioningState: etcd.RunnerProvisioningFailed,
		},
	}}}
	handler, err := newControllerTaskHandler(
		&fakeControllerTaskLocalAgents{}, testBackingZoneCascade(t), runners,
	)
	if err != nil {
		t.Fatalf("newControllerTaskHandler() error = %v", err)
	}
	if err := handler.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestLocalAgentControllerTaskHandlerReconcilesMatchingReplay(t *testing.T) {
	t.Parallel()

	task := testAgentEnrollmentTask()
	agents := &fakeControllerTaskLocalAgents{health: localagent.Health{Agent: localagent.Agent{
		ID: task.Target, EnrollmentTaskID: task.ID, Phase: localagent.PhaseReady,
	}}}
	handler, err := newControllerTaskHandler(agents, testBackingZoneCascade(t), &fakeControllerTaskRunners{})
	if err != nil {
		t.Fatalf("newControllerTaskHandler() error = %v", err)
	}
	if err := handler.Execute(context.Background(), task); err != nil {
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
	handler, err := newControllerTaskHandler(
		agents,
		testBackingZoneCascade(t),
		&fakeControllerTaskRunners{},
	)
	if err != nil {
		t.Fatalf("newControllerTaskHandler() error = %v", err)
	}
	if err := handler.Execute(context.Background(), retry); err != nil {
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
	handler, err := newControllerTaskHandler(agents, testBackingZoneCascade(t), &fakeControllerTaskRunners{})
	if err != nil {
		t.Fatalf("newControllerTaskHandler() error = %v", err)
	}
	err = handler.Execute(context.Background(), task)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || agents.reconcileCalls != 0 {
		t.Fatalf("Execute(mismatch) error/calls = %v/%d", err, agents.reconcileCalls)
	}
}

func TestLocalAgentControllerTaskHandlerRejectsUnclosedParams(t *testing.T) {
	t.Parallel()

	task := testAgentEnrollmentTask()
	task.Params["surprise"] = "value"
	handler, err := newControllerTaskHandler(
		&fakeControllerTaskLocalAgents{},
		testBackingZoneCascade(t),
		&fakeControllerTaskRunners{},
	)
	if err != nil {
		t.Fatalf("newControllerTaskHandler() error = %v", err)
	}
	if err := handler.Execute(context.Background(), task); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Execute(unclosed params) error = %v, want validation.failed", err)
	}
}

func testAgentEnrollmentTask() etcd.TaskRecord {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, now, 1)
	config := localagent.Config{
		PullIntervalSeconds: 2, MaxConcurrentTasks: 3,
		Labels: map[string]string{"arch": "arm64"},
	}
	return etcd.TaskRecord{
		ID: taskID, Executor: etcd.TaskExecutorController,
		Type: etcd.TaskCreate, Target: ids.NewAt(ids.KindAgent, now, 2),
		Params: agentEnrollmentTaskParams(taskID, testAppAgentImage, config), CreatedAt: now,
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

type fakeControllerTaskRunners struct {
	runner etcd.Versioned[etcd.RunnerRecord]
	err    error
}

func (runners *fakeControllerTaskRunners) GetRunner(
	_ context.Context,
	_ string,
) (etcd.Versioned[etcd.RunnerRecord], error) {
	return runners.runner, runners.err
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
