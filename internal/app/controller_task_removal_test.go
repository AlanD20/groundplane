package app

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testControllerRemovalTaskID  = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testControllerRemovalAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

func TestControllerTaskHandlerExecutesExactAgentRemoval(t *testing.T) {
	t.Parallel()

	agents := &removalTaskAgents{}
	handler, err := newControllerTaskHandler(agents, testBackingZoneCascade(t))
	if err != nil {
		t.Fatalf("newControllerTaskHandler() error = %v", err)
	}
	err = handler.Execute(context.Background(), validAgentRemovalTask())
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if agents.removedAgentID != testControllerRemovalAgentID {
		t.Fatalf("Remove() Agent ID = %q", agents.removedAgentID)
	}
}

func TestControllerTaskHandlerRejectsInvalidAgentRemovalParameters(t *testing.T) {
	t.Parallel()

	for name, params := range map[string]map[string]string{
		"missing resource": {},
		"wrong resource":   {etcd.TaskResourceKindParam: "service"},
		"extra parameter": {
			etcd.TaskResourceKindParam: etcd.TaskResourceAgent,
			"unexpected":               "value",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			agents := &removalTaskAgents{}
			handler, err := newControllerTaskHandler(agents, testBackingZoneCascade(t))
			if err != nil {
				t.Fatalf("newControllerTaskHandler() error = %v", err)
			}
			task := validAgentRemovalTask()
			task.Params = params
			err = handler.Execute(context.Background(), task)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Execute() error = %v, want %s", err, errs.CodeValidationFailed)
			}
			if agents.removedAgentID != "" {
				t.Fatalf("Remove() Agent ID = %q after rejected Task", agents.removedAgentID)
			}
		})
	}
}

func validAgentRemovalTask() etcd.TaskRecord {
	return etcd.TaskRecord{
		ID:       testControllerRemovalTaskID,
		Executor: etcd.TaskExecutorController,
		Type:     etcd.TaskRemove,
		Target:   testControllerRemovalAgentID,
		Params:   map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceAgent},
	}
}

type removalTaskAgents struct {
	removedAgentID string
}

func (agents *removalTaskAgents) Enroll(
	context.Context,
	localagent.EnrollRequest,
) (localagent.Agent, error) {
	return localagent.Agent{}, errs.New(errs.KindInternal, "unexpected enrollment")
}

func (agents *removalTaskAgents) Reconcile(context.Context) error {
	return errs.New(errs.KindInternal, "unexpected reconciliation")
}

func (agents *removalTaskAgents) Update(context.Context, localagent.UpdateRequest) error {
	return errs.New(errs.KindInternal, "unexpected update")
}

func (agents *removalTaskAgents) Health(context.Context, string) (localagent.Health, error) {
	return localagent.Health{}, errs.New(errs.KindInternal, "unexpected health lookup")
}

func (agents *removalTaskAgents) Remove(_ context.Context, agentID string) error {
	agents.removedAgentID = agentID
	return nil
}
