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

func TestControllerTaskHandlerExecutesExactAgentUpdate(t *testing.T) {
	t.Parallel()

	agents := &fakeControllerTaskLocalAgents{}
	handler, err := newControllerTaskHandler(agents, testBackingZoneCascade(t), &fakeControllerTaskRunners{})
	if err != nil {
		t.Fatalf("newControllerTaskHandler() error = %v", err)
	}
	now := time.Date(2026, time.August, 23, 11, 0, 0, 0, time.UTC)
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, now, 1), Executor: etcd.TaskExecutorController,
		Type: etcd.TaskUpdate, Target: ids.NewAt(ids.KindAgent, now, 2),
		Params: map[string]string{
			etcd.TaskResourceKindParam:     etcd.TaskResourceAgent,
			agentTaskPreviousImageKey:      testAgentUpdatePreviousImage,
			agentTaskImageKey:              testAgentUpdateDesiredImage,
			agentTaskStartingGenerationKey: "7",
		},
	}
	if err := handler.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	want := localagent.UpdateRequest{
		AgentID: task.Target, PreviousImage: testAgentUpdatePreviousImage,
		DesiredImage: testAgentUpdateDesiredImage, StartingGeneration: 7,
	}
	if agents.updateCalls != 1 || !reflect.DeepEqual(agents.updateRequest, want) {
		t.Fatalf("Update() calls/request = %d/%#v", agents.updateCalls, agents.updateRequest)
	}
}

func TestControllerTaskHandlerRejectsUnclosedAgentUpdate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 23, 11, 0, 0, 0, time.UTC)
	base := map[string]string{
		etcd.TaskResourceKindParam:     etcd.TaskResourceAgent,
		agentTaskPreviousImageKey:      testAgentUpdatePreviousImage,
		agentTaskImageKey:              testAgentUpdateDesiredImage,
		agentTaskStartingGenerationKey: "7",
	}
	tests := map[string]map[string]string{
		"missing generation": {
			etcd.TaskResourceKindParam: etcd.TaskResourceAgent,
			agentTaskPreviousImageKey:  testAgentUpdatePreviousImage,
			agentTaskImageKey:          testAgentUpdateDesiredImage,
		},
		"noncanonical generation": cloneAgentUpdateParams(base, agentTaskStartingGenerationKey, "07"),
		"mutable desired image":   cloneAgentUpdateParams(base, agentTaskImageKey, "groundplane-agent:latest"),
		"unknown field":           cloneAgentUpdateParams(base, "extra", "value"),
	}
	for name, params := range tests {
		name, params := name, params
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			agents := &fakeControllerTaskLocalAgents{}
			handler, err := newControllerTaskHandler(agents, testBackingZoneCascade(t), &fakeControllerTaskRunners{})
			if err != nil {
				t.Fatalf("newControllerTaskHandler() error = %v", err)
			}
			err = handler.Execute(context.Background(), etcd.TaskRecord{
				ID: ids.NewAt(ids.KindTask, now, 1), Executor: etcd.TaskExecutorController,
				Type: etcd.TaskUpdate, Target: ids.NewAt(ids.KindAgent, now, 2), Params: params,
			})
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || agents.updateCalls != 0 {
				t.Fatalf("Execute() error/calls = %v/%d", err, agents.updateCalls)
			}
		})
	}
}

func cloneAgentUpdateParams(source map[string]string, key string, value string) map[string]string {
	cloned := make(map[string]string, len(source)+1)
	for sourceKey, sourceValue := range source {
		cloned[sourceKey] = sourceValue
	}
	cloned[key] = value
	return cloned
}
