package app

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: a never-applied Entry finalizer is a closed Controller-local
// no-op; only terminal Task acknowledgement may delete its durable state.
func TestControllerTaskHandlerAcceptsOnlyExactEntryRemoval(t *testing.T) {
	t.Parallel()
	handler, err := newControllerTaskHandler(&fakeControllerTaskLocalAgents{}, testBackingZoneCascade(t), &fakeControllerTaskRunners{})
	if err != nil {
		t.Fatalf("newControllerTaskHandler() error = %v", err)
	}
	valid := etcd.TaskRecord{
		ID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", Executor: etcd.TaskExecutorController,
		Type: etcd.TaskRemove, Target: "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Params: map[string]string{
			etcd.TaskResourceKindParam:     etcd.TaskResourceEntry,
			etcd.TaskEntryEnvironmentParam: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
	}
	if err := handler.Execute(context.Background(), valid); err != nil {
		t.Fatalf("Execute(valid Entry removal) error = %v", err)
	}
	for name, mutate := range map[string]func(*etcd.TaskRecord){
		"wrong type":   func(task *etcd.TaskRecord) { task.Type = etcd.TaskCreate },
		"wrong target": func(task *etcd.TaskRecord) { task.Target = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV" },
		"wrong environment": func(task *etcd.TaskRecord) {
			task.Params[etcd.TaskEntryEnvironmentParam] = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		},
		"extra parameter": func(task *etcd.TaskRecord) { task.Params["extra"] = "value" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := valid
			candidate.Params = map[string]string{
				etcd.TaskResourceKindParam:     etcd.TaskResourceEntry,
				etcd.TaskEntryEnvironmentParam: valid.Params[etcd.TaskEntryEnvironmentParam],
			}
			mutate(&candidate)
			if err := handler.Execute(context.Background(), candidate); err == nil {
				t.Fatal("Execute(invalid Entry removal) succeeded")
			}
		})
	}
}
