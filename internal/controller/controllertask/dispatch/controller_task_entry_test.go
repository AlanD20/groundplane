package dispatch

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: a never-applied Entry finalizer is a closed Controller-local
// no-op; only terminal Task acknowledgement may delete its durable state.
func TestControllerTaskHandlerAcceptsOnlyExactEntryRemoval(t *testing.T) {
	t.Parallel()
	handler, err := NewResourceHandler(
		&fakeControllerTaskLocalAgents{},
		testBackingZoneCascade(t),
		&fakeControllerTaskRunners{},
	)
	if err != nil {
		t.Fatalf("NewResourceHandler() error = %v", err)
	}
	valid := etcd.TaskRecord{
		ID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", Executor: testtaskjournal.TaskExecutorController,
		Type: testtaskjournal.TaskRemove, Target: "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Params: map[string]string{
			testtaskjournal.TaskResourceKindParam:     testtaskjournal.TaskResourceEntry,
			testtaskjournal.TaskEntryEnvironmentParam: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
	}
	if err := handler.Execute(context.Background(), valid); err != nil {
		t.Fatalf("Execute(valid Entry removal) error = %v", err)
	}
	for name, mutate := range map[string]func(*etcd.TaskRecord){
		"wrong type":   func(task *etcd.TaskRecord) { task.Type = testtaskjournal.TaskCreate },
		"wrong target": func(task *etcd.TaskRecord) { task.Target = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV" },
		"wrong environment": func(task *etcd.TaskRecord) {
			task.Params[testtaskjournal.TaskEntryEnvironmentParam] = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		},
		"extra parameter": func(task *etcd.TaskRecord) { task.Params["extra"] = "value" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := valid
			candidate.Params = map[string]string{
				testtaskjournal.TaskResourceKindParam:     testtaskjournal.TaskResourceEntry,
				testtaskjournal.TaskEntryEnvironmentParam: valid.Params[testtaskjournal.TaskEntryEnvironmentParam],
			}
			mutate(&candidate)
			if err := handler.Execute(context.Background(), candidate); err == nil {
				t.Fatal("Execute(invalid Entry removal) succeeded")
			}
		})
	}
}
