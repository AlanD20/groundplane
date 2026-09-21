package dispatch

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: a Secret finalizer is a closed Controller-local no-op whose
// durable effect belongs to Task acknowledgement, not the execution handler.
func TestControllerTaskHandlerAcceptsOnlyExactSecretRemoval(t *testing.T) {
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
		Type: testtaskjournal.TaskRemove, Target: "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Params: map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceSecret},
	}
	if err := handler.Execute(context.Background(), valid); err != nil {
		t.Fatalf("Execute(valid Secret removal) error = %v", err)
	}
	for name, mutate := range map[string]func(*etcd.TaskRecord){
		"wrong type":      func(task *etcd.TaskRecord) { task.Type = testtaskjournal.TaskCreate },
		"wrong target":    func(task *etcd.TaskRecord) { task.Target = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV" },
		"extra parameter": func(task *etcd.TaskRecord) { task.Params["extra"] = "value" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			candidate := valid
			candidate.Params = map[string]string{
				testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceSecret,
			}
			mutate(&candidate)
			if err := handler.Execute(context.Background(), candidate); err == nil {
				t.Fatal("Execute(invalid Secret removal) succeeded")
			}
		})
	}
}
