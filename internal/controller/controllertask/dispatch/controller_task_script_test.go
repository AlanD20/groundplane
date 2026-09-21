package dispatch

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func TestControllerTaskHandlerAcceptsOnlyClosedScriptRemovalInput(t *testing.T) {
	// Rationale: a Controller-only no-op step is safe only when the Task
	// lifecycle owns an exact Script removal target and no hidden parameters.
	now := time.Now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), Executor: testtaskjournal.TaskExecutorController,
		Type: testtaskjournal.TaskRemove, Target: ids.New(ids.KindScript),
		Params:    map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceScript},
		CreatedAt: now,
	}
	handler := &ResourceHandler{}
	if err := handler.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute(valid Script removal): %v", err)
	}
	task.Params["unexpected"] = "value"
	if err := handler.Execute(context.Background(), task); err == nil {
		t.Fatal("Execute() accepted hidden Script removal input")
	}
}
