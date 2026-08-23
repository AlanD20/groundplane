package app

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestControllerTaskHandlerAcceptsOnlyClosedScriptRemovalInput(t *testing.T) {
	// Rationale: a Controller-only no-op step is safe only when the Task
	// lifecycle owns an exact Script removal target and no hidden parameters.
	now := time.Now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), Executor: etcd.TaskExecutorController,
		Type: etcd.TaskRemove, Target: ids.New(ids.KindScript),
		Params:    map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceScript},
		CreatedAt: now,
	}
	handler := &controllerTaskHandler{}
	if err := handler.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute(valid Script removal): %v", err)
	}
	task.Params["unexpected"] = "value"
	if err := handler.Execute(context.Background(), task); err == nil {
		t.Fatal("Execute() accepted hidden Script removal input")
	}
}
