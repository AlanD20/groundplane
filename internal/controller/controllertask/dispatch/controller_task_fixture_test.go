package dispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func newTestResourceHandler(agents controllerTaskLocalAgents,
	runners controllerTaskRunnerLifecycle, images Executor,
) (*ResourceHandler, error) {
	checks := &recordingControllerTaskExecutor{}
	return NewResourceHandler(agents, images, checks, checks, checks, checks, checks, runners)
}

type controllerProgressJournal struct{}

func (controllerProgressJournal) AppendControllerTaskEvent(ctx context.Context, _ taskjournal.TaskEventInput,
	_ time.Time,
) (etcd.TaskEventAppend, error) {
	return etcd.TaskEventAppend{}, ctx.Err()
}

func testControllerProgress(ctx context.Context, task etcd.TaskRecord) *controllertask.Progress {
	claim := etcd.TaskAssignment{Task: keyvalue.Versioned[etcd.TaskRecord]{Record: task}}
	return controllertask.NewProgress(ctx, controllerProgressJournal{}, claim, time.Now)
}

type recordingControllerTaskHandler struct {
	executor *recordingControllerTaskExecutor
}

func (handler recordingControllerTaskHandler) Execute(ctx context.Context, task etcd.TaskRecord,
	_ *controllertask.Progress,
) error {
	return handler.executor.Execute(ctx, task)
}

func testHierarchyDispatcher(fallback *recordingControllerTaskExecutor,
	deletions HierarchyExecutor,
) (*HierarchyDispatcher, error) {
	return NewHierarchyDispatcher(recordingControllerTaskHandler{executor: fallback}, deletions)
}
