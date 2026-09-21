package dispatch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type recordingControllerTaskExecutor struct{ calls int }

func (executor *recordingControllerTaskExecutor) Execute(context.Context, etcd.TaskRecord) error {
	executor.calls++
	return nil
}

type recordingHierarchyDeletionExecutor struct {
	calls  int
	taskID string
}

func (executor *recordingHierarchyDeletionExecutor) Execute(_ context.Context, taskID string) error {
	executor.calls++
	executor.taskID = taskID
	return nil
}

func TestHierarchyDeletionTaskDispatcherRoutesOnlyClosedDeletionTasks(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	fallback := &recordingControllerTaskExecutor{}
	deletions := &recordingHierarchyDeletionExecutor{}
	dispatcher, err := NewHierarchyDispatcher(fallback, deletions)
	if err != nil {
		t.Fatal(err)
	}
	task := etcd.TaskRecord{
		ID:       ids.NewAt(ids.KindTask, at, 1),
		Executor: testtaskjournal.TaskExecutorController,
		Type:     testtaskjournal.TaskRemove,
		Target:   ids.NewAt(ids.KindTenant, at, 2),
		Params: map[string]string{
			testtaskjournal.TaskResourceKindParam:                testtaskjournal.TaskResourceHierarchyDeletion,
			testtaskjournal.TaskHierarchyDeletionOperationParam:  "del_0123456789abcdef0123456789abcdef",
			testtaskjournal.TaskHierarchyDeletionTargetKindParam: "tenant",
		},
	}
	if err = dispatcher.Execute(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if deletions.calls != 1 || deletions.taskID != task.ID || fallback.calls != 0 {
		t.Fatalf("hierarchy/fallback calls = %d/%d, task = %q", deletions.calls, fallback.calls, deletions.taskID)
	}

	task.Params[testtaskjournal.TaskResourceKindParam] = testtaskjournal.TaskResourceAgent
	if err = dispatcher.Execute(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if fallback.calls != 1 || deletions.calls != 1 {
		t.Fatalf("hierarchy/fallback calls after delegation = %d/%d", deletions.calls, fallback.calls)
	}
}

func TestHierarchyDeletionTaskDispatcherRejectsMismatchedTarget(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	dispatcher, err := NewHierarchyDispatcher(
		&recordingControllerTaskExecutor{},
		&recordingHierarchyDeletionExecutor{},
	)
	if err != nil {
		t.Fatal(err)
	}
	task := etcd.TaskRecord{
		ID:       ids.NewAt(ids.KindTask, at, 1),
		Executor: testtaskjournal.TaskExecutorController,
		Type:     testtaskjournal.TaskRemove,
		Target:   ids.NewAt(ids.KindProject, at, 2),
		Params: map[string]string{
			testtaskjournal.TaskResourceKindParam:                testtaskjournal.TaskResourceHierarchyDeletion,
			testtaskjournal.TaskHierarchyDeletionOperationParam:  "del_0123456789abcdef0123456789abcdef",
			testtaskjournal.TaskHierarchyDeletionTargetKindParam: "tenant",
		},
	}
	err = dispatcher.Execute(context.Background(), task)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestHierarchyDeletionTaskDispatcherAcceptsBackingFacadeProjectIdentity(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 28, 21, 0, 0, 0, time.UTC)
	deletions := &recordingHierarchyDeletionExecutor{}
	dispatcher, err := NewHierarchyDispatcher(
		&recordingControllerTaskExecutor{},
		deletions,
	)
	if err != nil {
		t.Fatal(err)
	}
	task := etcd.TaskRecord{
		ID:       ids.NewAt(ids.KindTask, at, 1),
		Executor: testtaskjournal.TaskExecutorController,
		Type:     testtaskjournal.TaskRemove,
		Target:   ids.NewAt(ids.KindProject, at, 2),
		Params: map[string]string{
			testtaskjournal.TaskResourceKindParam:                testtaskjournal.TaskResourceHierarchyDeletion,
			testtaskjournal.TaskHierarchyDeletionOperationParam:  "del_0123456789abcdef0123456789abcdef",
			testtaskjournal.TaskHierarchyDeletionTargetKindParam: "backing-service",
		},
	}

	if err = dispatcher.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if deletions.calls != 1 || deletions.taskID != task.ID {
		t.Fatalf("hierarchy calls = %d, task = %q", deletions.calls, deletions.taskID)
	}
}
