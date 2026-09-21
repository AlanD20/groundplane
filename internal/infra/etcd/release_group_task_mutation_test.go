package etcd

import (
	errors "errors"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	testing "testing"
	time "time"
)

func TestReleaseGroupRemovalTaskRejectsOpenEndedParams(t *testing.T) {

	t.Parallel()

	now := time.Date(2026, 8, 26, 15, 20, 0, 0, time.UTC)
	task := releaseGroupMutationValidationTask(now, ids.NewAt(ids.KindReleaseGroup, now, 1), testtaskjournal.TaskRemove)
	task.Params["unexpected"] = "value"
	if _, err := taskOwnsReleaseGroupRemoval(task); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("taskOwnsReleaseGroupRemoval() error = %v, want internal boundary failure", err)
	}
}

func releaseGroupMutationValidationTask(now time.Time, groupID string, taskType testtaskjournal.TaskType) TaskRecord {
	return TaskRecord{
		ID: ids.NewAt(ids.KindTask, now, 3), Executor: testtaskjournal.TaskExecutorController, Type: taskType,
		Target: groupID, Status: testtaskjournal.TaskStatusPending, Params: map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceReleaseGroup},
	}
}
