package tasks

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: native activation cannot use ordinary retry to clone stale host
// authority. The API must reject before either intent or Task publication.
func TestTaskRetryRejectsNativeControllerUpdate(t *testing.T) {
	const taskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	repository := &fakeTaskRetryRepository{}
	repository.task.ID = taskID
	repository.task.Params = map[string]string{"resource_kind": "controller"}
	service, err := NewRetryService(repository, &fakeTaskRetryIdempotency{}, &fakeBackupTaskRetryer{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RetryTask(context.Background(), taskID, "native-retry-refused-0001"); !errors.Is(
		err,
		errs.New(errs.KindTaskNotRetryable, ""),
	) || repository.mutationCalls != 0 {
		t.Fatalf("native retry = %v, writes %d", err, repository.mutationCalls)
	}
}
