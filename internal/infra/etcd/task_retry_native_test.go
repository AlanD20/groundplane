package etcd

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: storage rejects native retry even if an internal caller bypasses
// HTTP admission. Stale executable/Agent pins require a fresh explicit update.
func TestNativeControllerTaskCannotBeClonedForRetry(t *testing.T) {
	now := taskJournalTime()
	source := validTaskRecord(now)
	source.Params[TaskResourceKindParam] = TaskResourceController
	running, err := transitionTaskStatus(source, TaskStatusPending, TaskStatusRunning, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	failed, err := transitionTaskStatus(running, TaskStatusRunning, TaskStatusFailed, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cloneRetryTask(failed, ids.New(ids.KindTask), TaskActorOperator, now.Add(3*time.Second)); !errors.Is(
		err,
		errs.New(errs.KindTaskNotRetryable, ""),
	) {
		t.Fatalf("native Task retry = %v", err)
	}
}
