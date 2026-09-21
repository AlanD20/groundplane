package etcd

import (
	"errors"
	"testing"
	"time"

	testtaskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BACK-13/BACK-14: a successful operation cannot outrun its durable hook
// result, while a failed hook leaves the operation failed without inventing a
// successful RESULT checkpoint.
func TestBackingAfterStartTerminalRequiresResultAndPreservesFailure(t *testing.T) {
	for _, test := range []struct {
		name   string
		status testtaskjournal.TaskStatus
	}{
		{name: "completion requires result", status: testtaskjournal.TaskStatusCompleted},
		{name: "failed hook blocks operation", status: testtaskjournal.TaskStatusFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 9, 20, 16, 0, 0, 0, time.UTC)
			store := newMemoryTaskStore()
			task := validTaskRecord(now)
			serviceID := task.Target
			task.Params = map[string]string{
				testtaskjournal.TaskBackingServiceCreationParam:         serviceID,
				testtaskconfiguration.TaskBackingServiceAfterStartParam: serviceID,
			}
			tasks, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			createLifecycleTask(t, tasks, task)
			claim, found, err := tasks.ClaimNextTask(
				t.Context(), taskEventTestAgentID, 1, now.Add(time.Second),
			)
			if err != nil || !found || claim.Task.Record.ID != task.ID {
				t.Fatalf("ClaimNextTask() = %#v/%t/%v", claim, found, err)
			}
			assignment := claim.Assignment.Record
			result := completedComposeTaskResult()
			result.ExecutionEpoch = assignment.ExecutionEpoch
			if test.status == testtaskjournal.TaskStatusFailed {
				result.ExitCode = 1
				terminal, acknowledgeErr := tasks.AcknowledgeTask(
					t.Context(),
					taskEventTestAgentID,
					1,
					task.ID,
					assignment.AssignmentID,
					testtaskjournal.TaskStatusFailed,
					result,
					now.Add(time.Second),
				)
				if acknowledgeErr != nil || terminal.Record.Status != testtaskjournal.TaskStatusFailed {
					t.Fatalf("failed hook acknowledgement = %#v, %v", terminal, acknowledgeErr)
				}
				return
			}

			if _, acknowledgeErr := tasks.AcknowledgeTask(
				t.Context(), taskEventTestAgentID, 1, task.ID, assignment.AssignmentID, testtaskjournal.TaskStatusCompleted, result, now.Add(time.Second),
			); !errors.Is(acknowledgeErr, errs.New(errs.KindStateConflict, "")) {
				t.Fatalf("completion without RESULT error = %v", acknowledgeErr)
			}
			current, err := tasks.GetTask(t.Context(), task.ID)
			if err != nil || current.Record.Status != testtaskjournal.TaskStatusRunning {
				t.Fatalf("rejected completion changed Task = %#v, %v", current, err)
			}
		})
	}
}
