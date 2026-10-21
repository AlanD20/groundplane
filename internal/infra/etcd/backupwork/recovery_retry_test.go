package backupwork

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A failed Backup/Restore must not become permission to repeat capture or a
// destructive restore. Only unresolved cleanup of the exact captured input may
// proceed to source authorization. The read probe proves the rejection precedes
// input/credential access; it does not qualify successful physical recovery.
func TestRecoveryRetryRefusesForwardReplayAndChangedCapture(t *testing.T) {
	for _, operation := range []workexecution.Operation{workexecution.Backup, workexecution.Restore, workexecution.BackupPrune} {
		t.Run(string(operation), func(t *testing.T) {
			_, resume, _, record, _, evidence, at := orphanCleanupFixture(t, false)
			view, previous := resume.Work, resume.Attempt
			view.Task.Task = workexecution.Task{ID: record.TaskID, Action: workexecution.ActionBackup,
				Actor: workexecution.OperatorActor, TargetID: record.EnvironmentID, Intent: previous.Attempt.Input,
				ExpectedItems: 1, PublicationComplete: true, Progress: workexecution.Progress{Attention: 1},
				CreatedAt: record.CreatedAt, UpdatedAt: at, Deadline: at.Add(time.Hour)}
			view.Item.Item.TaskID, view.Item.Item.TargetID = record.TaskID, record.EnvironmentID
			view.Item.Item.Placement = workexecution.Placement{
				Executor: workexecution.Agent,
				HostID:   previous.Attempt.Executor.HostID,
			}
			view.Item.Item.Claims = []workexecution.Claim{
				{HostID: previous.Attempt.Executor.HostID, ScopeID: record.EnvironmentID},
			}
			reference := previous.Attempt.Input
			view.Item.Item.Input = &reference
			view.Item.Item.CreatedAt, view.Item.Item.UpdatedAt = record.CreatedAt, at
			view.Item.Item.Deadline, view.Item.Item.AttemptNumber = at.Add(time.Hour), 1
			view.Item.Item.Operation = operation
			view.Item.Item.Phase, view.Item.Item.Cleanup = workexecution.Attention, workexecution.CleanupStopped
			view.Item.Item.Outcome = workexecution.Failed
			view.Item.Item.ActiveAttemptID = ""
			previous.Attempt.FinishedAt, previous.Attempt.Result = &at, &evidence
			previous.Attempt.Number = view.Item.Item.AttemptNumber
			if err := view.Task.Task.Validate(); err != nil {
				t.Fatal(err)
			}
			if err := view.Item.Item.Validate(); err != nil {
				t.Fatal(err)
			}
			for _, scenario := range []string{"captured cleanup", "settled forward failure", "changed input"} {
				t.Run(scenario, func(t *testing.T) {
					selected, attempt := view, previous
					store := &credentialInputReadProbe{}
					owner, err := NewInputRepository(store)
					if err != nil {
						t.Fatal(err)
					}
					switch scenario {
					case "settled forward failure":
						selected.Item.Item.Phase = workexecution.Settled
						selected.Item.Item.Outcome = workexecution.Failed
						selected.Item.Item.Cleanup = workexecution.CleanupComplete
					case "changed input":
						attempt.Attempt.Input.SHA256 = strings.Repeat("f", 64)
					}
					change, err := owner.PrepareRecoveryRetry(t.Context(), selected, attempt, at.Add(time.Second))
					if len(change.Mutations) != 0 {
						t.Fatal("unproved Retry published execution changes")
					}
					if scenario == "captured cleanup" {
						if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) || store.calls != 1 {
							t.Fatalf(
								"eligible cleanup did not reach captured source authorization: %v, reads=%d",
								err,
								store.calls,
							)
						}
					} else if !errors.Is(err, errs.New(errs.KindTaskNotRetryable, "")) || store.calls != 0 {
						t.Fatalf("unsafe Retry reached captured storage: %v, reads=%d", err, store.calls)
					}
				})
			}
		})
	}
}
