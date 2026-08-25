package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestBackupTaskTerminalTransactionFailureLeavesRunningAuthorityIntact(t *testing.T) {
	// Rationale: a failed terminal transaction must preserve the exact running
	// assignment and all domain authority so one exact retry can commit them together.
	t.Run("Backup", func(t *testing.T) {
		runtime, store, tasks, run, claim := publishBackupTerminalLifecycleTask(t)
		unknown := errs.New(errs.KindStorageUnavailable, "terminal transaction unavailable")
		failingStore := &backupTerminalTransactionFailureStore{
			hierarchyStore: store,
			failNext:       unknown,
		}
		failingTasks, err := newTaskRepository(failingStore)
		if err != nil {
			t.Fatal(err)
		}
		result := completedComposeTaskResult()
		result.ExitCode = 1
		terminalAt := claim.Assignment.Record.AssignedAt.Add(time.Second)
		if _, err := failingTasks.AcknowledgeTask(
			context.Background(), claim.Assignment.Record.AgentID,
			claim.Assignment.Record.AgentGeneration, run.TaskID,
			claim.Assignment.Record.AssignmentID, TaskStatusFailed, result, terminalAt,
		); !errors.Is(err, unknown) {
			t.Fatalf("AcknowledgeTask(transaction failure) error = %v", err)
		}
		assertRunningBackupTerminalAuthority(t, tasks, store, run.TaskID, run.EnvironmentID, claim)
		currentRun, err := runtime.GetBackupRun(context.Background(), run.TaskID)
		if err != nil || !backupRunRecordsEqual(currentRun.Record, run) {
			t.Fatalf("Backup run after transaction failure = %#v, %v", currentRun, err)
		}
		terminal, err := failingTasks.AcknowledgeTask(
			context.Background(), claim.Assignment.Record.AgentID,
			claim.Assignment.Record.AgentGeneration, run.TaskID,
			claim.Assignment.Record.AssignmentID, TaskStatusFailed, result, terminalAt,
		)
		if err != nil || terminal.Record.Status != TaskStatusFailed {
			t.Fatalf("AcknowledgeTask(retry) = %#v, %v", terminal, err)
		}
		failedRun, err := runtime.GetBackupRun(context.Background(), run.TaskID)
		if err != nil || failedRun.Record.State != BackupRunFailed || failedRun.Revision != terminal.Revision {
			t.Fatalf("Backup run after retry = %#v, %v", failedRun, err)
		}
	})

	t.Run("prune", func(t *testing.T) {
		_, store, tasks, dispatch, pointID := publishBackupPruneLifecycleTask(t)
		agentID := ids.NewAt(ids.KindAgent, dispatch.CreatedAt, 4101)
		claim, found, err := tasks.ClaimNextTask(
			context.Background(), agentID, 1, dispatch.CreatedAt.Add(time.Second),
		)
		if err != nil || !found || claim.Task.Record.ID != dispatch.TaskID {
			t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
		}
		unknown := errs.New(errs.KindStorageUnavailable, "terminal transaction unavailable")
		failingStore := &backupTerminalTransactionFailureStore{
			hierarchyStore: store,
			failNext:       unknown,
		}
		failingTasks, err := newTaskRepository(failingStore)
		if err != nil {
			t.Fatal(err)
		}
		result := completedComposeTaskResult()
		result.ExitCode = 1
		terminalAt := claim.Assignment.Record.AssignedAt.Add(time.Second)
		if _, err := failingTasks.AcknowledgeTask(
			context.Background(), agentID, 1, dispatch.TaskID,
			claim.Assignment.Record.AssignmentID, TaskStatusFailed, result, terminalAt,
		); !errors.Is(err, unknown) {
			t.Fatalf("AcknowledgeTask(transaction failure) error = %v", err)
		}
		assertRunningBackupTerminalAuthority(
			t, tasks, store, dispatch.TaskID, dispatch.EnvironmentID, claim,
		)
		dispatchEntry := mustOptionalKey(t, store, backupRecoveryPointPruneDispatchKey(dispatch.TaskID))
		pruneEntry := mustOptionalKey(t, store, backupRecoveryPointPruneKey(pointID))
		prune, decodeErr := decodeBackupRecoveryPointPruneRecord(pruneEntry.Value)
		if dispatchEntry == nil || pruneEntry == nil || decodeErr != nil ||
			prune.State != BackupPruneAssigned || prune.TaskID != dispatch.TaskID {
			t.Fatalf("prune authority after transaction failure = %#v/%#v/%v", dispatchEntry, prune, decodeErr)
		}
		terminal, err := failingTasks.AcknowledgeTask(
			context.Background(), agentID, 1, dispatch.TaskID,
			claim.Assignment.Record.AssignmentID, TaskStatusFailed, result, terminalAt,
		)
		if err != nil || terminal.Record.Status != TaskStatusFailed {
			t.Fatalf("AcknowledgeTask(retry) = %#v, %v", terminal, err)
		}
		pendingEntry := mustOptionalKey(t, store, backupRecoveryPointPruneKey(pointID))
		pending, decodeErr := decodeBackupRecoveryPointPruneRecord(pendingEntry.Value)
		if decodeErr != nil || pending.State != BackupPrunePending || pending.TaskID != "" ||
			pendingEntry.ModRevision != terminal.Revision {
			t.Fatalf("prune authority after retry = %#v/%#v/%v", pendingEntry, pending, decodeErr)
		}
	})
}

func TestTaskRepositoryMixedTimeoutCollectorsContinueAfterPrune(t *testing.T) {
	// Rationale: specialized prune terminalization must not stop either bounded
	// timeout collector from reaching later ordinary assignments in the same pass.
	for _, test := range []struct {
		name    string
		timeout func(*TaskRepository, string, uint64, time.Time) (int, error)
	}{
		{
			name: "deadline collector",
			timeout: func(tasks *TaskRepository, _ string, _ uint64, terminalAt time.Time) (int, error) {
				return tasks.ExpireTimedOutTasks(context.Background(), terminalAt)
			},
		},
		{
			name: "Agent collector",
			timeout: func(tasks *TaskRepository, agentID string, generation uint64, terminalAt time.Time) (int, error) {
				return tasks.TimeoutAgentAssignments(
					context.Background(), agentID, generation, 2, terminalAt,
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, store, tasks, dispatch, pointID := publishBackupPruneLifecycleTask(t)
			ordinary := validTaskRecord(dispatch.CreatedAt.Add(time.Second))
			ordinary.TimeoutSeconds = backupTaskTimeoutSeconds
			createLifecycleTask(t, tasks, ordinary)
			agentID := ids.NewAt(ids.KindAgent, dispatch.CreatedAt, 4102)
			pruneClaim, found, err := tasks.ClaimNextTask(
				context.Background(), agentID, 1, dispatch.CreatedAt.Add(2*time.Second),
			)
			if err != nil || !found || pruneClaim.Task.Record.ID != dispatch.TaskID {
				t.Fatalf("ClaimNextTask(prune) = %#v/%v/%v", pruneClaim, found, err)
			}
			ordinaryClaim, found, err := tasks.ClaimNextTask(
				context.Background(), agentID, 1, dispatch.CreatedAt.Add(3*time.Second),
			)
			if err != nil || !found || ordinaryClaim.Task.Record.ID != ordinary.ID {
				t.Fatalf("ClaimNextTask(ordinary) = %#v/%v/%v", ordinaryClaim, found, err)
			}
			count, err := test.timeout(
				tasks, agentID, 1, ordinaryClaim.Assignment.Record.Deadline,
			)
			if err != nil || count != 2 {
				t.Fatalf("mixed timeout collector = %d, %v", count, err)
			}
			for _, taskID := range []string{dispatch.TaskID, ordinary.ID} {
				terminal, getErr := tasks.GetTask(context.Background(), taskID)
				if getErr != nil || terminal.Record.Status != TaskStatusTimedOut {
					t.Fatalf("timed-out Task %s = %#v, %v", taskID, terminal, getErr)
				}
			}
			pendingEntry := mustOptionalKey(t, store, backupRecoveryPointPruneKey(pointID))
			pending, decodeErr := decodeBackupRecoveryPointPruneRecord(pendingEntry.Value)
			if decodeErr != nil || pending.State != BackupPrunePending || pending.TaskID != "" {
				t.Fatalf("timed-out prune authority = %#v/%#v/%v", pendingEntry, pending, decodeErr)
			}
		})
	}
}

func TestTaskRepositoryMixedTimeoutCollectorsContinueAfterBackup(t *testing.T) {
	// Rationale: Backup run terminalization must not stop either bounded timeout
	// collector from reaching later ordinary assignments in the same pass.
	for _, test := range []struct {
		name    string
		timeout func(*TaskRepository, string, uint64, time.Time) (int, error)
	}{
		{
			name: "deadline collector",
			timeout: func(tasks *TaskRepository, _ string, _ uint64, terminalAt time.Time) (int, error) {
				return tasks.ExpireTimedOutTasks(context.Background(), terminalAt)
			},
		},
		{
			name: "Agent collector",
			timeout: func(tasks *TaskRepository, agentID string, generation uint64, terminalAt time.Time) (int, error) {
				return tasks.TimeoutAgentAssignments(
					context.Background(), agentID, generation, 2, terminalAt,
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, tasks, run, backupClaim := publishBackupTerminalLifecycleTask(t)
			ordinary := validTaskRecord(run.CreatedAt.Add(time.Second))
			ordinary.TimeoutSeconds = backupTaskTimeoutSeconds
			createLifecycleTask(t, tasks, ordinary)
			agentID := backupClaim.Assignment.Record.AgentID
			generation := backupClaim.Assignment.Record.AgentGeneration
			ordinaryClaim, found, err := tasks.ClaimNextTask(
				context.Background(), agentID, generation, run.CreatedAt.Add(3*time.Second),
			)
			if err != nil || !found || ordinaryClaim.Task.Record.ID != ordinary.ID {
				t.Fatalf("ClaimNextTask(ordinary) = %#v/%v/%v", ordinaryClaim, found, err)
			}
			count, err := test.timeout(
				tasks, agentID, generation, ordinaryClaim.Assignment.Record.Deadline,
			)
			if err != nil || count != 2 {
				t.Fatalf("mixed timeout collector = %d, %v", count, err)
			}
			for _, taskID := range []string{run.TaskID, ordinary.ID} {
				terminal, getErr := tasks.GetTask(context.Background(), taskID)
				if getErr != nil || terminal.Record.Status != TaskStatusTimedOut {
					t.Fatalf("timed-out Task %s = %#v, %v", taskID, terminal, getErr)
				}
			}
		})
	}
}

type backupTerminalTransactionFailureStore struct {
	hierarchyStore
	failNext error
}

func (store *backupTerminalTransactionFailureStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if store.failNext != nil {
		failure := store.failNext
		store.failNext = nil
		return TransactionResult{}, failure
	}
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}

func publishBackupTerminalLifecycleTask(
	t *testing.T,
) (*BackupRuntimeRepository, *memoryHierarchyStore, *TaskRepository, BackupRunRecord, TaskAssignment) {
	t.Helper()
	runtime, store, run := newBackupRuntimeBareFixture(t)
	runPlan, err := runtime.prepareBackupRunPublication(
		context.Background(), run, backupRuntimeOperationLock(run),
		backupRuntimeCurrentRevision(t, store, run.EnvironmentID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer runPlan.clear()
	task, sealed, marker, initiation := backupRuntimePublicationTask(t, store, run)
	publication, err := runPlan.taskIdempotencyPlan(task, sealed, marker, initiation)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := idempotency.Apply(context.Background(), marker, publication); err != nil {
		t.Fatal(err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	agentID := ids.NewAt(ids.KindAgent, run.CreatedAt, 4100)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(), agentID, 1, run.CreatedAt.Add(time.Second),
	)
	if err != nil || !found || claim.Task.Record.ID != run.TaskID {
		t.Fatalf("ClaimNextTask() = %#v/%v/%v", claim, found, err)
	}
	return runtime, store, tasks, run, claim
}

func assertRunningBackupTerminalAuthority(
	t *testing.T,
	tasks *TaskRepository,
	store *memoryHierarchyStore,
	taskID string,
	environmentID string,
	claim TaskAssignment,
) {
	t.Helper()
	current, err := tasks.GetTask(context.Background(), taskID)
	if err != nil || current.Record.Status != TaskStatusRunning || current.Revision != claim.Task.Revision {
		t.Fatalf("Task after terminal transaction failure = %#v, %v", current, err)
	}
	assignment, err := tasks.GetTaskAssignment(context.Background(), taskID)
	if err != nil || assignment.Assignment.Record != claim.Assignment.Record ||
		assignment.Assignment.Revision != claim.Assignment.Revision {
		t.Fatalf("assignment after terminal transaction failure = %#v, %v", assignment, err)
	}
	if mustOptionalKey(t, store, environmentOperationLockKey(environmentID)) == nil {
		t.Fatal("terminal transaction failure released the Environment lock")
	}
}
