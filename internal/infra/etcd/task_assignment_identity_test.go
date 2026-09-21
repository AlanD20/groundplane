package etcd

import (
	"context"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"testing"
	"time"
)

const (
	taskEventTestAssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	taskEventTestAgentID      = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func seedTaskRepositoryRunningTask(t *testing.T, store *memoryTaskStore, task TaskRecord) {
	t.Helper()
	running := task
	if task.Status == testtaskjournal.TaskStatusPending {
		var err error
		running, err = TransitionTaskStatus(
			task,
			testtaskjournal.TaskStatusPending,
			testtaskjournal.TaskStatusRunning,
			task.UpdatedAt.Add(time.Nanosecond),
		)
		if err != nil {
			t.Fatalf("transition seeded Task to running: %v", err)
		}
	}
	seedTaskRepositoryTask(t, store, running)
	assignment := testtaskassignments.TaskAssignmentRecord{
		AssignmentID: taskEventTestAssignmentID,
		TaskID:       running.ID, Executor: testtaskjournal.TaskExecutorAgent,
		AgentID: taskEventTestAgentID, AgentGeneration: 1,
		ClaimedTaskRevision: 1, AssignedAt: *running.StartedAt,
		Deadline:         running.StartedAt.Add(time.Duration(running.TimeoutSeconds) * time.Second),
		RecoveryDeadline: running.StartedAt.Add(2 * time.Duration(running.TimeoutSeconds) * time.Second),
		ExecutionMode:    testtaskassignments.TaskExecutionModeForward, ExecutionEpoch: 1,
	}
	value, err := testtaskassignments.EncodeTaskAssignment(assignment)
	if err != nil {
		t.Fatalf("encode seeded Task assignment: %v", err)
	}
	result, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: testtaskjournal.TaskAssignmentKey(taskEventTestAgentID, running.ID)},
		{Key: testtaskjournal.TaskAssignmentIndexKey(running.ID)},
		{Key: testtaskjournal.TaskTimeoutIndexKey(running.ID, assignment.Deadline)},
	}, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testtaskjournal.TaskAssignmentKey(taskEventTestAgentID, running.ID),
			Value: value,
		},
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskAssignmentIndexKey(running.ID), Value: value},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testtaskjournal.TaskTimeoutIndexKey(running.ID, assignment.Deadline),
			Value: value,
		},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Task assignment = %#v, %v", result, err)
	}
}

func taskAssignmentIDForTest(
	t *testing.T,
	repository *TaskRepository,
	taskID string,
) string {
	t.Helper()
	assignment, err := repository.GetTaskAssignment(context.Background(), taskID)
	if err == nil {
		return assignment.Assignment.Record.AssignmentID
	}
	task, taskErr := repository.GetTask(context.Background(), taskID)
	if taskErr != nil || task.Record.TerminalAssignment == nil {
		t.Fatalf("durable Task assignment identity for %s = %#v, %v / %v", taskID, task, err, taskErr)
	}
	return task.Record.TerminalAssignment.AssignmentID
}
