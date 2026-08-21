package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestTaskRepositoryExpiresOnlyOverdueAssignments(t *testing.T) {
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task := validTaskRecord(taskJournalTime())
	task.TimeoutSeconds = 30
	createLifecycleTask(t, repository, task)
	agentID := ids.NewAt(ids.KindAgent, task.CreatedAt, 118)
	assignedAt := task.CreatedAt.Add(time.Second)
	assignment, found, err := repository.ClaimNextTask(ctx, agentID, 4, assignedAt)
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() = %#v, %t, %v", assignment, found, err)
	}

	count, err := repository.ExpireTimedOutTasks(ctx, assignment.Assignment.Record.Deadline.Add(-time.Nanosecond))
	if err != nil || count != 0 {
		t.Fatalf("ExpireTimedOutTasks(before) = %d, %v", count, err)
	}
	count, err = repository.ExpireTimedOutTasks(ctx, assignment.Assignment.Record.Deadline)
	if err != nil || count != 1 {
		t.Fatalf("ExpireTimedOutTasks(at deadline) = %d, %v", count, err)
	}
	terminal, err := repository.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if terminal.Record.Status != TaskStatusTimedOut || terminal.Record.Result == nil ||
		!terminal.Record.Result.ReconciliationRequired {
		t.Fatalf("timed out Task = %#v", terminal.Record)
	}
	assertTaskLifecycleValue(t, store, taskAssignmentKey(agentID, task.ID), false)
	assertTaskLifecycleValue(t, store, taskAssignmentIndexKey(task.ID), false)
	assertTaskLifecycleValue(t, store, taskTimeoutIndexKey(task.ID, assignment.Assignment.Record.Deadline), false)
}
