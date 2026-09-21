package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestGetTaskAssignmentResolvesTheTaskIndexedExecutionClaim(t *testing.T) {
	// QA: TASK-03, TASK-10 (L1 in-memory repository; no Controller abort dispatch,
	// real etcd fixed-revision race, Agent reconnect, or executor cancellation).
	// Rationale: operator abort must address the exact durable Agent generation
	// and Task-indexed claim even when that Agent owns other work, without
	// scanning sessions or trusting a process-local assignment cache.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(72 * time.Hour)
	task := validTaskRecord(now)
	createLifecycleTask(t, repository, task)
	agentID := ids.NewAt(ids.KindAgent, now, 901)
	claim, found, err := repository.ClaimNextTask(ctx, agentID, 7, now.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() = %#v, %t, %v", claim, found, err)
	}
	other := validTaskRecord(now.Add(time.Minute))
	createLifecycleTask(t, repository, other)
	otherClaim, found, err := repository.ClaimNextTask(ctx, agentID, 7, now.Add(2*time.Minute))
	if err != nil || !found || otherClaim.Task.Record.ID != other.ID ||
		otherClaim.Assignment.Record.AssignmentID == claim.Assignment.Record.AssignmentID {
		t.Fatalf("ClaimNextTask(other) = %#v, %t, %v", otherClaim, found, err)
	}
	resolved, err := repository.GetTaskAssignment(ctx, task.ID)
	if err != nil || resolved.Assignment.Record != claim.Assignment.Record ||
		resolved.Assignment.Revision != claim.Assignment.Revision || resolved.Task.Revision != claim.Task.Revision ||
		resolved.Assignment.ReadRevision != resolved.Task.ReadRevision ||
		resolved.Task.Record.ID != task.ID || resolved.Task.Record.Status != testtaskjournal.TaskStatusRunning {
		t.Fatalf("GetTaskAssignment() = %#v, %v", resolved, err)
	}

	pending := validTaskRecord(now.Add(3 * time.Minute))
	createLifecycleTask(t, repository, pending)
	if _, err := repository.GetTaskAssignment(ctx, pending.ID); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("GetTaskAssignment(pending) error = %v", err)
	}
}
