package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestGetTaskAssignmentResolvesTheTaskIndexedExecutionClaim(t *testing.T) {
	// Rationale: operator abort must address the exact durable Agent generation
	// without scanning sessions or trusting a process-local assignment cache.
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
	resolved, err := repository.GetTaskAssignment(ctx, task.ID)
	if err != nil || resolved.Assignment.Record != claim.Assignment.Record ||
		resolved.Task.Record.ID != task.ID || resolved.Task.Record.Status != TaskStatusRunning {
		t.Fatalf("GetTaskAssignment() = %#v, %v", resolved, err)
	}

	pending := validTaskRecord(now.Add(time.Minute))
	createLifecycleTask(t, repository, pending)
	if _, err := repository.GetTaskAssignment(ctx, pending.ID); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("GetTaskAssignment(pending) error = %v", err)
	}
}
