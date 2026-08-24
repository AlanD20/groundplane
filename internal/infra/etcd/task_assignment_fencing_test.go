package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestAgentTaskAcknowledgementFencesExactAssignmentAndReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := taskJournalTime().Add(10 * time.Minute)
	repository, err := newTaskRepository(newMemoryTaskStore())
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task := validTaskRecord(now)
	createLifecycleTask(t, repository, task)
	agentID := ids.NewAt(ids.KindAgent, now, 81)
	claim, found, err := repository.ClaimNextTask(ctx, agentID, 9, now.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() = %#v, %t, %v", claim, found, err)
	}
	staleID := ids.NewAt(ids.KindAssignment, now, 82)
	if _, err := repository.AcknowledgeTask(
		ctx, agentID, 9, task.ID, staleID,
		TaskStatusCompleted, completedComposeTaskResult(), now.Add(2*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(stale) error = %v, want state conflict", err)
	}
	terminal, err := repository.AcknowledgeTask(
		ctx, agentID, 9, task.ID, claim.Assignment.Record.AssignmentID,
		TaskStatusCompleted, completedComposeTaskResult(), now.Add(2*time.Second),
	)
	if err != nil || terminal.Record.TerminalAssignment == nil ||
		terminal.Record.TerminalAssignment.AssignmentID != claim.Assignment.Record.AssignmentID {
		t.Fatalf("AcknowledgeTask() = %#v, %v", terminal, err)
	}
	if _, err := repository.AcknowledgeTask(
		ctx, agentID, 9, task.ID, staleID,
		TaskStatusCompleted, completedComposeTaskResult(), now.Add(3*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(stale replay) error = %v, want state conflict", err)
	}
	if _, err := repository.AcknowledgeTask(
		ctx, agentID, 9, task.ID, claim.Assignment.Record.AssignmentID,
		TaskStatusCompleted, completedComposeTaskResult(), now.Add(3*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask(replay) error = %v", err)
	}
}

func TestAgentTaskProgressFencesExactAssignmentAndGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := taskJournalTime().Add(20 * time.Minute)
	repository, err := newTaskRepository(newMemoryTaskStore())
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task := validTaskRecord(now)
	createLifecycleTask(t, repository, task)
	agentID := ids.NewAt(ids.KindAgent, now, 91)
	claim, found, err := repository.ClaimNextTask(ctx, agentID, 6, now.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() = %#v, %t, %v", claim, found, err)
	}
	input := taskEventInput(task.ID, 1, TaskEventStateRunning)
	input.Identity.AssignmentID = claim.Assignment.Record.AssignmentID
	input.Identity.AgentID = agentID
	input.Identity.AgentGeneration = 6
	stale := input
	stale.Identity.AssignmentID = ids.NewAt(ids.KindAssignment, now, 92)
	if _, err := repository.AppendTaskEvent(
		ctx,
		stale,
		now.Add(2*time.Second),
	); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("AppendTaskEvent(stale assignment) error = %v, want state conflict", err)
	}
	stale = input
	stale.Identity.AgentGeneration++
	if _, err := repository.AppendTaskEvent(
		ctx,
		stale,
		now.Add(2*time.Second),
	); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("AppendTaskEvent(stale generation) error = %v, want state conflict", err)
	}
	if _, err := repository.AppendTaskEvent(ctx, input, now.Add(2*time.Second)); err != nil {
		t.Fatalf("AppendTaskEvent() error = %v", err)
	}
	missing := input
	missing.Identity.AssignmentID = ""
	missing.Identity.Ordinal++
	if _, err := repository.AppendTaskEvent(
		ctx,
		missing,
		now.Add(3*time.Second),
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("AppendTaskEvent(missing assignment) error = %v, want validation failed", err)
	}
}
