package etcd

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestAgentTaskAcknowledgementFencesExactAssignmentAndReplay(t *testing.T) {
	// QA: TASK-10 (L1 in-memory repository; no real etcd CAS race, reconnect, or Agent effects).
	// Rationale: a stale assignment must not terminalize another executor's Task,
	// while exact terminal replay must return the original immutable outcome.
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
	active, err := repository.GetTaskAssignment(ctx, task.ID)
	if err != nil || active.Assignment.Record != claim.Assignment.Record ||
		active.Task.Record.Status != TaskStatusRunning || active.Task.Record.Result != nil {
		t.Fatalf("GetTaskAssignment(after stale acknowledgement) = %#v, %v", active, err)
	}
	terminalAt := now.Add(2 * time.Second)
	terminal, err := repository.AcknowledgeTask(
		ctx, agentID, 9, task.ID, claim.Assignment.Record.AssignmentID,
		TaskStatusCompleted, completedComposeTaskResult(), terminalAt,
	)
	wantTerminalAssignment := TaskTerminalAssignmentRecord{
		AssignmentID:    claim.Assignment.Record.AssignmentID,
		AgentID:         agentID,
		AgentGeneration: 9,
	}
	if err != nil || terminal.Record.Status != TaskStatusCompleted || terminal.Record.Result == nil ||
		terminal.Record.Result.Kind != TaskResultCompose || terminal.Record.Result.ExitCode != 0 ||
		terminal.Record.Result.FailedStepID != "" || terminal.Record.Result.Diagnostic != TaskResultDiagnosticNone ||
		terminal.Record.Result.ReconciliationRequired || len(terminal.Record.Result.Projects) != 0 ||
		len(terminal.Record.Result.ProxyEvidence) != 0 || len(terminal.Record.Result.RecreateEvidence) != 0 ||
		terminal.Record.Result.CandidateAbsenceEvidence != nil ||
		terminal.Record.Result.DNSResolverCandidateObservation != nil ||
		terminal.Record.Result.DNSResolverRollbackObservation != nil || terminal.Record.Result.ExecutionEpoch != 0 ||
		terminal.Record.Result.ReleaseRecoveryRecordSHA256 != "" ||
		terminal.Record.TerminalAssignment == nil ||
		*terminal.Record.TerminalAssignment != wantTerminalAssignment || terminal.Record.FinishedAt == nil ||
		!terminal.Record.FinishedAt.Equal(terminalAt) {
		t.Fatalf("AcknowledgeTask() = %#v, %v", terminal, err)
	}
	if _, err := repository.GetTaskAssignment(ctx, task.ID); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("GetTaskAssignment(terminal) error = %v, want state conflict", err)
	}
	if _, err := repository.AcknowledgeTask(
		ctx, agentID, 9, task.ID, staleID,
		TaskStatusCompleted, completedComposeTaskResult(), now.Add(3*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("AcknowledgeTask(stale replay) error = %v, want state conflict", err)
	}
	replayed, err := repository.AcknowledgeTask(
		ctx, agentID, 9, task.ID, claim.Assignment.Record.AssignmentID,
		TaskStatusCompleted, completedComposeTaskResult(), now.Add(3*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(replay) error = %v", err)
	}
	terminalValue, terminalEncodeErr := encodeTaskRecord(terminal.Record)
	replayedValue, replayedEncodeErr := encodeTaskRecord(replayed.Record)
	if terminalEncodeErr != nil || replayedEncodeErr != nil || replayed.Revision != terminal.Revision ||
		!bytes.Equal(replayedValue, terminalValue) {
		t.Fatalf("AcknowledgeTask(replay) = %#v, %v, want original %#v", replayed, err, terminal)
	}
}

func TestAgentTaskProgressFencesExactAssignmentAndGeneration(t *testing.T) {
	// QA: TASK-07, TASK-10 (L1 in-memory repository; no protobuf transport, real etcd, or reconnect effects).
	// Rationale: stale or incomplete Agent authority must not append durable
	// progress, consume a sequence, or alter the running Task's summary.
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
	before, err := repository.ListTaskEvents(ctx, task.ID, 0)
	if err != nil || before.Task.EventCount != 0 || before.Task.NextEventSequence != 1 || len(before.Events) != 0 {
		t.Fatalf("ListTaskEvents(after stale progress) = %#v, %v", before, err)
	}
	appended, err := repository.AppendTaskEvent(ctx, input, now.Add(2*time.Second))
	if err != nil || appended.Sequence != 1 || appended.Duplicate {
		t.Fatalf("AppendTaskEvent() = %#v, %v", appended, err)
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
	after, err := repository.ListTaskEvents(ctx, task.ID, 0)
	if err != nil || after.Task.EventCount != 1 || after.Task.NextEventSequence != 2 || len(after.Events) != 1 ||
		after.Events[0].Identity != input.Identity || after.Events[0].State != input.State ||
		string(after.Events[0].Payload) != string(input.Payload) {
		t.Fatalf("ListTaskEvents(after valid and missing progress) = %#v, %v", after, err)
	}
}
