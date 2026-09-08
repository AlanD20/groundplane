package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: success, failure after start, and running Abort are all
// non-retryable and may terminalize only with exact cleanup and source release.
func TestManualScriptTerminalReleasesSources(t *testing.T) {
	for _, status := range []TaskStatus{TaskStatusCompleted, TaskStatusFailed, TaskStatusAborted} {
		t.Run(string(status), func(t *testing.T) {
			store, scripts, tasks, assignment, execution := claimedManualScriptFixture(t)
			manualScriptCleanupCheckpoints(t, scripts, assignment, execution, status)
			result := manualScriptTerminalResult(assignment, status)
			terminalAt := assignment.Task.Record.CreatedAt.Add(20 * time.Second)
			terminal, err := tasks.AcknowledgeTask(context.Background(), assignment.Assignment.Record.AgentID, 1,
				assignment.Task.Record.ID, assignment.Assignment.Record.AssignmentID, status, result, terminalAt)
			if err != nil || terminal.Record.Status != status {
				t.Fatalf("manual terminal acknowledgement = %s, %v", terminal.Record.Status, err)
			}
			if store.valueAt(scriptSourceRootKey(execution.OperationID), store.revision) != nil {
				t.Fatal("terminal manual Task retained its immutable source root")
			}
			script, err := scripts.GetScript(context.Background(), execution.ScriptID)
			if err != nil || script.Record.ActiveReferences != 0 {
				t.Fatalf("terminal Script references = %d, %v", script.Record.ActiveReferences, err)
			}
			closed, err := scripts.GetScriptExecution(context.Background(), execution.ID)
			if err != nil || closed.Record.ActiveReference || closed.Record.State != ScriptExecutionCleanupProven {
				t.Fatalf("terminal execution retained active authority: %v", err)
			}
			revision := store.revision
			replay, err := tasks.AcknowledgeTask(
				context.Background(),
				assignment.Assignment.Record.AgentID,
				1,
				assignment.Task.Record.ID,
				assignment.Assignment.Record.AssignmentID,
				status,
				result,
				terminalAt.Add(time.Second),
			)
			if err != nil || replay.Revision != terminal.Revision || store.revision != revision {
				t.Fatalf("terminal replay wrote state: %d -> %d, %v", revision, store.revision, err)
			}
		})
	}
}

// Rationale: a terminal Agent result alone cannot substitute for durable Script
// cleanup; without it the Task and every source reference must remain active.
func TestManualScriptTerminalRejectsMissingCleanup(t *testing.T) {
	store, _, tasks, assignment, execution := claimedManualScriptFixture(t)
	revision := store.revision
	_, err := tasks.AcknowledgeTask(
		context.Background(),
		assignment.Assignment.Record.AgentID,
		1,
		assignment.Task.Record.ID,
		assignment.Assignment.Record.AssignmentID,
		TaskStatusCompleted,
		manualScriptTerminalResult(
			assignment,
			TaskStatusCompleted,
		),
		assignment.Task.Record.CreatedAt.Add(20*time.Second),
	)
	if err == nil || store.revision != revision ||
		store.valueAt(scriptSourceRootKey(execution.OperationID), store.revision) == nil {
		t.Fatalf("terminal report without cleanup was accepted: %v", err)
	}
}

func claimedManualScriptFixture(
	t *testing.T,
) (*memoryHierarchyStore, *ScriptRepository, *TaskRepository, TaskAssignment, ScriptExecutionRecord) {
	t.Helper()
	store, sources, execution, task, marker := manualScriptLifecycleFixture(t)
	scripts := &ScriptRepository{store: store}
	result, err := scripts.PublishExecutionWithTask(context.Background(), sources, execution, task, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("publish manual execution = %#v, %v", result, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	assignment, found, err := tasks.ClaimNextTask(context.Background(), ids.NewAt(ids.KindAgent, task.CreatedAt, 99), 1,
		task.CreatedAt.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("claim manual execution = %t, %v", found, err)
	}
	return store, scripts, tasks, assignment, execution
}

func manualScriptTerminalResult(assignment TaskAssignment, status TaskStatus) TaskResultRecord {
	result := TaskResultRecord{Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone,
		ExecutionEpoch: assignment.Assignment.Record.ExecutionEpoch}
	if status == TaskStatusFailed {
		result.ExitCode, result.FailedStepID, result.Diagnostic = 7, assignment.Task.Record.Steps[0].ID, TaskResultDiagnosticComposeFailed
	}
	return result
}

func manualScriptCleanupCheckpoints(
	t *testing.T,
	scripts *ScriptRepository,
	assignment TaskAssignment,
	execution ScriptExecutionRecord,
	status TaskStatus,
) {
	t.Helper()
	exitCode := int32(0)
	if status == TaskStatusFailed {
		exitCode = 7
	}
	outcome := &ScriptOutcomeEvidence{Reason: ScriptOutcomeNormalExit, ExitCode: &exitCode,
		ObservedAt: assignment.Task.Record.CreatedAt.Add(5 * time.Second)}
	if status == TaskStatusAborted {
		outcome.Reason, outcome.ExitCode = ScriptOutcomeAbort, nil
	}
	checkpoints := []struct {
		state    ScriptExecutionState
		evidence ScriptCheckpointEvidence
	}{
		{
			ScriptExecutionStartAuthorized,
			ScriptCheckpointEvidence{
				Kind:            ScriptCheckpointEvidenceStartAuthorized,
				StartAuthorized: &ScriptStartAuthorizedEvidence{},
			},
		},
		{
			ScriptExecutionBodyPrepared,
			ScriptCheckpointEvidence{
				Kind: ScriptCheckpointEvidenceBodyPrepared,
				BodyPrepared: &ScriptBodyPreparedEvidence{
					BodySHA256: execution.BodySHA256, UID: 65534, GID: 65534, Device: 10, Inode: 20, Leaf: "body",
				},
			},
		},
		{
			ScriptExecutionContainerCreated,
			ScriptCheckpointEvidence{
				Kind: ScriptCheckpointEvidenceContainerCreated,
				ContainerCreated: &ScriptContainerCreatedEvidence{
					ContainerID: strings.Repeat("a", 64), OwnershipLabelsSHA256: strings.Repeat("b", 64),
				},
			},
		},
		{
			ScriptExecutionOutcomeRecorded,
			ScriptCheckpointEvidence{Kind: ScriptCheckpointEvidenceOutcome, Outcome: outcome},
		},
		{
			ScriptExecutionCleanupProven,
			ScriptCheckpointEvidence{Kind: ScriptCheckpointEvidenceCleanup, Cleanup: &ScriptCleanupEvidence{
				ContainerID: strings.Repeat("a", 64), BodyDevice: 10, BodyInode: 20, BodyLeaf: "body",
				ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true,
			}},
		},
	}
	previous := ScriptExecutionNotStarted
	for index, checkpoint := range checkpoints {
		input := scriptCheckpointTestInput(
			execution,
			assignment.Task.Record.CreatedAt.Add(time.Duration(index+2)*time.Second),
		)
		input.AgentID, input.AssignmentID = assignment.Assignment.Record.AgentID, assignment.Assignment.Record.AssignmentID
		input.ExpectedState, input.State, input.Evidence = previous, checkpoint.state, checkpoint.evidence
		input.PayloadSHA256 = scriptSourceReferenceDigest(string(checkpoint.state))
		if _, err := scripts.CheckpointScriptExecution(context.Background(), input); err != nil {
			t.Fatalf("checkpoint %s: %v", checkpoint.state, err)
		}
		previous = checkpoint.state
	}
}
