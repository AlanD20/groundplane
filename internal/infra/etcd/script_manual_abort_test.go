package etcd

import (
	"context"
	"testing"
	"time"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: an assigned Task can be aborted before start authorization; the
// Controller must fence that absence and release sources without running it.
func TestManualScriptAssignedAbortBeforeStartReleasesSources(t *testing.T) {
	ctx := context.Background()
	store, scripts, tasks, assignment, execution := claimedManualScriptFixture(t)
	terminal, err := tasks.AcknowledgeTask(
		ctx,
		assignment.Assignment.Record.AgentID,
		1,
		assignment.Task.Record.ID,
		assignment.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusAborted,
		manualScriptTerminalResult(assignment, testtaskjournal.TaskStatusAborted),
		assignment.Task.Record.CreatedAt.Add(20*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusAborted {
		t.Fatalf("assigned before-start Abort = %v", err)
	}
	if store.valueAt(testscriptsourceevidence.ScriptSourceRootKey(execution.OperationID), store.revision) != nil {
		t.Fatal("assigned Abort retained its source root")
	}
	closed, err := scripts.GetScriptExecution(ctx, execution.ID)
	if err != nil || closed.Record.State != testscriptexecutions.ScriptExecutionCleanupProven || closed.Record.ActiveReference ||
		closed.Record.StartAuthorized ||
		closed.Record.Outcome == nil ||
		closed.Record.Outcome.Reason != testscriptexecutions.ScriptOutcomeAbortBeforeStart {
		t.Fatalf("assigned Abort did not preserve fenced absence proof: %v", err)
	}
}

// Rationale: if start authorization wins the CAS race, Controller-owned
// absence is no longer provable and Abort must wait for real execution cleanup.
func TestManualScriptAssignedAbortCannotRacePastStart(t *testing.T) {
	ctx := context.Background()
	store, scripts, _, assignment, execution := claimedManualScriptFixture(t)
	input := scriptCheckpointTestInput(execution, assignment.Task.Record.CreatedAt.Add(2*time.Second))
	input.AgentID, input.AssignmentID = assignment.Assignment.Record.AgentID, assignment.Assignment.Record.AssignmentID
	input.ExpectedState, input.State = testscriptexecutions.ScriptExecutionNotStarted, testscriptexecutions.ScriptExecutionStartAuthorized
	input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
		Kind:            testscriptexecutions.ScriptCheckpointEvidenceStartAuthorized,
		StartAuthorized: &testscriptexecutions.ScriptStartAuthorizedEvidence{},
	}
	input.PayloadSHA256 = scriptSourceReferenceDigest(string(testscriptexecutions.ScriptExecutionStartAuthorized))
	racing := &manualAssignedAbortRaceStore{memoryHierarchyStore: store, scripts: scripts, input: input,
		rootKey: testscriptsourceevidence.ScriptSourceRootKey(execution.OperationID)}
	tasks, err := newTaskRepository(racing)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tasks.AcknowledgeTask(
		ctx,
		assignment.Assignment.Record.AgentID,
		1,
		assignment.Task.Record.ID,
		assignment.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusAborted,
		manualScriptTerminalResult(assignment, testtaskjournal.TaskStatusAborted),
		assignment.Task.Record.CreatedAt.Add(20*time.Second),
	)
	if !racing.raced || !isKind(err, errs.KindStateConflict) {
		t.Fatalf("assigned Abort/start race = raced %t, %v", racing.raced, err)
	}
	current, err := scripts.GetScriptExecution(ctx, execution.ID)
	if err != nil || current.Record.State != testscriptexecutions.ScriptExecutionStartAuthorized ||
		!current.Record.ActiveReference {
		t.Fatalf("Abort overwrote winning start authorization: %v", err)
	}
	task, err := tasks.GetTask(ctx, assignment.Task.Record.ID)
	if err != nil || task.Record.Status != testtaskjournal.TaskStatusRunning {
		t.Fatalf("Abort terminalized without real cleanup: %v", err)
	}
}

// Rationale: closing an assigned but unstarted execution must survive a lost
// release request without redispatching it or replacing the original report.
func TestManualScriptAssignedAbortResumesAfterInterruption(t *testing.T) {
	ctx := context.Background()
	store, _, _, assignment, execution := claimedManualScriptFixture(t)
	interrupted, err := newTaskRepository(&scriptSourceReferenceFailureStore{memoryHierarchyStore: store, failAt: 2})
	if err != nil {
		t.Fatal(err)
	}
	at := assignment.Task.Record.CreatedAt.Add(20 * time.Second)
	_, err = interrupted.AcknowledgeTask(
		ctx,
		assignment.Assignment.Record.AgentID,
		1,
		assignment.Task.Record.ID,
		assignment.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusAborted,
		manualScriptTerminalResult(assignment, testtaskjournal.TaskStatusAborted),
		at,
	)
	if !isKind(err, errs.KindInternal) {
		t.Fatalf("assigned Abort interruption = %v", err)
	}
	restarted, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := restarted.ReconnectAgentAssignment(ctx, assignment)
	if err != nil || resumed.Task.Record.Status != testtaskjournal.TaskStatusAborted ||
		resumed.Task.Record.FinishedAt == nil ||
		!resumed.Task.Record.FinishedAt.Equal(at) ||
		resumed.Assignment.Revision != assignment.Assignment.Revision {
		t.Fatalf("assigned Abort restart changed original authority: %v", err)
	}
	if store.valueAt(testscriptsourceevidence.ScriptSourceRootKey(execution.OperationID), store.revision) != nil {
		t.Fatal("assigned Abort restart retained its source root")
	}
}

type manualAssignedAbortRaceStore struct {
	*memoryHierarchyStore
	scripts *ScriptRepository
	input   testscriptexecutions.ScriptCheckpointInput
	rootKey string
	raced   bool
}

func (store *manualAssignedAbortRaceStore) Transact(
	ctx context.Context, conditions []testkeyvalue.Condition, mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	for _, mutation := range mutations {
		if !store.raced && mutation.Type == testkeyvalue.MutationPut && mutation.Key == store.rootKey {
			store.raced = true
			if _, err := store.scripts.CheckpointScriptExecution(ctx, store.input); err != nil {
				return testkeyvalue.TransactionResult{}, err
			}
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
