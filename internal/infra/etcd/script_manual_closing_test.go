package etcd

import (
	"context"
	"testing"
	"time"

	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: interruption after source closure must preserve the original
// report and reject new effects, then finish solely from Controller authority.
func TestManualScriptClosingResumesOriginalReport(t *testing.T) {
	for _, failAt := range []int{2, 3} {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			ctx := context.Background()
			store, scripts, tasks, assignment, execution := claimedManualScriptFixture(t)
			manualScriptCleanupCheckpoints(t, scripts, assignment, execution, testtaskjournal.TaskStatusCompleted)
			failing := &scriptSourceReferenceFailureStore{memoryHierarchyStore: store, failAt: failAt}
			interrupted, err := newTaskRepository(failing)
			if err != nil {
				t.Fatal(err)
			}
			result := manualScriptTerminalResult(assignment, testtaskjournal.TaskStatusCompleted)
			terminalAt := assignment.Task.Record.CreatedAt.Add(20 * time.Second)
			_, err = interrupted.AcknowledgeTask(
				ctx,
				assignment.Assignment.Record.AgentID,
				1,
				assignment.Task.Record.ID,
				assignment.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result,
				terminalAt,
			)
			if !isKind(err, errs.KindInternal) {
				t.Fatalf("injected terminal interruption = %v", err)
			}
			current, err := tasks.GetTaskAssignment(ctx, assignment.Task.Record.ID)
			if err != nil || current.Task.Revision != assignment.Task.Revision ||
				current.Assignment.Revision != assignment.Assignment.Revision {
				t.Fatalf("source closure changed Task or assignment authority: %v", err)
			}
			report, reportValue, err := tasks.readScriptClosingReport(ctx, current)
			if err != nil || reportValue == nil || !report.matches(testtaskjournal.TaskStatusCompleted, result) ||
				!report.ObservedAt.Equal(terminalAt) {
				t.Fatalf("original report was not durable: %v", err)
			}
			revision := store.revision
			_, err = tasks.AcknowledgeTask(
				ctx,
				assignment.Assignment.Record.AgentID,
				1,
				assignment.Task.Record.ID,
				assignment.Assignment.Record.AssignmentID,
				testtaskjournal.TaskStatusFailed,
				manualScriptTerminalResult(assignment, testtaskjournal.TaskStatusFailed),
				terminalAt.Add(time.Second),
			)
			if !isKind(err, errs.KindStateConflict) || store.revision != revision {
				t.Fatalf("conflicting report changed closing authority: %v", err)
			}
			_, err = tasks.AppendTaskEvent(
				ctx,
				testtaskjournal.TaskEventInput{Identity: testtaskjournal.TaskEventIdentity{
					AssignmentID: assignment.Assignment.Record.AssignmentID, AgentID: assignment.Assignment.Record.AgentID,
					AgentGeneration: 1, TaskID: assignment.Task.Record.ID, StepID: assignment.Task.Record.Steps[0].ID,
					Attempt: assignment.Assignment.Record.ExecutionEpoch, Ordinal: 1,
				}, State: testtaskjournal.TaskEventStateRunning, Payload: []byte(`{"message":"late event"}`)},
				terminalAt.Add(time.Second),
			)
			if !isKind(err, errs.KindStateConflict) || store.revision != revision {
				t.Fatalf("late event changed closing authority: %v", err)
			}
			restarted, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			resumed, err := restarted.ReconnectAgentAssignment(ctx, assignment)
			if err != nil || resumed.Task.Record.Status != testtaskjournal.TaskStatusCompleted ||
				resumed.Task.Record.FinishedAt == nil ||
				!resumed.Task.Record.FinishedAt.Equal(terminalAt) ||
				resumed.Assignment.Revision != assignment.Assignment.Revision {
				t.Fatalf("Controller-only terminal resumption lost original authority: %v", err)
			}
			for _, key := range []string{testscriptsourceevidence.ScriptSourceRootKey(execution.OperationID), manualScriptClosingReportKey(assignment.Task.Record.ID)} {
				if store.valueAt(key, store.revision) != nil {
					t.Fatalf("terminal resumption retained continuation authority: %s", key)
				}
			}
			script, err := scripts.GetScript(ctx, execution.ScriptID)
			if err != nil || script.Record.ActiveReferences != 0 {
				t.Fatalf("resumption leaked or doubled source release: %v", err)
			}
		})
	}
}
