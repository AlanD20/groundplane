package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: accepting newer desired input cannot invalidate the owned old
// execution's terminal evidence or let Retry reselect that obsolete input.
func TestBlueprintTerminalAcknowledgesExecutionAfterNewDesiredPublication(t *testing.T) {
	for _, status := range []TaskStatus{TaskStatusCompleted, TaskStatusTimedOut} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			published, tasks, claim, agentID := claimBlueprintTerminalDesiredFixture(t)
			successor := publishBlueprintTerminalSuccessor(t, published)
			head := published.store.valueAt(
				environmentBlueprintHeadKey(published.environmentID),
				published.store.revision,
			)
			original := published.store.valueAt(taskKey(claim.Task.Record.ID), published.store.revision)
			result := TaskResultRecord{Kind: TaskResultCompose, ExecutionEpoch: 1, Diagnostic: TaskResultDiagnosticNone}
			if status == TaskStatusTimedOut {
				result.Diagnostic = TaskResultDiagnosticTimeoutBeforeEffect
			}
			terminal, err := tasks.AcknowledgeTask(ctx, agentID, 1, claim.Task.Record.ID,
				claim.Assignment.Record.AssignmentID, status, result, published.task.CreatedAt.Add(time.Minute))
			if err != nil || terminal.Record.Status != status {
				t.Fatalf(
					"acknowledge original after successor publication: status=%s error=%v",
					terminal.Record.Status,
					err,
				)
			}
			afterHead := published.store.valueAt(head.Key, published.store.revision)
			if afterHead == nil || afterHead.ModRevision != head.ModRevision {
				t.Fatal("old completion changed latest desired head")
			}
			if writer := published.store.valueAt(taskMaterializationWriterKey(published.environmentID), published.store.revision); writer != nil {
				t.Fatal("acknowledged execution retained its writer")
			}
			applied := published.store.valueAt(
				environmentComposeProjectionKey(published.environmentID),
				published.store.revision,
			)
			if status == TaskStatusCompleted {
				if applied == nil {
					t.Fatal("successful old execution lost its applied result")
				}
				projection, decodeErr := decodeEnvironmentComposeProjection(applied.Value)
				if decodeErr != nil || projection.RevisionID != published.task.ID ||
					projection.RevisionID == successor.ID {
					t.Fatalf("old completion claimed wrong input applied: %v", decodeErr)
				}
			} else if applied != nil {
				t.Fatal("no-effect timeout promoted unapplied input")
			}
			beforeReplay := published.store.revision
			replay, err := tasks.AcknowledgeTask(ctx, agentID, 1, claim.Task.Record.ID,
				claim.Assignment.Record.AssignmentID, status, result, published.task.CreatedAt.Add(2*time.Minute))
			if err != nil || replay.Revision != terminal.Revision || published.store.revision != beforeReplay {
				t.Fatalf("old terminal replay was not read-only: %v", err)
			}
			for _, field := range []string{"epoch", "recovery digest"} {
				changed := result
				if field == "epoch" {
					changed.ExecutionEpoch++
				} else {
					changed.ReleaseRecoveryRecordSHA256 = strings.Repeat("a", 64)
				}
				_, replayErr := tasks.AcknowledgeTask(ctx, agentID, 1, claim.Task.Record.ID,
					claim.Assignment.Record.AssignmentID, status, changed, published.task.CreatedAt.Add(2*time.Minute))
				if !isKind(replayErr, errs.KindStateConflict) || published.store.revision != beforeReplay {
					t.Fatalf("terminal replay accepted changed %s: %v", field, replayErr)
				}
			}
			if original.ModRevision != claim.Task.Revision {
				t.Fatal("successor publication rewrote original Task history")
			}
			if status != TaskStatusCompleted {
				retry, cloneErr := cloneRetryTask(terminal.Record,
					ids.NewAt(ids.KindTask, published.task.CreatedAt, 19884), TaskActorOperator,
					published.task.CreatedAt.Add(3*time.Minute))
				if cloneErr != nil {
					t.Fatal(cloneErr)
				}
				change, retryErr := tasks.prepareBlueprintCandidateRetry(
					ctx,
					terminal.Record,
					retry,
					published.store.revision,
				)
				defer change.clear()
				if !isKind(retryErr, errs.KindStateConflict) || change.applies ||
					published.store.revision != beforeReplay {
					t.Fatalf("Retry revived obsolete intent: %v", retryErr)
				}
			}
		})
	}
}

func claimBlueprintTerminalDesiredFixture(
	t *testing.T,
) (environmentBlueprintAtomicPublication, *TaskRepository, TaskAssignment, string) {
	t.Helper()
	published, err := publishEnvironmentBlueprintAtomicShape(t, environmentBlueprintAtomicShape{
		name: "terminal desired authority", releases: 1, physicalSources: 1, realHookSources: true,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	seedBlueprintTerminalCandidateLedger(t, published)
	tasks, err := newTaskRepository(published.store)
	if err != nil {
		t.Fatal(err)
	}
	tasks.blueprintTerminalStore = published.store
	agentID := ids.NewAt(ids.KindAgent, published.task.CreatedAt, 19880)
	claim, found, err := tasks.ClaimNextTask(
		context.Background(),
		agentID,
		1,
		published.task.CreatedAt.Add(time.Second),
	)
	if err != nil || !found {
		t.Fatalf("claim original: found=%t error=%v", found, err)
	}
	return published, tasks, claim, agentID
}

func publishBlueprintTerminalSuccessor(t *testing.T, published environmentBlueprintAtomicPublication) TaskRecord {
	t.Helper()
	ctx := context.Background()
	hierarchy, err := newHierarchyRepository(published.store)
	if err != nil {
		t.Fatal(err)
	}
	project, err := hierarchy.GetProject(ctx, published.task.Owner.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	environment, err := hierarchy.GetEnvironment(ctx, published.environmentID)
	if err != nil {
		t.Fatal(err)
	}
	current, found, err := hierarchy.GetEnvironmentComposeProjection(ctx, published.environmentID)
	if err != nil || !found {
		t.Fatalf("current desired: found=%t error=%v", found, err)
	}
	task := environmentBlueprintTestTask(t, project.Record, environment.Record, 19881)
	task.RenderGeneration = published.task.RenderGeneration + 1
	projection := current.Record
	projection.RevisionID, projection.RenderGeneration = task.ID, uint64(task.RenderGeneration)
	projection.DesiredServices[0].Desired.Image = "example/api:2"
	result := publishEnvironmentBlueprintTestRevision(t, hierarchy, project, environment, current.Revision,
		environmentBlueprintTestRevision(published.environmentID, task, "services: {}\n"), projection,
		environmentBlueprintTestZoneChanges(t, hierarchy, projection),
		environmentBlueprintTestServiceChanges(t, hierarchy, projection),
		environmentBlueprintTestRouteChanges(t, hierarchy, projection), ComponentTaskPreparation{}, task,
		environmentBlueprintTestMarker(task, published.environmentID))
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("successor publication: outcome=%v conflict=%v error=%v", outcome, conflict, classifyErr)
	}
	return task
}
