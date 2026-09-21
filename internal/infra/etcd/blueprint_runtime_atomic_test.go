package etcd

import (
	"context"
	"testing"
	"time"

	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// QA: BP-04, SVC-15, TASK-03; in-memory terminal persistence, not host recovery.
// Rationale: recovery authority must commit with successful execution, survive a
// lost response without a second write, and never promote a no-effect failure.
func TestBlueprintRuntimeSharesTerminalCommitAndReplay(t *testing.T) {
	for _, outcome := range []string{"success", "lost response", "no effect"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			published, tasks, claim, agentID := claimBlueprintTerminalDesiredFixture(t)
			value := published.store.valueAt(
				testreleases.ReleasePublicationKey(published.releasePublicationID),
				published.store.revision,
			)
			marker, err := testreleases.DecodeReleaseRecord[testreleases.ReleasePublicationMarker](
				value.Value,
				"release-publication",
			)
			if err != nil || len(marker.BlueprintRuntimes) != 1 {
				t.Fatalf("fixture runtime preparation: %v", err)
			}
			key := serviceruntimerecord.Key(marker.BlueprintRuntimes[0].ServiceID)
			status := testtaskjournal.TaskStatusCompleted
			result := testtaskjournal.TaskResultRecord{
				Kind:           testtaskjournal.TaskResultCompose,
				ExecutionEpoch: 1,
				Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
			}
			if outcome == "no effect" {
				status, result.Diagnostic = testtaskjournal.TaskStatusTimedOut, testtaskjournal.TaskResultDiagnosticTimeoutBeforeEffect
			}
			if outcome == "lost response" {
				tasks.blueprintTerminalStore = &blueprintTerminalFaultStore{
					memoryHierarchyStore: published.store, t: t, fault: "lost response",
				}
			}
			_, err = tasks.AcknowledgeTask(ctx, agentID, 1, claim.Task.Record.ID,
				claim.Assignment.Record.AssignmentID, status, result, published.task.CreatedAt.Add(time.Minute))
			if outcome == "lost response" {
				if !isKind(err, errs.KindInternal) {
					t.Fatalf("expected lost response: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			terminal, err := tasks.GetTask(ctx, claim.Task.Record.ID)
			if err != nil || terminal.Record.Status != status {
				t.Fatalf("terminal outcome: %v", err)
			}
			receipt := published.store.valueAt(key, published.store.revision)
			if outcome == "no effect" {
				if receipt != nil {
					t.Fatal("no-effect failure promoted candidate recovery authority")
				}
			} else {
				if receipt == nil || receipt.ModRevision != terminal.Revision {
					t.Fatal("runtime and successful Task did not share the atomic commit")
				}
				record, err := testreleases.DecodeReleaseRecord[serviceruntimerecord.Record](receipt.Value, "service-acknowledged-runtime")
				if err != nil || serviceruntimerecord.Validate(record) != nil || record.Source.TaskID != terminal.Record.ID {
					t.Fatalf("invalid acknowledged runtime: %v", err)
				}
			}
			restarted, err := newTaskRepository(published.store)
			if err != nil {
				t.Fatal(err)
			}
			restarted.blueprintTerminalStore = published.store
			before := published.store.revision
			replay, err := restarted.AcknowledgeTask(ctx, agentID, 1, claim.Task.Record.ID,
				claim.Assignment.Record.AssignmentID, status, result, published.task.CreatedAt.Add(2*time.Minute))
			if err != nil || replay.Revision != terminal.Revision || published.store.revision != before {
				t.Fatalf("replay rewrote terminal or runtime authority: %v", err)
			}
		})
	}
}
