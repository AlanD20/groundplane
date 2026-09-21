package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: accepting newer desired input relaxes no execution ownership,
// captured applied-state, immutable source, or restoration-proof requirement.
func TestBlueprintTerminalAfterNewDesiredRetainsExecutionGuards(t *testing.T) {
	for _, fault := range []string{
		"missing writer", "foreign writer", "changed applied", "changed manifest", "invalid epoch",
		"wrong agent", "wrong execution epoch", "missing execution epoch", "foreign recovery digest",
		"unproven recovery", "timeout after step",
	} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			published, tasks, claim, agentID := claimBlueprintTerminalDesiredFixture(t)
			publishBlueprintTerminalSuccessor(t, published)
			status := testtaskjournal.TaskStatusCompleted
			result := testtaskjournal.TaskResultRecord{
				Kind:           testtaskjournal.TaskResultCompose,
				ExecutionEpoch: 1,
				Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
			}
			var mutations []testkeyvalue.Mutation
			switch fault {
			case "missing writer":
				mutations = []testkeyvalue.Mutation{
					{
						Type: testkeyvalue.MutationDelete,
						Key:  testtaskjournal.TaskMaterializationWriterKey(published.environmentID),
					},
				}
			case "foreign writer":
				key := testtaskjournal.TaskMaterializationWriterKey(published.environmentID)
				writer, err := decodeTaskMaterializationWriter(
					published.store.valueAt(key, published.store.revision).Value,
				)
				if err != nil {
					t.Fatal(err)
				}
				writer.TaskID = ids.NewAt(ids.KindTask, published.task.CreatedAt, 19885)
				value, err := encodeTaskMaterializationWriter(writer)
				if err != nil {
					t.Fatal(err)
				}
				mutations = []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: value}}
			case "changed applied":
				hierarchy, _ := newHierarchyRepository(published.store)
				projection, found, err := hierarchy.GetEnvironmentComposeProjectionRevision(
					ctx,
					published.environmentID,
					published.task.ID,
				)
				if err != nil || !found {
					t.Fatalf("read original projection: %v", err)
				}
				value, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection.Record)
				if err != nil {
					t.Fatal(err)
				}
				mutations = []testkeyvalue.Mutation{
					{
						Type: testkeyvalue.MutationPut,
						Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(
							published.environmentID,
						),
						Value: value,
					},
				}
			case "changed manifest":
				mutations = []testkeyvalue.Mutation{
					{
						Type:  testkeyvalue.MutationPut,
						Key:   testreleases.ReleaseManifestStagingKey(published.releasePublicationID),
						Value: []byte("invalid"),
					},
				}
			case "invalid epoch":
				mutations = []testkeyvalue.Mutation{
					{
						Type:  testkeyvalue.MutationPut,
						Key:   testhierarchy.EnvironmentMutationEpochKey(published.environmentID),
						Value: []byte("invalid"),
					},
				}
			case "wrong agent":
				agentID = ids.NewAt(ids.KindAgent, published.task.CreatedAt, 19886)
			case "wrong execution epoch":
				result.ExecutionEpoch++
			case "missing execution epoch":
				result.ExecutionEpoch = 0
			case "foreign recovery digest":
				result.ReleaseRecoveryRecordSHA256 = strings.Repeat("a", 64)
			case "unproven recovery":
				status, result.ReconciliationRequired = testtaskjournal.TaskStatusFailed, true
				result.FailedStepID = published.task.Steps[0].ID
			case "timeout after step":
				status, result.Diagnostic = testtaskjournal.TaskStatusTimedOut, testtaskjournal.TaskResultDiagnosticTimeoutBeforeEffect
				result.FailedStepID = published.task.Steps[0].ID
			}
			if len(mutations) != 0 {
				changed, err := published.store.Transact(ctx, nil, mutations)
				if err != nil || !changed.Succeeded {
					t.Fatalf("inject %s: %v", fault, err)
				}
			}
			before := published.store.revision
			acknowledged, err := tasks.AcknowledgeTask(ctx, agentID, 1, claim.Task.Record.ID,
				claim.Assignment.Record.AssignmentID, status, result, published.task.CreatedAt.Add(time.Minute))
			if fault == "unproven recovery" {
				current, readErr := tasks.GetTaskAssignment(ctx, claim.Task.Record.ID)
				if err != nil || readErr != nil || acknowledged.Record.Status != testtaskjournal.TaskStatusRunning ||
					acknowledged.Record.Result != nil || current.Assignment.Record.ExecutionMode != testtaskassignments.TaskExecutionModeRecoveryOnly ||
					current.Assignment.Record.ExecutionEpoch != claim.Assignment.Record.ExecutionEpoch+1 {
					t.Fatalf("unproven failure did not retain recovery ownership: terminal=%v read=%v", err, readErr)
				}
				if published.store.valueAt(
					testtaskjournal.TaskMaterializationWriterKey(published.environmentID),
					published.store.revision,
				) == nil ||
					published.store.valueAt(
						testenvironmentprojection.EnvironmentComposeProjectionStorageKey(published.environmentID),
						published.store.revision,
					) != nil {
					t.Fatal("unproven recovery released its writer or promoted applied state")
				}
				return
			}
			if err == nil || published.store.revision != before {
				t.Fatalf("invalid terminal evidence mutated state: %v", err)
			}
			storedTask := published.store.valueAt(
				testtaskjournal.TaskStorageKey(claim.Task.Record.ID),
				published.store.revision,
			)
			storedAssignment := published.store.valueAt(
				testtaskjournal.TaskAssignmentIndexKey(claim.Task.Record.ID),
				published.store.revision,
			)
			if storedTask == nil || storedAssignment == nil || storedTask.ModRevision != claim.Task.Revision ||
				storedAssignment.ModRevision != claim.Assignment.Revision {
				t.Fatal("invalid terminal evidence changed ownership")
			}
		})
	}
}

// Rationale: using the current desired epoch must still compare it at the actual
// atomic commit, and uncertain commit replay must never repeat terminal effects.
func TestBlueprintTerminalAfterNewDesiredKeepsAtomicCommitAndReplay(t *testing.T) {
	for _, fault := range []string{"compare-loss", "lost-response"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			published, tasks, claim, agentID := claimBlueprintTerminalDesiredFixture(t)
			publishBlueprintTerminalSuccessor(t, published)
			faults := &blueprintTerminalFaultStore{memoryHierarchyStore: published.store, t: t,
				epochKey: testhierarchy.EnvironmentMutationEpochKey(published.environmentID), fault: fault}
			tasks.blueprintTerminalStore = faults
			result := testtaskjournal.TaskResultRecord{
				Kind:           testtaskjournal.TaskResultCompose,
				ExecutionEpoch: 1,
				Diagnostic:     testtaskjournal.TaskResultDiagnosticNone,
			}
			_, err := tasks.AcknowledgeTask(
				ctx,
				agentID,
				1,
				claim.Task.Record.ID,
				claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result,
				published.task.CreatedAt.Add(time.Minute),
			)
			if faults.calls != 1 {
				t.Fatalf("terminal transaction count = %d", faults.calls)
			}
			if fault == "compare-loss" {
				current, readErr := tasks.GetTaskAssignment(ctx, claim.Task.Record.ID)
				if !isKind(err, errs.KindStateConflict) || readErr != nil ||
					published.store.revision != faults.raceRevision ||
					current.Task.Revision != claim.Task.Revision ||
					current.Assignment.Revision != claim.Assignment.Revision {
					t.Fatalf("lost terminal compare changed ownership: terminal=%v read=%v", err, readErr)
				}
				return
			}
			if !isKind(err, errs.KindInternal) || faults.committedRevision == 0 {
				t.Fatalf("expected uncertain terminal commit: %v", err)
			}
			restarted, _ := newTaskRepository(published.store)
			restarted.blueprintTerminalStore = published.store
			before := published.store.revision
			replay, err := restarted.AcknowledgeTask(
				ctx,
				agentID,
				1,
				claim.Task.Record.ID,
				claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result,
				published.task.CreatedAt.Add(2*time.Minute),
			)
			if err != nil || replay.Revision != faults.committedRevision || published.store.revision != before {
				t.Fatalf("uncertain terminal replay repeated effects: %v", err)
			}
		})
	}
}

// Rationale: separating terminal authority must not remove Retry's stricter
// epoch fence even when the desired head itself has not moved.
func TestBlueprintRetryStillRejectsInterveningEpoch(t *testing.T) {
	ctx := context.Background()
	published, tasks, claim, agentID := claimBlueprintTerminalDesiredFixture(t)
	terminal, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		claim.Task.Record.ID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusTimedOut, testtaskjournal.TaskResultRecord{
			Kind:           testtaskjournal.TaskResultCompose,
			ExecutionEpoch: 1,
			Diagnostic:     testtaskjournal.TaskResultDiagnosticTimeoutBeforeEffect,
		}, published.task.CreatedAt.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := CloneRetryTask(
		terminal.Record,
		ids.NewAt(ids.KindTask, published.task.CreatedAt, 19887),
		testtaskjournal.TaskActorOperator,
		published.task.CreatedAt.Add(2*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	change, err := tasks.prepareBlueprintCandidateRetry(ctx, terminal.Record, retry, published.store.revision)
	change.clear()
	if err != nil || !change.applies {
		t.Fatalf("unchanged retry authority rejected: %v", err)
	}
	key := testhierarchy.EnvironmentMutationEpochKey(published.environmentID)
	epoch := published.store.valueAt(key, published.store.revision)
	changed, err := published.store.Transact(
		ctx,
		nil,
		[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: epoch.Value}},
	)
	if err != nil || !changed.Succeeded {
		t.Fatal(err)
	}
	change, err = tasks.prepareBlueprintCandidateRetry(ctx, terminal.Record, retry, published.store.revision)
	defer change.clear()
	if !isKind(err, errs.KindStateConflict) || change.applies || published.store.revision != changed.Revision {
		t.Fatalf("Retry ignored intervening epoch: %v", err)
	}
}
