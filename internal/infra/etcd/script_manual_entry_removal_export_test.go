package etcd

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (fixture *ExecutedArtifactFixture) CheckManualJourneyEntryRemoval(t *testing.T, entryID string, active bool) {
	t.Helper()
	ctx := context.Background()
	current, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, fixture.Environment.Record.ID)
	if err != nil || !found || len(current.Record.Entries) != 1 || current.Record.Entries[0].Entry.ID != entryID {
		t.Fatal("manual Entry removal lost its current desired source")
	}
	headKey := environmentBlueprintHeadKey(fixture.Environment.Record.ID)
	before := fixture.store.valueAt(headKey, fixture.store.revision)
	task := fixture.Task(t, 953)
	task.RenderGeneration = int32(current.Record.RenderGeneration + 1)
	projection := current.Record
	projection.RevisionID, projection.RenderGeneration = task.ID, uint64(task.RenderGeneration)
	projection.Entries = nil
	result, err := fixture.tryPublish(t, task, projection, BlueprintReleasePublication{})
	if active {
		if !isKind(err, errs.KindResourceInUse) && !isKind(result.conflict, errs.KindResourceInUse) {
			t.Fatalf("desired Entry removal ignored retained Script generation: %v / %v", err, result.conflict)
		}
		after := fixture.store.valueAt(headKey, fixture.store.revision)
		if before == nil || after == nil || before.ModRevision != after.ModRevision ||
			!bytes.Equal(before.Value, after.Value) ||
			fixture.store.valueAt(taskKey(task.ID), fixture.store.revision) != nil {
			t.Fatal("rejected Entry removal published desired state or a Task")
		}
		return
	}
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("unblocked Entry removal = %v / %v", err, result.conflict)
	}
	fixture.head = result.revision
	claim, found, err := fixture.Tasks.ClaimNextTask(ctx, ids.New(ids.KindAgent), 1, task.CreatedAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != task.ID {
		t.Fatalf("Entry removal claim = %t, %v", found, err)
	}
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, claim.Assignment.Record.AgentID, 1, task.ID,
		claim.Assignment.Record.AssignmentID, TaskStatusCompleted,
		TaskResultRecord{Kind: TaskResultCompose, ExecutionEpoch: 1, Diagnostic: TaskResultDiagnosticNone},
		task.CreatedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	remaining, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, fixture.Environment.Record.ID)
	if err != nil || !found || len(remaining.Record.Entries) != 0 {
		t.Fatal("Entry removal did not update the desired projection")
	}
}
