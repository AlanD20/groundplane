package etcd_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the operator's generic Retry must advance the retained removal
// operation atomically, not merely enqueue a cloned Task with stale parameters.
func TestVolumeRemovalGenericRetryAdvancesRetainedOperation(t *testing.T) {
	for _, checkpoint := range []string{"published", "detached", "partial", "absent"} {
		t.Run(checkpoint, func(t *testing.T) { proveVolumeRemovalRetry(t, checkpoint) })
	}
}

func proveVolumeRemovalRetry(t *testing.T, checkpoint string) {
	t.Helper()
	ctx := context.Background()
	fixture, tasks, assignment, runtime := prepareVolumeRemovalAttemptTerminal(t, checkpoint)
	failedAt := runtime.CreatedAt.Add(10 * time.Second)
	if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, assignment.TaskID,
		assignment.AssignmentID, etcd.TaskStatusFailed, etcd.TaskResultRecord{
			Kind: etcd.TaskResultEnvironmentDirectory, Diagnostic: etcd.TaskResultDiagnosticNone,
		}, failedAt); err != nil {
		t.Fatal(err)
	}
	retryAt := failedAt.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 93)
	marker := volumeRemovalRetryMarker(fixture.Marker, retryID, retryAt)
	rootKey, err := etcd.CapabilityIdempotencyMarkerKey(fixture.Marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	retainedKeys := []string{rootKey, etcd.CapabilityTaskKey(assignment.TaskID),
		removalrecord.ProgressKey(runtime.OperationID), removalrecord.OwnerKey(runtime.VolumeID),
		removalrecord.EnvironmentLockKey(runtime.EnvironmentID)}
	before, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: retainedKeys})
	if err != nil {
		t.Fatal(err)
	}
	result, err := tasks.RetryTask(ctx, assignment.TaskID, retryID, etcd.TaskActorOperator, marker)
	if err != nil {
		t.Fatalf("generic Volume Retry: %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf("generic Volume Retry outcome: %v/%v/%v", outcome, conflict, err)
	}
	fixture.AssertRemovalRetryBudget(t)
	retryMarkerKey, err := etcd.CapabilityIdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	atomic, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: []string{
		etcd.CapabilityTaskKey(retryID), removalrecord.RuntimeKey(runtime.OperationID),
		removalrecord.AttemptKey(runtime.OperationID, 2), retryMarkerKey,
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range atomic.Values {
		if value == nil || value.ModRevision != fixture.Revision() {
			t.Fatal("Retry did not publish Task, runtime, attempt and response atomically")
		}
	}
	removals, err := volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	state, err := removals.Resume(ctx, runtime.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Runtime.Record.CurrentTaskID != retryID || state.Runtime.Record.OriginTaskID != assignment.TaskID ||
		state.Runtime.Record.AttemptOrdinal != 2 || state.Attempt.PredecessorTaskID != assignment.TaskID ||
		state.Runtime.Record.DesiredRevisionID != runtime.DesiredRevisionID {
		t.Fatal("generic Retry did not advance the same removal operation")
	}
	after, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: retainedKeys})
	if err != nil {
		t.Fatal(err)
	}
	for index, original := range before.Values {
		if original == nil || after.Values[index] == nil || original.ModRevision != after.Values[index].ModRevision ||
			!bytes.Equal(original.Value, after.Values[index].Value) {
			t.Fatalf("Retry changed retained authority %s", retainedKeys[index])
		}
	}
	revision := fixture.Revision()
	replay, err := tasks.RetryTask(ctx, assignment.TaskID, retryID, etcd.TaskActorOperator, marker)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err = replay.Classify()
	if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownExisting || fixture.Revision() != revision {
		t.Fatalf("Retry replay changed state: %v/%v/%v", outcome, conflict, err)
	}
	if _, err := fixture.Publish(ctx); err != nil || fixture.Revision() != revision {
		t.Fatalf("root DELETE replay changed state: %v", err)
	}
	claimed, found, err := tasks.ClaimNextTask(ctx, assignment.AgentID, 1, retryAt.Add(time.Second))
	if err != nil || !found || claimed.Task.Record.ID != retryID {
		t.Fatalf("claim successor: %v/%v", found, err)
	}
	next := volumeremoval.EnvironmentVolumeRemovalAssignment{
		OperationID: runtime.OperationID, TaskID: retryID, AssignmentID: claimed.Assignment.Record.AssignmentID,
		AgentID: assignment.AgentID, AgentGeneration: 1,
	}
	if checkpoint == "published" {
		if _, err := removals.MarkConsumersDetached(ctx, next, retryAt.Add(2*time.Second)); err != nil {
			t.Fatalf("successor assignment rejected: %v", err)
		}
	}
}

// Rationale: Retry cannot publish a queued Task after either removal owner
// changes, including a replacement immediately before the final transaction.
func TestVolumeRemovalGenericRetryRejectsChangedOwner(t *testing.T) {
	for _, family := range []string{"volume", "environment", "root response"} {
		for _, mode := range []string{"held", "late"} {
			t.Run(family+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				fixture, tasks, assignment, runtime := prepareVolumeRemovalAttemptTerminal(t, "published")
				at := runtime.CreatedAt.Add(10 * time.Second)
				if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, assignment.TaskID,
					assignment.AssignmentID, etcd.TaskStatusFailed, etcd.TaskResultRecord{
						Kind: etcd.TaskResultEnvironmentDirectory, Diagnostic: etcd.TaskResultDiagnosticNone,
					}, at); err != nil {
					t.Fatal(err)
				}
				key := removalrecord.OwnerKey(runtime.VolumeID)
				if family == "environment" {
					key = removalrecord.EnvironmentLockKey(runtime.EnvironmentID)
				}
				value, err := removalrecord.EncodeOwner(removalrecord.Owner{VolumeID: runtime.VolumeID,
					EnvironmentID: runtime.EnvironmentID, OperationID: ids.New(ids.KindOperation)})
				if err != nil {
					t.Fatal(err)
				}
				if family == "root response" {
					key, err = etcd.CapabilityIdempotencyMarkerKey(fixture.Marker.Locator)
					if err != nil {
						t.Fatal(err)
					}
					changed := fixture.Marker
					changed.TaskID = ids.NewAt(ids.KindTask, at, 96)
					changed.Response.Body = []byte(`{"task_id":"` + changed.TaskID + `"}`)
					value, err = etcd.EncodeCapabilityIdempotencyMarker(changed)
					if err != nil {
						t.Fatal(err)
					}
				}
				inject := func() {
					if _, err := fixture.Store.Put(ctx, key, value); err != nil {
						t.Fatal(err)
					}
				}
				backend := &volumeAttemptTerminalRaceStore{Store: fixture.Store}
				if mode == "held" {
					inject()
				} else {
					backend.before = inject
				}
				tasks, err = etcd.NewTaskRepository(backend)
				if err != nil {
					t.Fatal(err)
				}
				retryAt := at.Add(time.Second)
				retryID := ids.NewAt(ids.KindTask, retryAt, 95)
				before := fixture.Revision()
				result, err := tasks.RetryTask(ctx, assignment.TaskID, retryID, etcd.TaskActorOperator,
					volumeRemovalRetryMarker(fixture.Marker, retryID, retryAt))
				if err == nil {
					_, _, conflict, classifyErr := result.Classify()
					if classifyErr != nil {
						t.Fatal(classifyErr)
					}
					err = conflict
				}
				writes := int64(0)
				if mode == "late" {
					writes = 1
				}
				if kind, _ := errs.KindOf(err); kind != errs.KindStateConflict || fixture.Revision() != before+writes {
					t.Fatalf("Retry ignored changed owner: %v; revision delta %d", err, fixture.Revision()-before)
				}
			})
		}
	}
}

func volumeRemovalRetryMarker(marker etcd.IdempotencyMarker, taskID string, at time.Time) etcd.IdempotencyMarker {
	marker.Locator.Method, marker.Locator.Route = "POST", "/tasks/{id}/retry"
	marker.Locator.Key = "volume-removal-retry-0001"
	marker.ReplayTarget = nil
	marker.TaskID = taskID
	marker.CreatedAt, marker.UpdatedAt = at, at
	marker.Response.Body = []byte(`{"task_id":"` + taskID + `"}`)
	return marker
}
