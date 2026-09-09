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
	for _, checkpoint := range []string{"published", "detached", "pending", "partial", "absent"} {
		t.Run(checkpoint, func(t *testing.T) { proveVolumeRemovalRetry(t, checkpoint, "failed") })
	}
	for _, mode := range []string{"timeout", "stale agent", "repeated", "lost response"} {
		t.Run("pending/"+mode, func(t *testing.T) { proveVolumeRemovalRetry(t, "pending", mode) })
	}
}

func proveVolumeRemovalRetry(t *testing.T, checkpoint, mode string) {
	t.Helper()
	ctx := context.Background()
	fixture, tasks, assignment, runtime := prepareVolumeRemovalAttemptTerminal(t, checkpoint)
	failedAt := runtime.CreatedAt.Add(10 * time.Second)
	if mode == "timeout" || mode == "stale agent" {
		failedAt = runtime.CreatedAt.Add(6*time.Hour + time.Second)
		var count int
		var err error
		if mode == "timeout" {
			count, err = tasks.ExpireTimedOutTasks(ctx, failedAt)
		} else {
			count, err = tasks.TimeoutAgentAssignments(ctx, assignment.AgentID, 1, 24, failedAt)
		}
		if err != nil || count != 1 {
			t.Fatalf("pending timeout: %d/%v", count, err)
		}
	} else {
		if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, assignment.TaskID,
			assignment.AssignmentID, etcd.TaskStatusFailed, etcd.TaskResultRecord{
				Kind: etcd.TaskResultEnvironmentDirectory, Diagnostic: etcd.TaskResultDiagnosticNone,
			}, failedAt); err != nil {
			t.Fatal(err)
		}
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
	if checkpoint == "pending" {
		retainedKeys = append(retainedKeys, removalrecord.PendingPathKey(runtime.OperationID))
	}
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
	if checkpoint == "pending" {
		if mode == "repeated" {
			if _, err := tasks.AcknowledgeTask(ctx, next.AgentID, 1, next.TaskID, next.AssignmentID,
				etcd.TaskStatusFailed, etcd.TaskResultRecord{Kind: etcd.TaskResultEnvironmentDirectory,
					Diagnostic: etcd.TaskResultDiagnosticNone}, retryAt.Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}
			retryAt = retryAt.Add(3 * time.Second)
			retryID = ids.NewAt(ids.KindTask, retryAt, 94)
			marker = volumeRemovalRetryMarker(fixture.Marker, retryID, retryAt)
			marker.Locator.Key = "volume-removal-retry-0002"
			result, err := tasks.RetryTask(ctx, next.TaskID, retryID, etcd.TaskActorOperator, marker)
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, err := result.Classify()
			if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
				t.Fatal("second successor did not publish", err, conflict)
			}
			claimed, found, err := tasks.ClaimNextTask(ctx, assignment.AgentID, 1, retryAt.Add(time.Second))
			if err != nil || !found || claimed.Task.Record.ID != retryID {
				t.Fatalf("claim repeated successor: %v/%v", found, err)
			}
			next.TaskID, next.AssignmentID = retryID, claimed.Assignment.Record.AssignmentID
		}
		beforeDelivery := fixture.Revision()
		pending, replay, err := removals.BeginPathCall(ctx, next, retryAt.Add(2*time.Second))
		if err != nil || !replay || pending.Record.TaskID != assignment.TaskID ||
			fixture.Revision() != beforeDelivery ||
			pending.Revision != before.Values[len(before.Values)-1].ModRevision {
			t.Fatalf("successor must recover the original pending call: %v/%v", replay, err)
		}
		completion := removalrecord.Completion{OperationID: runtime.OperationID,
			RequestOrdinal: pending.Record.RequestOrdinal, RequestSHA256: pending.Record.RequestSHA256,
			DirectoryAbsent: true, CompletedAt: retryAt.Add(3 * time.Second)}
		result := volumeremoval.EnvironmentVolumeRemovalPathResult{
			Assignment: assignment, RequestOrdinal: completion.RequestOrdinal, RequestSHA256: completion.RequestSHA256,
			DirectoryAbsent: true, CompletedAt: completion.CompletedAt,
			ResponseBytes: removalrecord.PathResponseBytes(
				completion,
			), ResponseSHA256: removalrecord.PathResponseDigest(completion),
		}
		if _, _, err := removals.CompletePathCall(ctx, result); err == nil {
			t.Fatal("terminal predecessor assignment completed the pending call")
		}
		result.Assignment = next
		if mode == "lost response" {
			removals, err = volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(
				&volumePendingResponseLossStore{Store: fixture.Store, lose: true},
			)
			if err != nil {
				t.Fatal(err)
			}
		}
		_, _, err = removals.CompletePathCall(ctx, result)
		if mode == "lost response" {
			if kind, _ := errs.KindOf(err); kind != errs.KindRequestFailed {
				t.Fatalf("completion response was not lost: %v", err)
			}
		} else if err != nil {
			t.Fatalf("successor completion: %v", err)
		}
		completedRevision := fixture.Revision()
		if _, replay, err := removals.CompletePathCall(ctx, result); err != nil || !replay ||
			fixture.Revision() != completedRevision {
			t.Fatalf("successor completion replay: %v/%v", replay, err)
		}
		if _, err := tasks.AcknowledgeTask(ctx, next.AgentID, 1, next.TaskID, next.AssignmentID,
			etcd.TaskStatusCompleted, etcd.TaskResultRecord{Kind: etcd.TaskResultEnvironmentDirectory,
				Diagnostic: etcd.TaskResultDiagnosticNone}, retryAt.Add(4*time.Second)); err != nil {
			t.Fatalf("recovered operation terminalization: %v", err)
		}
		final, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: []string{
			etcd.CapabilityTaskKey(runtime.OriginTaskID), removalrecord.RuntimeKey(runtime.OperationID),
			removalrecord.OwnerKey(runtime.VolumeID), removalrecord.EnvironmentLockKey(runtime.EnvironmentID),
			removalrecord.PendingPathKey(runtime.OperationID),
		}})
		if err != nil || final.Values[0] == nil || final.Values[0].ModRevision != before.Values[1].ModRevision ||
			!bytes.Equal(final.Values[0].Value, before.Values[1].Value) {
			t.Fatal("recovery changed the failed origin Task", err)
		}
		for _, value := range final.Values[1:] {
			if value != nil {
				t.Fatal("recovered completion retained removal authority")
			}
		}
	}
}

type volumePendingResponseLossStore struct {
	etcd.Store
	lose bool
}

func (store *volumePendingResponseLossStore) Transact(ctx context.Context, conditions []etcd.Condition,
	mutations []etcd.Mutation) (etcd.TransactionResult, error) {
	result, err := store.Store.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.lose {
		store.lose = false
		return etcd.TransactionResult{}, errs.New(errs.KindRequestFailed, "lost pending completion response")
	}
	return result, err
}

// Rationale: Retry cannot publish a queued Task after either removal owner
// changes, including a replacement immediately before the final transaction.
func TestVolumeRemovalGenericRetryRejectsChangedOwner(t *testing.T) {
	for _, family := range []string{"volume", "environment", "root response"} {
		modes := []string{"held", "late"}
		if family != "root response" {
			modes = append(modes, "missing", "corrupt", "wrong environment", "wrong volume")
		}
		for _, mode := range modes {
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
				owner := removalrecord.Owner{VolumeID: runtime.VolumeID,
					EnvironmentID: runtime.EnvironmentID, OperationID: ids.New(ids.KindOperation)}
				if mode == "wrong environment" {
					owner.OperationID, owner.EnvironmentID = runtime.OperationID, ids.New(ids.KindEnvironment)
				}
				if mode == "wrong volume" {
					owner.OperationID, owner.VolumeID = runtime.OperationID, ids.New(ids.KindVolume)
				}
				value, err := removalrecord.EncodeOwner(owner)
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
				if mode == "corrupt" {
					value = []byte("corrupt owner")
				}
				inject := func() {
					if mode == "missing" {
						if _, err := fixture.Store.Transact(ctx, nil, []etcd.Mutation{{Type: etcd.MutationDelete, Key: key}}); err != nil {
							t.Fatal(err)
						}
						return
					}
					if _, err := fixture.Store.Put(ctx, key, value); err != nil {
						t.Fatal(err)
					}
				}
				backend := &volumeAttemptTerminalRaceStore{Store: fixture.Store}
				if mode != "late" {
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

// Rationale: retained calls cannot become execution authority for another
// Volume or a non-failed attempt, and all recovery reads must survive final CAS.
func TestVolumeRemovalPendingRecoveryRejectsChangedEvidence(t *testing.T) {
	for _, phase := range []string{"retry", "completion"} {
		for _, family := range []string{"pending", "origin", "volume owner", "environment owner"} {
			for _, mode := range []string{"held", "late"} {
				t.Run(phase+"/"+family+"/"+mode, func(t *testing.T) {
					proveVolumeRemovalPendingEvidence(t, phase, family, mode)
				})
			}
		}
	}
}

func proveVolumeRemovalPendingEvidence(t *testing.T, phase, family, mode string) {
	t.Helper()
	ctx := context.Background()
	fixture, tasks, assignment, runtime := prepareVolumeRemovalAttemptTerminal(t, "pending")
	at := runtime.CreatedAt.Add(10 * time.Second)
	if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, assignment.TaskID, assignment.AssignmentID,
		etcd.TaskStatusFailed, etcd.TaskResultRecord{Kind: etcd.TaskResultEnvironmentDirectory,
			Diagnostic: etcd.TaskResultDiagnosticNone}, at); err != nil {
		t.Fatal(err)
	}
	retryID := ids.NewAt(ids.KindTask, at.Add(time.Second), 98)
	marker := volumeRemovalRetryMarker(fixture.Marker, retryID, at.Add(time.Second))
	if phase == "completion" {
		result, err := tasks.RetryTask(ctx, assignment.TaskID, retryID, etcd.TaskActorOperator, marker)
		if err != nil {
			t.Fatal(err)
		}
		outcome, _, conflict, err := result.Classify()
		if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
			t.Fatal("Retry did not publish", err, conflict)
		}
		claimed, found, err := tasks.ClaimNextTask(ctx, assignment.AgentID, 1, at.Add(2*time.Second))
		if err != nil || !found || claimed.Task.Record.ID != retryID {
			t.Fatalf("claim: %v/%v", found, err)
		}
		assignment.TaskID, assignment.AssignmentID = retryID, claimed.Assignment.Record.AssignmentID
	}
	key := removalrecord.PendingPathKey(runtime.OperationID)
	switch family {
	case "origin":
		key = etcd.CapabilityTaskKey(runtime.OriginTaskID)
	case "volume owner":
		key = removalrecord.OwnerKey(runtime.VolumeID)
	case "environment owner":
		key = removalrecord.EnvironmentLockKey(runtime.EnvironmentID)
	}
	read, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: []string{key,
		removalrecord.PendingPathKey(runtime.OperationID)}})
	if err != nil || read.Values[0] == nil || read.Values[1] == nil {
		t.Fatal("missing recovery evidence", err)
	}
	pending, err := removalrecord.DecodePendingPath(read.Values[1].Value)
	if err != nil {
		t.Fatal(err)
	}
	value := read.Values[0].Value
	if mode == "held" {
		switch family {
		case "pending":
			changed := pending
			changed.VolumeID = ids.New(ids.KindVolume)
			changed.RequestSHA256 = removalrecord.PathRequestDigest(changed)
			value, err = removalrecord.EncodePendingPath(changed)
		case "origin":
			var origin etcd.TaskRecord
			origin, err = etcd.DecodeCapabilityTaskRecord(value)
			if err == nil {
				origin.Status = etcd.TaskStatusCompleted
				value, err = etcd.EncodeCapabilityTaskRecord(origin)
			}
		default:
			value, err = removalrecord.EncodeOwner(removalrecord.Owner{VolumeID: runtime.VolumeID,
				EnvironmentID: runtime.EnvironmentID, OperationID: ids.New(ids.KindOperation)})
		}
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
	before := fixture.Revision()
	if phase == "retry" {
		tasks, err = etcd.NewTaskRepository(backend)
		if err != nil {
			t.Fatal(err)
		}
		var result etcd.IdempotencyTransactionResult
		result, err = tasks.RetryTask(ctx, assignment.TaskID, retryID, etcd.TaskActorOperator, marker)
		if err == nil {
			_, _, conflict, classifyErr := result.Classify()
			if classifyErr != nil {
				t.Fatal(classifyErr)
			}
			err = conflict
		}
	} else {
		removals, setupErr := volumeremoval.NewEnvironmentVolumeRemovalRuntimeRepository(backend)
		if setupErr != nil {
			t.Fatal(setupErr)
		}
		completion := removalrecord.Completion{OperationID: runtime.OperationID,
			RequestOrdinal: pending.RequestOrdinal, RequestSHA256: pending.RequestSHA256,
			DirectoryAbsent: true, CompletedAt: at.Add(3 * time.Second)}
		_, _, err = removals.CompletePathCall(ctx, volumeremoval.EnvironmentVolumeRemovalPathResult{
			Assignment: assignment, RequestOrdinal: completion.RequestOrdinal, RequestSHA256: completion.RequestSHA256,
			DirectoryAbsent: true, CompletedAt: completion.CompletedAt,
			ResponseBytes: removalrecord.PathResponseBytes(completion), ResponseSHA256: removalrecord.PathResponseDigest(completion),
		})
	}
	writes := int64(0)
	if mode == "late" {
		writes = 1
	}
	if err == nil || fixture.Revision() != before+writes {
		t.Fatalf("recovery ignored changed evidence: %v; revision delta %d", err, fixture.Revision()-before)
	}
}
