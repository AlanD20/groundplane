package etcd_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: every terminal attempt remains queryable while the same removal
// operation is retained, even after the retry response's ordinary TTL expires.
func TestVolumeRemovalSuccessorFailureRetainsReplay(t *testing.T) {
	ctx := context.Background()
	fixture, tasks, assignment, runtime := prepareVolumeRemovalAttemptTerminal(t, "partial")
	at := runtime.CreatedAt.Add(10 * time.Second)
	result := etcd.TaskResultRecord{
		Kind:       etcd.TaskResultEnvironmentDirectory,
		Diagnostic: etcd.TaskResultDiagnosticNone,
	}
	if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, assignment.TaskID,
		assignment.AssignmentID, etcd.TaskStatusFailed, result, at); err != nil {
		t.Fatal(err)
	}
	retryAt := at.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 94)
	marker := volumeRemovalRetryMarker(fixture.Marker, retryID, retryAt)
	if published, err := tasks.RetryTask(ctx, assignment.TaskID, retryID, etcd.TaskActorOperator, marker); err != nil {
		t.Fatal(err)
	} else if outcome, _, conflict, err := published.Classify(); err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf("retry publication: %v/%v/%v", outcome, conflict, err)
	}
	claimed, found, err := tasks.ClaimNextTask(ctx, assignment.AgentID, 1, retryAt.Add(time.Second))
	if err != nil || !found || claimed.Task.Record.ID != retryID {
		t.Fatalf("claim successor: %v/%v", found, err)
	}
	if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, retryID,
		claimed.Assignment.Record.AssignmentID, etcd.TaskStatusFailed, result, retryAt.Add(2*time.Second)); err != nil {
		t.Fatalf("successor failure: %v", err)
	}
	terminal, err := tasks.GetTask(ctx, retryID)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := etcd.NewIdempotencyRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	pruneAt := terminal.Record.RetainUntil.Add(time.Hour)
	if count, err := idempotency.PruneExpired(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("collection should expire only the unrelated seed marker: %d/%v", count, err)
	}
	if count, err := tasks.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 0 {
		t.Fatalf("retained successor Task was pruned: %d/%v", count, err)
	}
	before := fixture.Revision()
	replayed, err := tasks.RetryTask(ctx, assignment.TaskID, retryID, etcd.TaskActorOperator, marker)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, err := replayed.Classify(); err != nil || conflict != nil ||
		outcome != etcd.IdempotencyKnownExisting || fixture.Revision() != before {
		t.Fatalf("retained Retry replay changed: %v/%v/%v", outcome, conflict, err)
	}
}

// Rationale: more than one scan page of retained retries must not starve an
// unrelated expired response or cause the collector to exceed its read bound.
func TestVolumeRemovalRetainedRetriesDoNotStarvePruning(t *testing.T) {
	ctx := context.Background()
	fixture, tasks, assignment, runtime := prepareVolumeRemovalAttemptTerminal(t, "published")
	result := etcd.TaskResultRecord{
		Kind:       etcd.TaskResultEnvironmentDirectory,
		Diagnostic: etcd.TaskResultDiagnosticNone,
	}
	sourceID, assignmentID := assignment.TaskID, assignment.AssignmentID
	at := runtime.CreatedAt.Add(10 * time.Second)
	retries := []string{}
	for index := range 17 {
		if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, sourceID, assignmentID,
			etcd.TaskStatusFailed, result, at); err != nil {
			t.Fatalf("fail attempt %d: %v", index, err)
		}
		retryAt := at.Add(time.Second)
		retryID := ids.NewAt(ids.KindTask, retryAt, int64(index+300))
		marker := volumeRemovalRetryMarker(fixture.Marker, retryID, retryAt)
		marker.Locator.Key += "-" + strconv.Itoa(index)
		published, err := tasks.RetryTask(ctx, sourceID, retryID, etcd.TaskActorOperator, marker)
		if err != nil {
			t.Fatalf("retry %d: %v", index, err)
		}
		if outcome, _, conflict, err := published.Classify(); err != nil || conflict != nil ||
			outcome != etcd.IdempotencyKnownApplied {
			t.Fatalf("retry %d publication: %v/%v/%v", index, outcome, conflict, err)
		}
		claimed, found, err := tasks.ClaimNextTask(ctx, assignment.AgentID, 1, retryAt.Add(time.Second))
		if err != nil || !found || claimed.Task.Record.ID != retryID {
			t.Fatalf("claim attempt %d: %v/%v", index, found, err)
		}
		sourceID, assignmentID = retryID, claimed.Assignment.Record.AssignmentID
		retries = append(retries, retryID)
		at = retryAt.Add(2 * time.Second)
	}
	if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, sourceID, assignmentID,
		etcd.TaskStatusFailed, result, at); err != nil {
		t.Fatal(err)
	}
	controlKey := etcd.SeedIndependentPruneMarker(t, fixture.Store, at.Add(time.Second))
	terminal, err := tasks.GetTask(ctx, sourceID)
	if err != nil {
		t.Fatal(err)
	}
	pruneAt := terminal.Record.RetainUntil.Add(time.Hour)
	backend := &boundedRemovalPruneStore{Store: fixture.Store}
	pruned := 0
	for range 4 {
		backend.scans = 0
		idempotency, err := etcd.NewIdempotencyRepository(backend)
		if err != nil {
			t.Fatal(err)
		}
		count, err := idempotency.PruneExpired(ctx, pruneAt)
		if err != nil {
			t.Fatal(err)
		}
		pruned += count
	}
	control, err := fixture.Store.Get(ctx, controlKey)
	if err != nil || control.Entry != nil || pruned != 2 {
		t.Fatalf("independent markers were starved or retries expired: %d/%v", pruned, err)
	}
	if count, err := tasks.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 0 {
		t.Fatalf("retained Task was pruned: %d/%v", count, err)
	}
	for _, taskID := range retries {
		if task, err := tasks.GetTask(ctx, taskID); err != nil || task.Record.Status != etcd.TaskStatusFailed {
			t.Fatalf("retained retry %s disappeared: %v", taskID, err)
		}
	}
}

type boundedRemovalPruneStore struct {
	etcd.Store
	before func()
	scans  int
}

func (store *boundedRemovalPruneStore) Range(
	ctx context.Context,
	request etcd.RangeRequest,
) (*etcd.RangeResult, error) {
	if request.Limit > 16 {
		return nil, errs.New(errs.KindInternal, "retention scan exceeded 16 entries")
	}
	if request.Limit == 16 {
		store.scans++
		if store.scans > 1 {
			return nil, errs.New(errs.KindInternal, "retention collection scanned more than one page")
		}
	}
	return store.Store.Range(ctx, request)
}

func (store *boundedRemovalPruneStore) Transact(ctx context.Context, conditions []etcd.Condition,
	mutations []etcd.Mutation) (etcd.TransactionResult, error) {
	if len(conditions)+len(mutations) > 96 {
		return etcd.TransactionResult{}, errs.New(errs.KindInternal, "retention transaction exceeded 96 operations")
	}
	if store.before != nil {
		before := store.before
		store.before = nil
		before()
		store.scans = 0 // A known CAS conflict permits a fresh bounded scan.
	}
	return store.Store.Transact(ctx, conditions, mutations)
}

// Rationale: expiry resumes after proved operation completion, but ownership
// appearing after the collection read must still prevent deletion at commit.
func TestVolumeRemovalFinalizedMarkerExpiryFencesOwnership(t *testing.T) {
	for _, lateOwner := range []bool{false, true} {
		t.Run(strconv.FormatBool(lateOwner), func(t *testing.T) {
			ctx := context.Background()
			fixture, tasks, assignment, runtime := prepareVolumeRemovalAttemptTerminal(t, "absent")
			if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, assignment.TaskID,
				assignment.AssignmentID, etcd.TaskStatusCompleted, etcd.TaskResultRecord{
					Kind: etcd.TaskResultEnvironmentDirectory, Diagnostic: etcd.TaskResultDiagnosticNone,
				}, runtime.CreatedAt.Add(10*time.Second)); err != nil {
				t.Fatal(err)
			}
			terminal, err := tasks.GetTask(ctx, assignment.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			backend := &boundedRemovalPruneStore{Store: fixture.Store}
			if lateOwner {
				backend.before = func() {
					value, err := removalrecord.EncodeOwner(removalrecord.Owner{VolumeID: runtime.VolumeID,
						EnvironmentID: runtime.EnvironmentID, OperationID: runtime.OperationID})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := fixture.Store.Put(ctx, removalrecord.OwnerKey(runtime.VolumeID), value); err != nil {
						t.Fatal(err)
					}
				}
			}
			idempotency, err := etcd.NewIdempotencyRepository(backend)
			if err != nil {
				t.Fatal(err)
			}
			want := 2
			if lateOwner {
				want = 1
			}
			if count, err := idempotency.PruneExpired(ctx, terminal.Record.RetainUntil.Add(time.Hour)); err != nil ||
				count != want {
				t.Fatalf("finalized marker collection: %d/%v, want %d", count, err, want)
			}
			key, err := etcd.CapabilityIdempotencyMarkerKey(fixture.Marker.Locator)
			if err != nil {
				t.Fatal(err)
			}
			marker, err := fixture.Store.Get(ctx, key)
			if err != nil || (marker.Entry != nil) != lateOwner {
				t.Fatalf("finalized marker ownership fence: %v", err)
			}
		})
	}
}

// Rationale: added removal fences reduce the number of collectible markers in
// a transaction, never enlarge the existing 96-operation store limit.
func TestVolumeRemovalExpiredBatchKeepsTransactionBound(t *testing.T) {
	ctx := context.Background()
	fixture, tasks, assignment, runtime := prepareVolumeRemovalAttemptTerminal(t, "absent")
	if _, err := tasks.AcknowledgeTask(ctx, assignment.AgentID, 1, assignment.TaskID,
		assignment.AssignmentID, etcd.TaskStatusCompleted, etcd.TaskResultRecord{
			Kind: etcd.TaskResultEnvironmentDirectory, Diagnostic: etcd.TaskResultDiagnosticNone,
		}, runtime.CreatedAt.Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	completed, err := tasks.GetTask(ctx, assignment.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	backend := &boundedRemovalPruneStore{Store: fixture.Store}
	collector, err := etcd.NewIdempotencyRepository(backend)
	if err != nil {
		t.Fatal(err)
	}
	at := completed.Record.RetainUntil.Add(time.Hour)
	if count, err := collector.PruneExpired(ctx, at); err != nil || count != 2 {
		t.Fatalf("initial marker expiry: %d/%v", count, err)
	}
	keys := etcd.SeedCompletedVolumePruneBatch(t, fixture, completed.Record)
	for _, want := range []int{9, 7} {
		backend.scans = 0
		collector, err = etcd.NewIdempotencyRepository(backend)
		if err != nil {
			t.Fatal(err)
		}
		if count, err := collector.PruneExpired(ctx, at); err != nil || count != want {
			t.Fatalf("bounded expired Volume batch: %d/%v, want %d", count, err, want)
		}
	}
	read, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range read.Values {
		if value != nil {
			t.Fatal("completed Volume marker survived bounded collection")
		}
	}
}
