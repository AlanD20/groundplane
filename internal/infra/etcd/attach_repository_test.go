package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: Attach creation must publish metadata, every ownership index, and encrypted facts in one revision.
func TestAttachRepositoryCreatesAndReadsAtomicAttach(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 31, "api-db", nil)

	created := createTestAttach(t, ctx, repository, scope, record, &facts)
	if created.Revision == 0 || created.Record.ID != record.ID {
		t.Fatalf("CreateAttach() = %#v", created)
	}
	epoch, err := store.Get(ctx, environmentMutationEpochKey(record.EnvironmentID))
	if err != nil || epoch.Entry == nil || epoch.Entry.ModRevision != created.Revision {
		t.Fatalf("Attach creation mutation epoch = %#v, %v", epoch, err)
	}
	resolved, err := repository.ResolveAttach(ctx, record.EnvironmentID, record.Name)
	if err != nil {
		t.Fatalf("ResolveAttach() error = %v", err)
	}
	if resolved.Record.ID != record.ID || resolved.ReadRevision != created.Revision {
		t.Fatalf("ResolveAttach() = %#v", resolved)
	}
	if resolved.Record.BackingNetworkID != record.BackingNetworkID {
		t.Fatalf("ResolveAttach() lost backing network binding: %#v", resolved.Record)
	}
	page, err := repository.ListAttaches(ctx, record.EnvironmentID, PageRequest{Limit: 10})
	if err != nil {
		t.Fatalf("ListAttaches() error = %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].Record.ID != record.ID {
		t.Fatalf("ListAttaches() = %#v", page)
	}
	storedFacts, ok, err := repository.GetAttachFacts(ctx, created)
	if err != nil {
		t.Fatalf("GetAttachFacts() error = %v", err)
	}
	defer clear(storedFacts.Ciphertext)
	if !ok || string(storedFacts.Ciphertext) != "encrypted-facts" {
		t.Fatalf("GetAttachFacts() = %#v, %t", storedFacts, ok)
	}
	for _, key := range []string{
		attachOwnerKey(record.EnvironmentID, record.ID),
		attachNameKey(record.EnvironmentID, record.Name),
		attachServiceKey(record.ServiceID, record.ID),
		attachBackingServiceKey(record.BackingServiceID, record.ID),
		attachBackingProjectKey(record.BackingProjectID, record.ID),
	} {
		result, getErr := store.Get(ctx, key)
		if getErr != nil || result.Entry == nil || string(result.Entry.Value) != record.ID ||
			result.Entry.ModRevision != created.Revision {
			t.Fatalf("index %s = %#v, error = %v", key, result, getErr)
		}
	}
}

// Rationale: retry must retain the exact generated identity and fact metadata while changing only task lifecycle state.
func TestAttachLifecycleRetryPreservesIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 41, "worker-db", nil)
	current := createTestAttach(t, ctx, repository, scope, record, &facts)
	provisioning, err := MarkAttachProvisioning(current.Record, current.Record.TaskID)
	if err != nil {
		t.Fatalf("MarkAttachProvisioning() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, provisioning)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(provisioning) error = %v", err)
	}
	failed, err := CompleteAttachProvisioning(current.Record, current.Record.TaskID, false)
	if err != nil {
		t.Fatalf("CompleteAttachProvisioning() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, failed)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(failed) error = %v", err)
	}
	retryTaskID := ids.NewAt(ids.KindTask, testAttachTime.Add(time.Minute), 42)
	retried, err := RetryAttachOperation(current.Record, retryTaskID)
	if err != nil {
		t.Fatalf("RetryAttachOperation() error = %v", err)
	}
	if retried.ID != record.ID || retried.Name != record.Name || retried.TaskID != retryTaskID ||
		retried.Status != core.AttachPending || retried.BackingNetworkID != record.BackingNetworkID ||
		len(retried.FactSets) != len(record.FactSets) ||
		retried.FactSets[0].Facts[0] != record.FactSets[0].Facts[0] {
		t.Fatalf("RetryAttachOperation() changed durable identity: %#v", retried)
	}
	if _, err = repository.ReplaceLifecycle(ctx, current, retried); err != nil {
		t.Fatalf("ReplaceLifecycle(retry) error = %v", err)
	}
}

// Rationale: a missing Service runtime sidecar is the normal pre-observation state and must
// resolve to the explicit running intent without changing the sealed desired projection.
func TestServiceResolutionDefaultsMissingRuntimeToRunning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	serviceID := scope.Services[0].Record.Desired.ID
	if _, err := store.Delete(ctx, serviceRuntimeKey(serviceID)); err != nil {
		t.Fatalf("Delete(Service runtime sidecar) error = %v", err)
	}
	services, err := NewServiceRepository(store)
	if err != nil {
		t.Fatalf("NewServiceRepository() error = %v", err)
	}
	resolved, err := services.GetService(ctx, serviceID)
	if err != nil {
		t.Fatalf("GetService() error = %v", err)
	}
	if resolved.Record.Desired.ID != serviceID || resolved.Record.Runtime.ServiceID != serviceID ||
		resolved.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		t.Fatalf("GetService(missing runtime) = %#v", resolved.Record)
	}
}

// Rationale: reverse grant membership must prevent target deletion and serialize grant creation against target lifecycle.
func TestAttachRepositoryProtectsGrantedAttach(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	targetRecord, targetFacts := testPendingAttach(t, scope, 51, "target-db", nil)
	target := createTestAttach(t, ctx, repository, scope, targetRecord, &targetFacts)
	target, err = advanceAttachReady(ctx, repository, target)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}

	grantScope := scope
	grantScope.Grants = []Versioned[AttachRecord]{target}
	sourceRecord, sourceFacts := testPendingAttach(t, grantScope, 52, "source-db", []Versioned[AttachRecord]{target})
	createTestAttach(t, ctx, repository, grantScope, sourceRecord, &sourceFacts)
	target, err = repository.GetAttach(ctx, target.Record.ID)
	if err != nil {
		t.Fatalf("GetAttach(target) error = %v", err)
	}
	detachTaskID := ids.NewAt(ids.KindTask, testAttachTime.Add(2*time.Minute), 53)
	detaching, err := BeginAttachDetaching(target.Record, detachTaskID)
	if err != nil {
		t.Fatalf("BeginAttachDetaching() error = %v", err)
	}
	target, err = repository.ReplaceLifecycle(ctx, target, detaching)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detaching) error = %v", err)
	}
	detached, err := CompleteAttachDetaching(target.Record, detachTaskID, true)
	if err != nil {
		t.Fatalf("CompleteAttachDetaching() error = %v", err)
	}
	target, err = repository.ReplaceLifecycle(ctx, target, detached)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detached) error = %v", err)
	}
	_, err = repository.DeleteDetachedAttach(ctx, target)
	kind, _ := errs.KindOf(err)
	if kind != errs.KindResourceInUse {
		t.Fatalf("DeleteDetachedAttach() error = %v, want resource in use", err)
	}
}

// Rationale: the locked one-consumer/eight-grant maximum must remain within the atomic transaction ceiling.
func TestAttachRepositoryMaximumCombinationFitsTransactionBudget(t *testing.T) {
	t.Parallel()
	record := AttachRecord{
		ID:                 ids.NewAt(ids.KindAttach, testAttachTime, 902),
		ServiceID:          ids.NewAt(ids.KindService, testAttachTime, 901),
		CredentialAttachID: ids.NewAt(ids.KindAttach, testAttachTime, 902),
		GrantAttachIDs:     make([]string, 8),
	}
	if got := attachCreateWithTaskOperationCount(record, true); got > maximumTransactionOperations {
		t.Fatalf(
			"attachCreateWithTaskOperationCount() = %d, want at most %d",
			got,
			maximumTransactionOperations,
		)
	}
	if got := attachDetachWithTaskOperationCount(record); got != 63 {
		t.Fatalf("attachDetachWithTaskOperationCount() = %d, want 63", got)
	}
}

// Rationale: detach intent, immutable plan input, Agent Task, queue membership, replay evidence,
// and the Environment topology fence must become visible at one revision.
func TestAttachRepositoryPublishesDetachTaskAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 61, "detach-atomic", nil)
	ready := createTestAttach(t, ctx, repository, scope, record, &facts)
	ready, err = advanceAttachReady(ctx, repository, ready)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}

	task := publishTestDetach(t, ctx, repository, scope, ready, record.CreatedAt.Add(time.Minute))
	detaching, err := repository.GetAttach(ctx, record.ID)
	if err != nil || detaching.Record.Status != core.AttachDetaching || detaching.Record.TaskID != task.ID {
		t.Fatalf("detaching Attach = %#v, %v", detaching, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	currentTask, err := tasks.GetTask(ctx, task.ID)
	if err != nil || currentTask.Revision != detaching.Revision {
		t.Fatalf("detach Task = %#v, %v", currentTask, err)
	}
	renderInput, err := repository.GetAttachTaskRenderInput(ctx, task.PlanID)
	if err != nil || renderInput.Revision != detaching.Revision {
		t.Fatalf("detach render input = %#v, %v", renderInput, err)
	}
	epoch, err := store.Get(ctx, environmentMutationEpochKey(record.EnvironmentID))
	if err != nil || epoch.Entry == nil || epoch.Entry.ModRevision != detaching.Revision {
		t.Fatalf("Attach mutation epoch = %#v, %v", epoch, err)
	}
}

// Rationale: the final transaction after idempotency, Environment fencing,
// and Backup exclusion composition must fit both its estimator and store cap.
func TestAttachDetachOperationBudgetMatchesComposedTransaction(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 65, "detach-budget", nil)
	ready := createTestAttach(t, ctx, repository, scope, record, &facts)
	ready, err = advanceAttachReady(ctx, repository, ready)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}
	capture := &attachDetachOperationCaptureStore{
		attachTestStore: store,
		attachID:        record.ID,
	}
	capturedRepository, err := NewAttachRepository(capture)
	if err != nil {
		t.Fatalf("NewAttachRepository(capture) error = %v", err)
	}
	publishTestDetach(t, ctx, capturedRepository, scope, ready, record.CreatedAt.Add(5*time.Minute))
	want := attachDetachWithTaskOperationCount(record)
	if capture.operations <= 0 || capture.operations > want || capture.operations > maximumTransactionOperations {
		t.Fatalf(
			"composed Attach detach operations = %d, want at most %d and at most %d",
			capture.operations,
			want,
			maximumTransactionOperations,
		)
	}
}

// Rationale: an Attach selected by an active Backup run must remain stable
// across detach initiation and final durable deprovisioning.
func TestAttachRepositoryRejectsDestructiveBackupSourceMutations(t *testing.T) {
	t.Run("detach lifecycle replacement", func(t *testing.T) {
		ctx := context.Background()
		store := newAttachTestStore()
		scope := seedAttachScope(t, ctx, store)
		repository, err := NewAttachRepository(store)
		if err != nil {
			t.Fatalf("NewAttachRepository() error = %v", err)
		}
		record, facts := testPendingAttach(t, scope, 62, "backup-source-detach", nil)
		ready := createTestAttach(t, ctx, repository, scope, record, &facts)
		ready, err = advanceAttachReady(ctx, repository, ready)
		if err != nil {
			t.Fatalf("advanceAttachReady() error = %v", err)
		}
		exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
		if err != nil {
			t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
		}
		exclusionValue := testAttachBackupExclusionValue(
			t, scope.Environment.Record.ID, record.ID, 621,
		)
		if _, err := store.Put(ctx, exclusionKey, exclusionValue); err != nil {
			t.Fatalf("Put(Backup source exclusion) error = %v", err)
		}
		ready, err = repository.GetAttach(ctx, record.ID)
		if err != nil {
			t.Fatalf("GetAttach() error = %v", err)
		}
		detaching, err := BeginAttachDetaching(
			ready.Record,
			ids.NewAt(ids.KindTask, testAttachTime.Add(2*time.Minute), 620),
		)
		if err != nil {
			t.Fatalf("BeginAttachDetaching() error = %v", err)
		}
		_, err = repository.ReplaceLifecycle(ctx, ready, detaching)
		if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
			t.Fatalf("ReplaceLifecycle(detaching) error = %v, want resource in use", err)
		}
	})

	t.Run("detached record removal", func(t *testing.T) {
		ctx := context.Background()
		store := newAttachTestStore()
		scope := seedAttachScope(t, ctx, store)
		repository, err := NewAttachRepository(store)
		if err != nil {
			t.Fatalf("NewAttachRepository() error = %v", err)
		}
		record, facts := testPendingAttach(t, scope, 63, "backup-source-removal", nil)
		current := createTestAttach(t, ctx, repository, scope, record, &facts)
		current, err = advanceAttachReady(ctx, repository, current)
		if err != nil {
			t.Fatalf("advanceAttachReady() error = %v", err)
		}
		taskID := ids.NewAt(ids.KindTask, testAttachTime.Add(3*time.Minute), 630)
		detaching, err := BeginAttachDetaching(current.Record, taskID)
		if err != nil {
			t.Fatalf("BeginAttachDetaching() error = %v", err)
		}
		current, err = repository.ReplaceLifecycle(ctx, current, detaching)
		if err != nil {
			t.Fatalf("ReplaceLifecycle(detaching) error = %v", err)
		}
		detached, err := CompleteAttachDetaching(current.Record, taskID, true)
		if err != nil {
			t.Fatalf("CompleteAttachDetaching() error = %v", err)
		}
		current, err = repository.ReplaceLifecycle(ctx, current, detached)
		if err != nil {
			t.Fatalf("ReplaceLifecycle(detached) error = %v", err)
		}
		exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
		if err != nil {
			t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
		}
		exclusionValue := testAttachBackupExclusionValue(
			t, scope.Environment.Record.ID, record.ID, 631,
		)
		if _, err := store.Put(ctx, exclusionKey, exclusionValue); err != nil {
			t.Fatalf("Put(Backup source exclusion) error = %v", err)
		}
		current, err = repository.GetAttach(ctx, record.ID)
		if err != nil {
			t.Fatalf("GetAttach() error = %v", err)
		}
		_, err = repository.DeleteDetachedAttach(ctx, current)
		if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
			t.Fatalf("DeleteDetachedAttach() error = %v, want resource in use", err)
		}
	})
}

// Rationale: an Attach detach failure preserves truthful non-destructive state
// even when a Backup source exclusion becomes active after detach initiation.
func TestAttachRepositoryAllowsDetachFailureWithActiveBackupSourceExclusion(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 68, "backup-source-detach-failure", nil)
	current := createTestAttach(t, ctx, repository, scope, record, &facts)
	current, err = advanceAttachReady(ctx, repository, current)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}
	taskID := ids.NewAt(ids.KindTask, testAttachTime.Add(8*time.Minute), 680)
	detaching, err := BeginAttachDetaching(current.Record, taskID)
	if err != nil {
		t.Fatalf("BeginAttachDetaching() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, detaching)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detaching) error = %v", err)
	}
	exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
	if err != nil {
		t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
	}
	exclusionValue := testAttachBackupExclusionValue(
		t, scope.Environment.Record.ID, record.ID, 681,
	)
	if _, err := store.Put(ctx, exclusionKey, exclusionValue); err != nil {
		t.Fatalf("Put(Backup source exclusion) error = %v", err)
	}
	current, err = repository.GetAttach(ctx, record.ID)
	if err != nil {
		t.Fatalf("GetAttach(detaching) error = %v", err)
	}
	failed, err := CompleteAttachDetaching(current.Record, taskID, false)
	if err != nil {
		t.Fatalf("CompleteAttachDetaching(failed) error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, failed)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(failed) error = %v", err)
	}
	if current.Record.Status != core.AttachFailed || current.Record.Operation != AttachOperationDetach {
		t.Fatalf("failed Attach = %#v", current.Record)
	}
}

// Rationale: malformed or misbucketed Backup exclusion bytes are corrupt
// authority, not a valid resource-in-use fence that may be trusted blindly.
func TestAttachRepositoryRejectsInvalidBackupSourceExclusionEvidence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		value func(*testing.T, AttachCreateScope, AttachRecord) []byte
	}{
		{
			name: "malformed",
			value: func(*testing.T, AttachCreateScope, AttachRecord) []byte {
				return []byte("not-an-exclusion-record")
			},
		},
		{
			name: "target mismatch",
			value: func(t *testing.T, scope AttachCreateScope, _ AttachRecord) []byte {
				return testAttachBackupExclusionValue(
					t,
					scope.Environment.Record.ID,
					ids.NewAt(ids.KindAttach, testAttachTime.Add(7*time.Minute), 671),
					672,
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := newAttachTestStore()
			scope := seedAttachScope(t, ctx, store)
			repository, err := NewAttachRepository(store)
			if err != nil {
				t.Fatalf("NewAttachRepository() error = %v", err)
			}
			record, facts := testPendingAttach(t, scope, 67, "invalid-exclusion", nil)
			current := createTestAttach(t, ctx, repository, scope, record, &facts)
			current, err = advanceAttachReady(ctx, repository, current)
			if err != nil {
				t.Fatalf("advanceAttachReady() error = %v", err)
			}
			exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
			if err != nil {
				t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
			}
			if _, err := store.Put(ctx, exclusionKey, test.value(t, scope, record)); err != nil {
				t.Fatalf("Put(invalid Backup exclusion) error = %v", err)
			}
			current, err = repository.GetAttach(ctx, record.ID)
			if err != nil {
				t.Fatalf("GetAttach() error = %v", err)
			}
			detaching, err := BeginAttachDetaching(
				current.Record,
				ids.NewAt(ids.KindTask, testAttachTime.Add(7*time.Minute), 673),
			)
			if err != nil {
				t.Fatalf("BeginAttachDetaching() error = %v", err)
			}
			if _, err := repository.ReplaceLifecycle(ctx, current, detaching); !errors.Is(
				err,
				errs.New(errs.KindInternal, ""),
			) {
				t.Fatalf("ReplaceLifecycle(invalid exclusion) error = %v, want internal", err)
			}
			stored, err := repository.GetAttach(ctx, record.ID)
			if err != nil || stored.Record.Status != core.AttachReady {
				t.Fatalf("GetAttach(after invalid exclusion) = %#v/%v", stored, err)
			}
		})
	}
}

// Rationale: a new incoming grant committed after the fixed read must fail the
// removal CAS so the detached Attach and its new reverse membership survive.
func TestDeleteDetachedAttachRacesIncomingGrant(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 66, "incoming-grant-race", nil)
	current := createTestAttach(t, ctx, repository, scope, record, &facts)
	current, err = advanceAttachReady(ctx, repository, current)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}
	taskID := ids.NewAt(ids.KindTask, testAttachTime.Add(6*time.Minute), 660)
	detaching, err := BeginAttachDetaching(current.Record, taskID)
	if err != nil {
		t.Fatalf("BeginAttachDetaching() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, detaching)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detaching) error = %v", err)
	}
	detached, err := CompleteAttachDetaching(current.Record, taskID, true)
	if err != nil {
		t.Fatalf("CompleteAttachDetaching() error = %v", err)
	}
	current, err = repository.ReplaceLifecycle(ctx, current, detached)
	if err != nil {
		t.Fatalf("ReplaceLifecycle(detached) error = %v", err)
	}

	dependentID := ids.NewAt(ids.KindAttach, testAttachTime.Add(6*time.Minute), 661)
	reverseKey := attachGrantedByKey(record.ID, dependentID)
	racingStore := &attachIncomingGrantRaceStore{
		attachTestStore: store,
		prefix:          attachGrantedByPrefix(record.ID),
		key:             reverseKey,
		value:           []byte(dependentID),
	}
	racingRepository, err := NewAttachRepository(racingStore)
	if err != nil {
		t.Fatalf("NewAttachRepository(race) error = %v", err)
	}
	if _, err := racingRepository.DeleteDetachedAttach(ctx, current); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("DeleteDetachedAttach(incoming grant race) error = %v, want state conflict", err)
	}
	stored, err := repository.GetAttach(ctx, record.ID)
	if err != nil || stored.Record.Status != core.AttachDetached {
		t.Fatalf("GetAttach(after incoming grant race) = %#v/%v", stored, err)
	}
	reverse, err := store.Get(ctx, reverseKey)
	if err != nil || reverse.Entry == nil || string(reverse.Entry.Value) != dependentID {
		t.Fatalf("incoming grant reverse membership = %#v/%v", reverse, err)
	}
}

// Rationale: an exclusion committed after the fixed read is ResourceInUse only
// when it is valid and exactly bucketed; corrupt concurrent evidence is internal.
func TestAttachLifecycleReplacementRacesBackupSourceExclusion(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	repository, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 64, "backup-source-race", nil)
	ready := createTestAttach(t, ctx, repository, scope, record, &facts)
	ready, err = advanceAttachReady(ctx, repository, ready)
	if err != nil {
		t.Fatalf("advanceAttachReady() error = %v", err)
	}
	exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
	if err != nil {
		t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
	}
	for index, test := range []struct {
		name     string
		value    []byte
		wantKind errs.Kind
	}{
		{
			name:     "valid",
			value:    testAttachBackupExclusionValue(t, scope.Environment.Record.ID, record.ID, 641),
			wantKind: errs.KindResourceInUse,
		},
		{
			name:     "malformed",
			value:    []byte("not-an-exclusion-record"),
			wantKind: errs.KindInternal,
		},
		{
			name: "misbucketed",
			value: testAttachBackupExclusionValue(
				t,
				scope.Environment.Record.ID,
				ids.NewAt(ids.KindAttach, testAttachTime.Add(4*time.Minute), 642),
				643,
			),
			wantKind: errs.KindInternal,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			raceStore := &attachBackupExclusionRaceStore{
				attachTestStore: store,
				exclusionKey:    exclusionKey,
				exclusionValue:  test.value,
			}
			raceRepository, err := NewAttachRepository(raceStore)
			if err != nil {
				t.Fatalf("NewAttachRepository(race) error = %v", err)
			}
			detaching, err := BeginAttachDetaching(
				ready.Record,
				ids.NewAt(ids.KindTask, testAttachTime.Add(4*time.Minute), int64(640+index)),
			)
			if err != nil {
				t.Fatalf("BeginAttachDetaching() error = %v", err)
			}
			if _, err = raceRepository.ReplaceLifecycle(ctx, ready, detaching); !isKind(err, test.wantKind) {
				t.Fatalf("ReplaceLifecycle(detaching %s race) error = %v, want %v", test.name, err, test.wantKind)
			}
			if _, err := store.Delete(ctx, exclusionKey); err != nil {
				t.Fatalf("Delete(%s exclusion) error = %v", test.name, err)
			}
			stored, err := repository.GetAttach(ctx, record.ID)
			if err != nil || stored.Record.Status != core.AttachReady {
				t.Fatalf("GetAttach(after exclusion race) = %#v/%v", stored, err)
			}
		})
	}
}

var testAttachTime = time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)

// Rationale: multiple Services resolved from one Environment revision share
// one desired-head compare without dropping the separate backing Environment fence.
func TestAttachDesiredHeadConditionsCoalesceOneEnvironmentRevision(t *testing.T) {
	consumerEnvironmentID := ids.NewAt(ids.KindEnvironment, testAttachTime, 700)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, testAttachTime, 701)
	consumerRevision := int64(41)
	consumerService := Versioned[ServiceRecord]{
		Record: ServiceRecord{
			EnvironmentID:   consumerEnvironmentID,
			Desired:         core.Service{ID: ids.NewAt(ids.KindService, testAttachTime, 703)},
			desiredFenceKey: environmentBlueprintHeadKey(consumerEnvironmentID),
		},
		Revision: consumerRevision,
	}
	secondConsumerService := consumerService
	secondConsumerService.Record.Desired.ID = ids.NewAt(ids.KindService, testAttachTime, 704)
	backingService := Versioned[ServiceRecord]{
		Record: ServiceRecord{
			EnvironmentID:   backingEnvironmentID,
			desiredFenceKey: environmentBlueprintHeadKey(backingEnvironmentID),
		},
		Revision: 52,
	}

	conditions, err := attachDesiredHeadConditions(
		consumerEnvironmentID,
		consumerRevision,
		backingService,
		[]Versioned[ServiceRecord]{consumerService, secondConsumerService},
	)
	if err != nil {
		t.Fatalf("attachDesiredHeadConditions() error = %v", err)
	}
	if len(conditions) != 2 {
		t.Fatalf("attachDesiredHeadConditions() count = %d, want 2", len(conditions))
	}
	if conditions[0].Key != environmentBlueprintHeadKey(consumerEnvironmentID) ||
		conditions[0].ModRevision != consumerRevision {
		t.Fatalf("consumer desired-head condition = %#v", conditions[0])
	}
	if conditions[1].Key != environmentBlueprintHeadKey(backingEnvironmentID) ||
		conditions[1].ModRevision != backingService.Revision {
		t.Fatalf("backing desired-head condition = %#v", conditions[1])
	}
}

// Rationale: coalescing must never hide a Service desired-head change observed
// at a different revision of the same Environment.
func TestAttachDesiredHeadConditionsRejectConflictingEnvironmentRevision(t *testing.T) {
	environmentID := ids.NewAt(ids.KindEnvironment, testAttachTime, 702)
	service := Versioned[ServiceRecord]{
		Record: ServiceRecord{
			EnvironmentID:   environmentID,
			desiredFenceKey: environmentBlueprintHeadKey(environmentID),
		},
		Revision: 61,
	}

	_, err := attachDesiredHeadConditions(
		environmentID,
		service.Revision+1,
		service,
		[]Versioned[ServiceRecord]{service},
	)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("attachDesiredHeadConditions() error = %v, want state conflict", err)
	}
}

type attachTestStore struct {
	*memoryHierarchyStore
}

type attachBackupExclusionRaceStore struct {
	*attachTestStore
	exclusionKey   string
	exclusionValue []byte
	injected       bool
}

type attachIncomingGrantRaceStore struct {
	*attachTestStore
	prefix   string
	key      string
	value    []byte
	injected bool
}

type attachDetachOperationCaptureStore struct {
	*attachTestStore
	attachID   string
	operations int
}

func (store *attachDetachOperationCaptureStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	for _, mutation := range mutations {
		if mutation.Type == MutationPut && mutation.Key == attachKey(store.attachID) {
			store.operations = len(conditions) + len(mutations)
			break
		}
	}
	return store.attachTestStore.Transact(ctx, conditions, mutations)
}

func (store *attachIncomingGrantRaceStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if !store.injected {
		for _, condition := range conditions {
			if !condition.Prefix || condition.Key != store.prefix {
				continue
			}
			store.injected = true
			raced, err := store.memoryHierarchyStore.Transact(ctx, nil, []Mutation{{
				Type: MutationPut, Key: store.key, Value: store.value,
			}})
			if err != nil {
				return TransactionResult{}, err
			}
			return TransactionResult{Revision: raced.Revision}, nil
		}
	}
	return store.attachTestStore.Transact(ctx, conditions, mutations)
}

func (store *attachBackupExclusionRaceStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if !store.injected {
		for _, condition := range conditions {
			if condition.Key != store.exclusionKey || condition.Prefix {
				continue
			}
			store.injected = true
			if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []Mutation{{
				Type: MutationPut, Key: store.exclusionKey, Value: store.exclusionValue,
			}}); err != nil {
				return TransactionResult{}, err
			}
			break
		}
	}
	return store.attachTestStore.Transact(ctx, conditions, mutations)
}

func testAttachBackupExclusionValue(
	t *testing.T,
	environmentID string,
	attachID string,
	seed int64,
) []byte {
	t.Helper()
	at := testAttachTime.Add(time.Duration(seed) * time.Second)
	value, err := encodeBackupSourceTargetExclusionRecord(BackupSourceTargetExclusionRecord{
		EnvironmentID: environmentID,
		OperationID:   ids.NewAt(ids.KindOperation, at, seed),
		TaskID:        ids.NewAt(ids.KindTask, at, seed+1),
		OperationKind: BackupOperationBackup,
		TargetKind:    BackupSourceTargetAttach,
		TargetID:      attachID,
		CreatedAt:     at,
		UpdatedAt:     at,
	})
	if err != nil {
		t.Fatalf("encodeBackupSourceTargetExclusionRecord() error = %v", err)
	}
	t.Cleanup(func() { clear(value) })
	return value
}

func newAttachTestStore() *attachTestStore {
	return &attachTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
}

func (store *attachTestStore) Health(context.Context) error {
	return nil
}

func (store *attachTestStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut, Key: key, Value: value}})
	return result.Revision, err
}

func (store *attachTestStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := store.Transact(ctx, nil, []Mutation{{Type: MutationDelete, Key: key}})
	return result.Revision, err
}

func (store *attachTestStore) Watch(context.Context, string, int64) (*WatchStream, error) {
	return nil, errs.New(errs.KindInternal, "Attach test store does not implement Watch")
}

func (store *attachTestStore) Snapshot(context.Context, io.Writer) error {
	return errs.New(errs.KindInternal, "Attach test store does not implement Snapshot")
}

func (store *attachTestStore) Close() error {
	return nil
}

type desiredServiceFixture struct {
	Service    Versioned[ServiceRecord]
	Projection Versioned[EnvironmentComposeProjection]
	Blueprint  Versioned[EnvironmentBlueprintRevision]
	Claim      EnvironmentBlueprintStageClaim
}

func seedDesiredServiceFixture(
	t *testing.T,
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	desired core.Service,
	backingNetworkID string,
	seed int64,
	includeRuntime bool,
	publishHead bool,
) desiredServiceFixture {
	t.Helper()
	record, err := NewServiceRecord(environmentID, desired, backingNetworkID)
	if err != nil {
		t.Fatalf("NewServiceRecord() error = %v", err)
	}
	revisionID := ids.NewAt(ids.KindTask, testAttachTime, seed)
	projection := EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
		DesiredServices: []EnvironmentServiceProjection{{
			EnvironmentID: environmentID, BackingNetworkID: backingNetworkID, Desired: desired,
		}},
	}
	canonicalYAML := []byte("services:\n  " + desired.Name + ":\n    image: " + desired.Image + "\n")
	projection.NormalizedCompose = append([]byte(nil), canonicalYAML...)
	digest := sha256.Sum256(canonicalYAML)
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId:          ids.NewAt(ids.KindConfig, testAttachTime, seed+1),
		OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:             environmentID,
		ProjectName:         "gp-" + strings.ToLower(environmentID),
		CanonicalYaml:       canonicalYAML,
		YamlSha256:          digest[:],
		AuthorizedVolumeDir: "/srv/groundplane",
		Services:            []*agentpb.ComposeService{{ServiceId: desired.ID, ComposeName: desired.Name}},
	})
	if err != nil {
		t.Fatalf("marshal Environment Compose artifact: %v", err)
	}
	dependencyDigest, err := EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatalf("EnvironmentBlueprintDependencyDigest() error = %v", err)
	}
	intentCiphertext := []byte("desired-service-fixture-intent-" + desired.ID)
	intentDigest := sha256.Sum256(intentCiphertext)
	claim := EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(revisionID, "task_"),
		EnvironmentID: environmentID, RevisionID: revisionID, TaskID: revisionID,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: http.MethodPost, Route: "/blueprints", Key: "desired-service-fixture-" + desired.ID,
		},
		Intent: ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(intentDigest[:]), Ciphertext: intentCiphertext,
		},
		SourceKind: EnvironmentBlueprintSourceApply, RenderGeneration: 1,
		ProjectionSchema: 1, CreatedAt: testAttachTime,
	}
	blueprint := EnvironmentBlueprintRevision{
		EnvironmentID: environmentID, RevisionID: revisionID,
		RootPath: "blueprint.yaml", ComposeSources: []string{"blueprint.yaml"},
		Files: []EnvironmentBlueprintFile{{Path: "blueprint.yaml", Content: canonicalYAML}}, CreatedAt: testAttachTime,
	}
	streams, err := buildEnvironmentBlueprintStreams(EnvironmentBlueprintStageRequest{
		Claim: claim, Blueprint: &blueprint, Projection: projection, DependencyDigest: dependencyDigest,
	})
	if err != nil {
		t.Fatalf("buildEnvironmentBlueprintStreams() error = %v", err)
	}
	defer clear(streams.Audit)
	defer clear(streams.Projection)
	rootValue, err := encodeEnvironmentBlueprintSeal(environmentBlueprintSealFromDescriptor(streams.Descriptor))
	if err != nil {
		t.Fatalf("encodeEnvironmentBlueprintSeal() error = %v", err)
	}
	defer clear(rootValue)
	mutations := []Mutation{{
		Type: MutationPut, Key: environmentBlueprintRootKey(environmentID, revisionID), Value: rootValue,
	}}
	if publishHead {
		headValue, encodeErr := encodeTaskReference(revisionID)
		if encodeErr != nil {
			t.Fatalf("encodeTaskReference() error = %v", encodeErr)
		}
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: environmentBlueprintHeadKey(environmentID), Value: headValue,
		})
	} else {
		descriptor := streams.Descriptor
		descriptor.State = EnvironmentBlueprintStageSealed
		descriptor.NextAuditChunk = descriptor.AuditChunks
		descriptor.NextProjectionChunk = descriptor.ProjectionChunks
		descriptorValue, encodeErr := encodeEnvironmentBlueprintStageDescriptor(descriptor)
		if encodeErr != nil {
			t.Fatalf("encodeEnvironmentBlueprintStageDescriptor() error = %v", encodeErr)
		}
		intentDigest, encodeErr := protectedBlueprintIntentDigest(claim.Intent)
		if encodeErr != nil {
			clear(descriptorValue)
			t.Fatalf("protectedBlueprintIntentDigest() error = %v", encodeErr)
		}
		locatorValue, encodeErr := encodeEnvironmentBlueprintStageLocator(claim.DescriptorID, intentDigest)
		if encodeErr != nil {
			clear(descriptorValue)
			t.Fatalf("encodeEnvironmentBlueprintStageLocator() error = %v", encodeErr)
		}
		locatorKey, _, encodeErr := environmentBlueprintLocatorKey(claim.Locator)
		if encodeErr != nil {
			clear(descriptorValue)
			clear(locatorValue)
			t.Fatalf("environmentBlueprintLocatorKey() error = %v", encodeErr)
		}
		mutations = append(mutations,
			Mutation{Type: MutationPut, Key: environmentBlueprintDescriptorKeyByID(claim.DescriptorID), Value: descriptorValue},
			Mutation{Type: MutationPut, Key: locatorKey, Value: locatorValue},
		)
	}
	for _, family := range []struct {
		id    uint8
		value []byte
	}{
		{id: EnvironmentBlueprintChunkAudit, value: streams.Audit},
		{id: EnvironmentBlueprintChunkProjection, value: streams.Projection},
	} {
		for index := uint32(0); index < chunkCount32(len(family.value)); index++ {
			from := int(index) * EnvironmentBlueprintChunkBytes
			to := min(from+EnvironmentBlueprintChunkBytes, len(family.value))
			data := family.value[from:to]
			chunkValue, encodeErr := encodeEnvironmentBlueprintChunk(EnvironmentBlueprintChunk{
				Family: family.id, Sequence: index, LogicalOffset: uint64(from),
				LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data,
			})
			if encodeErr != nil {
				t.Fatalf("encodeEnvironmentBlueprintChunk() error = %v", encodeErr)
			}
			mutations = append(mutations, Mutation{
				Type:  MutationPut,
				Key:   environmentBlueprintChunkKeyFor(environmentID, revisionID, family.id, index),
				Value: chunkValue,
			})
		}
	}
	defer clearMutationValues(mutations)
	result, err := store.Transact(ctx, nil, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed sealed desired projection = %#v, %v", result, err)
	}
	rootRevision := result.Revision
	runtimeRevision := int64(0)
	if includeRuntime {
		runtimeValue, encodeErr := encodeServiceRuntimeRecord(newServiceRuntimeRecord(record))
		if encodeErr != nil {
			t.Fatalf("encodeServiceRuntimeRecord() error = %v", encodeErr)
		}
		runtimeResult, transactErr := store.Transact(ctx, nil, []Mutation{{
			Type: MutationPut, Key: serviceRuntimeKey(desired.ID), Value: runtimeValue,
		}})
		clear(runtimeValue)
		if transactErr != nil || !runtimeResult.Succeeded {
			t.Fatalf("seed Service runtime sidecar = %#v, %v", runtimeResult, transactErr)
		}
		runtimeRevision = runtimeResult.Revision
	}
	record.desiredFenceKey = environmentBlueprintHeadKey(environmentID)
	record.runtimeRevision = runtimeRevision
	return desiredServiceFixture{
		Service: Versioned[ServiceRecord]{
			Record: record, Revision: rootRevision, ReadRevision: max(rootRevision, runtimeRevision),
		},
		Projection: Versioned[EnvironmentComposeProjection]{
			Record: projection, Revision: rootRevision, ReadRevision: rootRevision,
		},
		Blueprint: Versioned[EnvironmentBlueprintRevision]{
			Record: blueprint, Revision: rootRevision, ReadRevision: rootRevision,
		},
		Claim: claim,
	}
}

func seedAttachScope(t *testing.T, ctx context.Context, store *attachTestStore) AttachCreateScope {
	t.Helper()
	hierarchy, err := NewHierarchyRepository(store)
	if err != nil {
		t.Fatalf("NewHierarchyRepository() error = %v", err)
	}
	tenant := TenantRecord{
		ID: ids.NewAt(ids.KindTenant, testAttachTime, 1), Slug: "acme", Name: "Acme",
	}
	tenantVersion, err := hierarchy.CreateTenant(ctx, tenant)
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(ctx, ProjectRecord{
		ID: ids.NewAt(ids.KindProject, testAttachTime, 2), TenantID: tenant.ID, Slug: "app", Name: "App",
		Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	backingProject, err := hierarchy.CreateProject(ctx, ProjectRecord{
		ID: ids.NewAt(ids.KindProject, testAttachTime, 3), Slug: "postgres", Name: "Postgres",
		Kind: ProjectKindBacking,
	})
	if err != nil {
		t.Fatalf("CreateProject(backing) error = %v", err)
	}
	environmentRecord, err := NewProvisioningEnvironment(
		"/srv/groundplane",
		project.Record,
		ids.NewAt(ids.KindEnvironment, testAttachTime, 4),
		"production",
		"10.32.0.0/16",
		ids.NewAt(ids.KindTask, testAttachTime, 5),
		testAttachTime,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment() error = %v", err)
	}
	environment, err := hierarchy.CreateEnvironment(ctx, environmentRecord)
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	backingEnvironmentRecord, err := NewProvisioningEnvironment(
		"/srv/groundplane",
		backingProject.Record,
		ids.NewAt(ids.KindEnvironment, testAttachTime, 6),
		"main",
		"10.33.0.0/16",
		ids.NewAt(ids.KindTask, testAttachTime, 7),
		testAttachTime,
	)
	if err != nil {
		t.Fatalf("NewProvisioningEnvironment(backing) error = %v", err)
	}
	backingEnvironmentValue, err := encodeEnvironment(backingEnvironmentRecord)
	if err != nil {
		t.Fatalf("encodeEnvironment(backing Environment) error = %v", err)
	}
	defer clear(backingEnvironmentValue)
	backingEnvironmentResult, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: environmentKey(backingEnvironmentRecord.ID), Value: backingEnvironmentValue,
	}})
	if err != nil || !backingEnvironmentResult.Succeeded {
		t.Fatalf("seed backing Environment = %#v, %v", backingEnvironmentResult, err)
	}
	backingEnvironment := Versioned[EnvironmentRecord]{
		Record: backingEnvironmentRecord, Revision: backingEnvironmentResult.Revision,
		ReadRevision: backingEnvironmentResult.Revision,
	}
	serviceID := ids.NewAt(ids.KindService, testAttachTime, 8)
	serviceFixture := seedDesiredServiceFixture(t, ctx, store, environment.Record.ID, core.Service{
		ID: serviceID, Name: "api", Image: "example/api:1",
	}, "", 10, true, true)
	service := serviceFixture.Service
	backingServiceFixture := seedDesiredServiceFixture(t, ctx, store, backingEnvironment.Record.ID, core.Service{
		ID: ids.NewAt(ids.KindService, testAttachTime, 9), Name: "postgres", Image: "postgres:16-alpine",
		Adapter: "postgres:16",
	}, ids.NewAt(ids.KindNetwork, testAttachTime, 200), 11, true, true)
	backingService := backingServiceFixture.Service
	environment, err = hierarchy.GetEnvironment(ctx, environment.Record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment(after projection) error = %v", err)
	}
	return AttachCreateScope{
		Tenant: tenantVersion, Project: project, Environment: environment,
		DesiredHead: Versioned[EnvironmentBlueprintHead]{
			Record: EnvironmentBlueprintHead{
				EnvironmentID: environment.Record.ID,
				RevisionID:    serviceFixture.Projection.Record.RevisionID,
			},
			Revision:     serviceFixture.Projection.Revision,
			ReadRevision: serviceFixture.Projection.ReadRevision,
		},
		ComposeProjection: Versioned[EnvironmentComposeProjection]{
			Record: serviceFixture.Projection.Record, Revision: serviceFixture.Projection.Revision,
			ReadRevision: serviceFixture.Projection.ReadRevision,
		},
		Services:       []Versioned[ServiceRecord]{service},
		BackingProject: backingProject, BackingEnvironment: backingEnvironment, BackingService: backingService,
	}
}

func testPendingAttach(
	t *testing.T,
	scope AttachCreateScope,
	seed int64,
	name string,
	grants []Versioned[AttachRecord],
) (AttachRecord, AttachEncryptedFacts) {
	t.Helper()
	grantIDs := make([]string, 0, len(grants))
	factSets := []AttachFactSetMetadata{{Facts: []AttachFactDefinition{
		{Key: "pg16_DATABASE"},
		{Key: "pg16_PASSWORD", Secret: true},
		{Key: "pg16_URL", Secret: true},
	}}}
	for _, grant := range grants {
		grantIDs = append(grantIDs, grant.Record.ID)
		factSets = append(factSets, AttachFactSetMetadata{
			GrantAttachID: grant.Record.ID,
			Facts: []AttachFactDefinition{
				{Key: "pg16_DATABASE"},
				{Key: "pg16_PASSWORD", Secret: true},
				{Key: "pg16_URL", Secret: true},
			},
		})
	}
	id := ids.NewAt(ids.KindAttach, testAttachTime, seed)
	record, err := NewPendingAttachRecord(
		id,
		scope.Environment.Record.ID,
		name,
		scope.BackingProject.Record.ID,
		scope.BackingEnvironment.Record.ID,
		scope.BackingService.Record.Desired.ID,
		scope.BackingService.Record.BackingNetworkID,
		scope.Services[0].Record.Desired.ID,
		id,
		grantIDs,
		factSets,
		ids.NewAt(ids.KindTask, testAttachTime, seed+100),
		testAttachTime,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	facts, err := NewAttachEncryptedFacts(record.ID, 1, "age-x25519", "sha256", []byte("encrypted-facts"))
	if err != nil {
		t.Fatalf("NewAttachEncryptedFacts() error = %v", err)
	}
	return record, facts
}

func advanceAttachReady(
	ctx context.Context,
	repository *AttachRepository,
	current Versioned[AttachRecord],
) (Versioned[AttachRecord], error) {
	provisioning, err := MarkAttachProvisioning(current.Record, current.Record.TaskID)
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	current, err = repository.ReplaceLifecycle(ctx, current, provisioning)
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	ready, err := CompleteAttachProvisioning(current.Record, current.Record.TaskID, true)
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	return repository.ReplaceLifecycle(ctx, current, ready)
}

func createTestAttach(
	t *testing.T,
	ctx context.Context,
	repository *AttachRepository,
	scope AttachCreateScope,
	record AttachRecord,
	facts *AttachEncryptedFacts,
) Versioned[AttachRecord] {
	t.Helper()
	hierarchy, err := NewHierarchyRepository(repository.store)
	if err != nil {
		t.Fatalf("NewHierarchyRepository() error = %v", err)
	}
	scope.Environment, err = hierarchy.GetEnvironment(ctx, scope.Environment.Record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	recordDigest := sha256.Sum256([]byte(record.ID))
	seed := int64(binary.BigEndian.Uint64(recordDigest[:8]))
	planDigest := sha256.Sum256([]byte("attach-plan-" + record.ID))
	stepCount := len(record.GrantAttachIDs) + 2
	if scope.BackingService.Record.Desired.Adapter == "manual" {
		stepCount = 1
	}
	steps := make([]TaskStepRecord, stepCount)
	for index := range steps {
		steps[index] = TaskStepRecord{Kind: TaskStepOperation, ID: ids.NewAt(ids.KindStep, record.CreatedAt, seed+2+int64(index))}
	}
	owner, err := EnvironmentTaskOwner(scope.Project.Record, scope.Environment.Record)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	task := TaskRecord{
		ID:             record.TaskID,
		OperationID:    ids.NewAt(ids.KindOperation, record.CreatedAt, seed),
		IdempotencyKey: "attach-create-key-" + record.ID,
		Owner:          owner,
		Actor:          TaskActorOperator,
		Executor:       TaskExecutorAgent,
		PlanID:         ids.NewAt(ids.KindPlan, record.CreatedAt, seed+1),
		PlanHash: hex.EncodeToString(
			planDigest[:],
		),
		RenderGeneration:  int32(scope.ComposeProjection.Record.RenderGeneration),
		Type:              TaskAttach,
		Target:            record.ID,
		Params:            map[string]string{TaskMutationEnvironmentParam: record.EnvironmentID},
		Steps:             steps,
		TimeoutSeconds:    120,
		Status:            TaskStatusPending,
		NextEventSequence: 1,
		CreatedAt:         record.CreatedAt,
		UpdatedAt:         record.CreatedAt,
	}
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		t.Fatalf("json.Marshal(TaskAccepted) error = %v", err)
	}
	intentCiphertext := []byte("protected-attach-intent-" + record.ID)
	intentDigest := sha256.Sum256(intentCiphertext)
	marker := IdempotencyMarker{
		Kind: IdempotencyMarkerTask, State: IdempotencyMarkerPending,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeEnvironment, ScopeID: record.EnvironmentID,
			Method: http.MethodPost, Route: "/attaches", Key: task.IdempotencyKey,
		},
		Intent: ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(intentDigest[:]), Ciphertext: intentCiphertext,
		},
		Response: IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: responseBody,
		},
		TaskID: task.ID, CreatedAt: record.CreatedAt, UpdatedAt: record.CreatedAt,
	}
	renderInput := AttachTaskRenderInput{
		PlanID: task.PlanID, AttachID: record.ID, AttachName: record.Name,
		TenantID: scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
		ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
		EnvironmentID: record.EnvironmentID, EnvironmentName: scope.Environment.Record.Name,
		AuthorizedVolumeDir: scope.Environment.Record.VolumeDir,
		BackingServiceID:    scope.BackingService.Record.Desired.ID,
		BackingProjectID:    record.BackingProjectID,
		AdapterKey:          scope.BackingService.Record.Desired.Adapter,
		DesiredRevisionID:   scope.DesiredHead.Record.RevisionID,
		ArtifactID:          ids.NewAt(ids.KindConfig, record.CreatedAt, seed+2000),
		RenderGeneration:    scope.ComposeProjection.Record.RenderGeneration,
		Services:            attachTaskServiceSnapshots(scope.ComposeProjection.Record.DesiredServices),
		Networks:            attachTaskOwnedNetworkSnapshots(scope.ComposeProjection.Record.DesiredZones),
		Volumes:             append([]EnvironmentVolumeIdentity(nil), scope.ComposeProjection.Record.Volumes...),
		VolumeMounts:        append([]EnvironmentServiceVolumeMount(nil), scope.ComposeProjection.Record.VolumeMounts...),
		NetworkJoins: []AttachTaskNetworkJoin{{
			NetworkID: record.BackingNetworkID, ServiceIDs: []string{record.ServiceID},
		}},
		ConsumerServiceIDs: []string{record.ServiceID},
		GrantAttachIDs:     append([]string(nil), record.GrantAttachIDs...),
	}
	result, err := repository.CreateAttachWithTask(ctx, scope, record, facts, renderInput, task, marker)
	if err != nil {
		t.Fatalf("CreateAttachWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf(
			"CreateAttachWithTask().Classify() = %v, %v, %v",
			outcome,
			conflict,
			classifyErr,
		)
	}
	created, err := repository.GetAttach(ctx, record.ID)
	if err != nil {
		t.Fatalf("GetAttach(created) error = %v", err)
	}
	queued, err := repository.store.Get(ctx, taskQueueKey(TaskExecutorAgent, task.ID))
	if err != nil || queued == nil || queued.Entry == nil || queued.Entry.ModRevision != created.Revision {
		t.Fatalf("Attach Task queue = %#v, error = %v", queued, err)
	}
	storedRenderInput, err := repository.GetAttachTaskRenderInput(ctx, task.PlanID)
	if err != nil || storedRenderInput.Record.PlanID != task.PlanID || storedRenderInput.Revision != created.Revision {
		t.Fatalf("Attach Task render input = %#v, error = %v", storedRenderInput, err)
	}
	return created
}

func publishTestDetach(
	t *testing.T,
	ctx context.Context,
	repository *AttachRepository,
	scope AttachCreateScope,
	current Versioned[AttachRecord],
	createdAt time.Time,
) TaskRecord {
	t.Helper()
	hierarchy, err := NewHierarchyRepository(repository.store)
	if err != nil {
		t.Fatalf("NewHierarchyRepository() error = %v", err)
	}
	scope.Environment, err = hierarchy.GetEnvironment(ctx, scope.Environment.Record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	task := validTaskRecord(createdAt)
	owner, err := EnvironmentTaskOwner(scope.Project.Record, scope.Environment.Record)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	task.Owner = owner
	task.Actor = TaskActorOperator
	task.ID = ids.NewAt(ids.KindTask, createdAt, 901)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 902)
	task.IdempotencyKey = "attach-detach-key-0001"
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 903)
	task.RenderGeneration = int32(scope.ComposeProjection.Record.RenderGeneration)
	task.Type = TaskDetach
	task.Target = current.Record.ID
	task.Params = map[string]string{TaskMutationEnvironmentParam: current.Record.EnvironmentID}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = current.Record.EnvironmentID
	marker.Locator.Method = http.MethodDelete
	marker.Locator.Route = "/attaches/{id}"
	marker.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetAttach, ID: current.Record.ID}
	renderInput := AttachTaskRenderInput{
		PlanID: task.PlanID, AttachID: current.Record.ID, AttachName: current.Record.Name,
		TenantID: scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
		ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
		EnvironmentID: current.Record.EnvironmentID, EnvironmentName: scope.Environment.Record.Name,
		AuthorizedVolumeDir: scope.Environment.Record.VolumeDir,
		BackingServiceID:    scope.BackingService.Record.Desired.ID,
		BackingProjectID:    current.Record.BackingProjectID,
		AdapterKey:          scope.BackingService.Record.Desired.Adapter,
		DesiredRevisionID:   scope.DesiredHead.Record.RevisionID,
		ArtifactID:          ids.NewAt(ids.KindConfig, createdAt, 904),
		RenderGeneration:    scope.ComposeProjection.Record.RenderGeneration,
		Services:            attachTaskServiceSnapshots(scope.ComposeProjection.Record.DesiredServices),
		Networks:            attachTaskOwnedNetworkSnapshots(scope.ComposeProjection.Record.DesiredZones),
		Volumes:             append([]EnvironmentVolumeIdentity(nil), scope.ComposeProjection.Record.Volumes...),
		VolumeMounts:        append([]EnvironmentServiceVolumeMount(nil), scope.ComposeProjection.Record.VolumeMounts...),
		NetworkJoins:        nil,
		ConsumerServiceIDs:  []string{current.Record.ServiceID},
		GrantAttachIDs:      append([]string(nil), current.Record.GrantAttachIDs...),
	}
	result, err := repository.BeginAttachDetachWithTask(ctx, scope, current, renderInput, task, marker)
	if err != nil {
		t.Fatalf("BeginAttachDetachWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("BeginAttachDetachWithTask().Classify() = %v, %v, %v", outcome, conflict, classifyErr)
	}
	return task
}
