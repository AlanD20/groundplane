package workqueue

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// TASK-16: stopped cleanup must release worker capacity without releasing its
// resource/host-effect claims. An explicit captured retry survives reloading,
// cannot replay a transaction into a second attempt, and never erases the
// original failure when cleanup eventually succeeds. Atomic-CAS storage proof;
// operation authorization, public request replay and host effects are not run.
func TestNativeStoppedRecoveryRetainsClaimsAndRetryIdentity(t *testing.T) {
	store := &assignmentStore{revision: 1, values: make(map[string]keyvalue.KeyValue)}
	initial := assignmentFixture(t, store, workexecution.Release, nil)
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	at := initial.At.Add(-time.Second)
	taskID, workID := initial.Task.Task.ID, initial.Item.Item.ID
	commit := func(change Change) {
		t.Helper()
		result, err := repository.Commit(t.Context(), change)
		if err != nil || !result.Succeeded {
			t.Fatalf("native recovery transaction committed=%t: %v", result.Succeeded, err)
		}
	}
	state := func() (TaskSnapshot, ItemSnapshot) {
		t.Helper()
		task, revision := recoveryRecord(t, store, TaskKey(taskID), DecodeTask)
		item, itemRevision := recoveryRecord(t, store, ItemKey(workID), DecodeItem)
		return TaskSnapshot{Task: task, Revision: revision}, ItemSnapshot{Item: item, Revision: itemRevision}
	}
	start := func(direction workexecution.Direction, second int, slot uint16) workexecution.Attempt {
		t.Helper()
		task, item := state()
		host, err := readHostEffect(recoveryValue(store, HostEffectKey(item.Item.Placement.HostID, workID)), item.Item,
			direction == workexecution.Recovery)
		if err != nil {
			t.Fatal(err)
		}
		request := Assignment{Task: task, Item: item, Executor: initial.Executor,
			Claims: recoveryClaims(t, store, item.Item), HostEffect: host,
			QueueRevision: store.values[QueueKey(item.Item)].ModRevision, Slot: slot,
			AttemptID: ids.New(ids.KindAttempt), Direction: direction,
			At: at.Add(time.Duration(second) * time.Second), Deadline: item.Item.Deadline}
		change, attempt, err := PrepareAssignment(request)
		if err != nil {
			t.Fatal(err)
		}
		commit(change)
		task, item = state()
		attempt, revision := recoveryRecord(t, store, AttemptKey(attempt.ID), DecodeAttempt)
		change, err = PrepareStart(task, item, AttemptSnapshot{Attempt: attempt, Revision: revision},
			initial.Executor, request.At.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		commit(change)
		attempt, _ = recoveryRecord(t, store, AttemptKey(attempt.ID), DecodeAttempt)
		return attempt
	}
	finish := func(attempt workexecution.Attempt, second int, result workexecution.Completion, automaticCleanup bool, slot uint16) {
		t.Helper()
		task, item := state()
		attempt, revision := recoveryRecord(t, store, AttemptKey(attempt.ID), DecodeAttempt)
		worker, workerRevision := recoveryRecord(
			t,
			store,
			SlotKey(attempt.Executor, slot),
			func(raw []byte) (Slot, error) {
				return recordcodec.Decode[Slot](raw, "execution_slot")
			},
		)
		host, err := readHostEffect(
			recoveryValue(store, HostEffectKey(item.Item.Placement.HostID, workID)),
			item.Item,
			true,
		)
		if err != nil {
			t.Fatal(err)
		}
		request := Completion{Task: task, Item: item, Attempt: AttemptSnapshot{Attempt: attempt, Revision: revision},
			Slot:   SlotSnapshot{Index: slot, Record: worker, Revision: workerRevision},
			Claims: recoveryClaims(t, store, item.Item), HostEffect: host, Result: result,
			At: at.Add(time.Duration(second) * time.Second)}
		var change Change
		if automaticCleanup {
			change, err = PrepareCompletionWithRecovery(request, at.Add(20*time.Second))
		} else {
			change, err = PrepareCompletion(request)
		}
		if err != nil {
			t.Fatal(err)
		}
		commit(change)
	}
	input := *initial.Item.Item.Input
	failure := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("b", 64)}
	cleanupFailure := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("c", 64)}
	forward := start(workexecution.Forward, 1, 0)
	finish(forward, 3, workexecution.Completion{Outcome: workexecution.Failed, Evidence: failure}, true, 0)
	cleanup := start(workexecution.Recovery, 4, 0)
	finish(cleanup, 6, workexecution.Completion{Outcome: workexecution.Failed, Evidence: cleanupFailure}, false, 0)
	task, item := state()
	if item.Item.Phase != workexecution.Attention || item.Item.Cleanup != workexecution.CleanupStopped ||
		task.Task.Progress.Attention != 1 || item.Item.FirstFailure == nil || *item.Item.FirstFailure != failure {
		t.Fatal("failed cleanup did not stop while retaining the original failure")
	}
	if _, exists := store.values[SlotKey(initial.Executor.Record.Identity, 0)]; exists {
		t.Fatal("stopped cleanup retained worker capacity")
	}
	claimKey := ClaimKey(item.Item.Claims[0])
	claimBefore, hostBefore := store.values[claimKey], store.values[HostEffectKey(item.Item.Placement.HostID, workID)]
	if claimBefore.ModRevision == 0 || hostBefore.ModRevision == 0 {
		t.Fatal("stopped cleanup released unresolved resource or host-effect ownership")
	}
	unrelated := assignmentFixture(t, store, workexecution.Release, &initial.Executor)
	unrelated.At = at.Add(7 * time.Second)
	change, _, err := PrepareAssignment(unrelated)
	if err != nil {
		t.Fatal(err)
	}
	commit(change)
	if current := store.values[claimKey]; current.ModRevision != claimBefore.ModRevision ||
		!bytes.Equal(current.Value, claimBefore.Value) {
		t.Fatal("unrelated assignment changed stopped cleanup ownership")
	}
	// Reconstruct all retry inputs from stored records through a new repository.
	repository, err = NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	task, item = state()
	previous, previousRevision := recoveryRecord(t, store, AttemptKey(cleanup.ID), DecodeAttempt)
	decision := workexecution.RetryDecision{Direction: workexecution.Recovery, Result: cleanupFailure,
		NotBefore: at.Add(9 * time.Second), Deadline: at.Add(30 * time.Second)}
	retry, err := PrepareRetry(
		task,
		item,
		AttemptSnapshot{Attempt: previous, Revision: previousRevision},
		decision,
		at.Add(8*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	commit(retry)
	duplicate, err := repository.Commit(t.Context(), retry)
	if err != nil || duplicate.Succeeded {
		t.Fatalf("retry transaction replay created another permission: %v", err)
	}
	task, item = state()
	if item.Item.Phase != workexecution.Waiting || item.Item.AttemptNumber != 2 || item.Item.ActiveAttemptID != "" ||
		task.Task.ID != taskID || task.Task.Intent != initial.Task.Task.Intent || task.Task.Scope != initial.Task.Task.Scope ||
		*item.Item.Input != input || item.Item.FirstFailure == nil || *item.Item.FirstFailure != failure ||
		item.Item.FirstFailureAttempt != 1 || item.Item.Result == nil || *item.Item.Result != failure {
		t.Fatal("explicit retry replaced captured intent, the original failure or attempt identity")
	}
	if current := store.values[claimKey]; current.ModRevision != claimBefore.ModRevision ||
		!bytes.Equal(current.Value, claimBefore.Value) {
		t.Fatal("queued retry released or replaced unresolved resource ownership")
	}
	wake, err := PrepareRetryReady(task, item, store.values[QueueKey(item.Item)].ModRevision, at.Add(9*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	commit(wake)
	// Branch from the same persisted retry: expiry must stop it without a new
	// attempt or lost claims. The original branch below completes normally.
	task, item = state()
	expiry, err := PrepareCleanupRetryExpiry(task, item, decision.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	expiredStore := &assignmentStore{revision: store.revision, values: make(map[string]keyvalue.KeyValue)}
	for key, value := range store.values {
		value.Value = bytes.Clone(value.Value)
		expiredStore.values[key] = value
	}
	expiredRepository, err := NewRepository(expiredStore)
	if err != nil {
		t.Fatal(err)
	}
	expiredResult, err := expiredRepository.Commit(t.Context(), expiry)
	if err != nil || !expiredResult.Succeeded {
		t.Fatalf("cleanup retry expiry did not commit: %v", err)
	}
	expiredItem, _ := recoveryRecord(t, expiredStore, ItemKey(workID), DecodeItem)
	if expiredItem.Phase != workexecution.Attention || expiredItem.Cleanup != workexecution.CleanupStopped ||
		expiredItem.ActiveAttemptID != "" || expiredItem.AttemptNumber != 2 ||
		expiredItem.FirstFailure == nil || *expiredItem.FirstFailure != failure || *expiredItem.Input != input {
		t.Fatal("expiry created execution permission or changed captured recovery authority")
	}
	for _, key := range []string{claimKey, HostEffectKey(item.Item.Placement.HostID, workID),
		AttemptKey(forward.ID), AttemptKey(cleanup.ID), SlotKey(initial.Executor.Record.Identity, 0)} {
		before, after := store.values[key], expiredStore.values[key]
		if before.ModRevision == 0 || after.ModRevision != before.ModRevision ||
			!bytes.Equal(after.Value, before.Value) {
			t.Fatal("expiry released claims, changed an attempt or consumed unrelated capacity")
		}
	}
	if _, exists := expiredStore.values[SlotKey(initial.Executor.Record.Identity, 1)]; exists {
		t.Fatal("expired retry occupied a worker slot")
	}
	last := start(workexecution.Recovery, 10, 1)
	proof := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("d", 64)}
	finish(
		last,
		12,
		workexecution.Completion{Outcome: workexecution.Succeeded, Evidence: proof, EffectsSettled: true},
		false,
		1,
	)
	task, item = state()
	if item.Item.Phase != workexecution.Settled || item.Item.Outcome != workexecution.Failed ||
		item.Item.Cleanup != workexecution.CleanupComplete || task.Task.Progress.Failed != 1 ||
		item.Item.CleanupResult == nil || *item.Item.CleanupResult != proof ||
		item.Item.FirstFailure == nil || *item.Item.FirstFailure != failure || *item.Item.Input != input {
		t.Fatal("successful cleanup rewrote the original failed operation")
	}
	for _, key := range []string{claimKey, HostEffectKey(item.Item.Placement.HostID, workID), SlotKey(initial.Executor.Record.Identity, 1)} {
		if _, exists := store.values[key]; exists {
			t.Fatal("proven cleanup retained execution ownership")
		}
	}
	if _, exists := store.values[SlotKey(initial.Executor.Record.Identity, 0)]; !exists {
		t.Fatal("cleanup released the unrelated operation's worker slot")
	}
	original, _ := recoveryRecord(t, store, AttemptKey(forward.ID), DecodeAttempt)
	if original.Outcome != workexecution.Failed || original.Result == nil || *original.Result != failure {
		t.Fatal("cleanup changed the original forward attempt")
	}
	lateExpiry, err := repository.Commit(t.Context(), expiry)
	if err != nil || lateExpiry.Succeeded {
		t.Fatal("stale expiry overwrote completed cleanup")
	}
}

func recoveryRecord[T any](
	t *testing.T,
	store *assignmentStore,
	key string,
	decode func([]byte) (T, error),
) (T, int64) {
	t.Helper()
	value, found := store.values[key]
	if !found {
		t.Fatalf("native recovery record is missing: %s", key)
	}
	record, err := decode(value.Value)
	if err != nil {
		t.Fatal(err)
	}
	return record, value.ModRevision
}

func recoveryValue(store *assignmentStore, key string) *keyvalue.KeyValue {
	value, found := store.values[key]
	if !found {
		return nil
	}
	return &value
}

func recoveryClaims(t *testing.T, store *assignmentStore, item workexecution.Item) []ClaimSnapshot {
	t.Helper()
	claims := make([]ClaimSnapshot, len(item.Claims))
	for index, claim := range item.Claims {
		claims[index].Claim = claim
		value := recoveryValue(store, ClaimKey(claim))
		if value == nil {
			continue
		}
		owner, err := DecodeClaimOwner(value.Value)
		if err != nil {
			t.Fatal(err)
		}
		claims[index].Owner, claims[index].Revision = &owner, value.ModRevision
	}
	return claims
}
