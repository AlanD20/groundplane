package app

import (
	"bytes"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// A failed transaction must not swap either half of the key or release its
// lock. Reopening the actual repositories must finish the same captured work,
// not manufacture a replacement Task or repeat an acknowledged key swap.
func TestNativeKeyRotationAtomicSettlementAndRestart(t *testing.T) {
	f := newNativeRotationFixture(t)
	change := f.settlement(t, workexecution.Succeeded)
	f.store.failNext = true
	if _, err := f.queue.Commit(t.Context(), change); err == nil {
		t.Fatal("injected storage failure was ignored")
	}
	f.state(t, 1, workexecution.Ready, "", backupruntime.BackupKeyRotationPrepared, true)
	queue, err := workqueue.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.queue = queue
	f.repository, err = etcd.NewBackupPolicyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	change = f.settlement(t, workexecution.Succeeded)
	result, err := f.queue.Commit(t.Context(), change)
	if err != nil || !result.Succeeded {
		t.Fatalf("rotation commit=%t: %v", result.Succeeded, err)
	}
	f.state(t, 2, workexecution.Settled, workexecution.Succeeded, backupruntime.BackupKeyRotationApplied, false)
	revision := f.store.revision
	result, err = f.queue.Commit(t.Context(), change)
	if err != nil || result.Succeeded || f.store.revision != revision {
		t.Fatalf("stale settlement repeated a key swap: committed=%t error=%v", result.Succeeded, err)
	}
	keyRead, err := f.store.GetMany(t.Context(), keyvalue.GetManyRequest{Keys: []string{
		backuppolicy.BackupKeyKey(f.input.EnvironmentID), backuppolicy.BackupKeyValueKey(f.input.EnvironmentID),
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearValues(keyRead.Values)
	key, err := backuppolicy.DecodeBackupKeyRecord(keyRead.Values[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	value, err := backuppolicy.DecodeBackupKeyEncryptedValue(keyRead.Values[1].Value)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value.Ciphertext)
	if key.Recipient != f.nextKey.Recipient || !bytes.Equal(value.Ciphertext, f.nextCiphertext) ||
		keyRead.Values[0].ModRevision != keyRead.Values[1].ModRevision {
		t.Fatal("rotation did not publish the exact captured recipient and encrypted identity together")
	}
	finished, err := f.queue.ReadFinishedWork(t.Context(), f.input.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.repository.PrepareBackupKeyRotationFinish(t.Context(), finished)
	if err != nil {
		t.Fatal(err)
	}
	at := f.input.CreatedAt.Add(2 * time.Second)
	finish, err := f.queue.PrepareTaskFinish(t.Context(), finished.Task, at, at.Add(workexecution.TaskHistoryRetention))
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(finish.Mutations)
	result, err = f.queue.Commit(t.Context(), receipt, finish)
	if err != nil || !result.Succeeded {
		t.Fatalf("rotation finish=%t: %v", result.Succeeded, err)
	}
	task, _, err := f.queue.ReadTask(t.Context(), f.input.TaskID, 0)
	if err != nil || task.Task.Outcome != workexecution.Succeeded || task.Task.FinishedAt == nil ||
		task.Task.Progress.Succeeded != 1 {
		t.Fatalf("visible Task did not finish the settled rotation: %v", err)
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(f.marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := f.store.Get(t.Context(), markerKey)
	if err != nil || stored.Entry == nil {
		t.Fatalf("rotation replay unavailable: %v", err)
	}
	defer clear(stored.Entry.Value)
	marker, err := idempotency.DecodeIdempotencyMarker(stored.Entry.Value, f.marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.State != idempotency.IdempotencyMarkerCompleted || marker.TaskID != f.input.TaskID ||
		!bytes.Equal(marker.Response.Body, f.marker.Response.Body) {
		t.Fatal("completion changed the accepted replay response or lost its terminal outcome")
	}
	for _, history := range f.store.history {
		for _, version := range history {
			if bytes.Contains(version.value, []byte(f.oldKey.Identity)) ||
				bytes.Contains(version.value, []byte(f.nextKey.Identity)) {
				t.Fatal("rotation persisted an unwrapped private identity")
			}
		}
	}
}

// A lost committed response differs from a failed transaction: reopen must read
// the durable receipt and continue finishing, never generate or swap another key.
func TestNativeKeyRotationReconcilesLostCommittedReply(t *testing.T) {
	f := newNativeRotationFixture(t)
	change := f.settlement(t, workexecution.Succeeded)
	f.store.loseReply = true
	if _, err := f.queue.Commit(t.Context(), change); err == nil {
		t.Fatal("lost-response injection did not run")
	}
	var err error
	f.queue, err = workqueue.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.repository, err = etcd.NewBackupPolicyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.state(t, 2, workexecution.Settled, workexecution.Succeeded, backupruntime.BackupKeyRotationApplied, false)
	revision := f.store.revision
	finished, err := f.queue.ReadFinishedWork(t.Context(), f.input.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.repository.PrepareBackupKeyRotationFinish(t.Context(), finished); err != nil {
		t.Fatalf("reopened rotation cannot reconcile its committed receipt: %v", err)
	}
	if f.store.revision != revision {
		t.Fatal("receipt reconciliation rewrote the key")
	}
}

// Current-key revision and operation ownership must fence settlement; an
// overlapping operation or a source race must not erase a pending rotation.
func TestNativeKeyRotationRejectsOverlapAndSourceRace(t *testing.T) {
	f := newNativeRotationFixture(t)
	input := f.input
	input.TaskID, input.WorkItemID = ids.New(ids.KindTask), ids.New(ids.KindWorkItem)
	input.OperationID, input.PlanID = ids.New(ids.KindOperation), ids.New(ids.KindPlan)
	if prepared, err := f.repository.PrepareBackupKeyRotation(t.Context(), input,
		backuppolicymutations.BackupPolicyInitialKeyMaterial{Recipient: f.nextKey.Recipient, Ciphertext: f.nextCiphertext}); err == nil {
		prepared.Clear()
		t.Fatal("overlapping rotation acquired the locked Environment")
	}
	change := f.settlement(t, workexecution.Succeeded)
	key := backuppolicy.BackupKeyValueKey(f.input.EnvironmentID)
	before, err := f.store.Get(t.Context(), key)
	if err != nil || before.Entry == nil {
		t.Fatalf("current key unavailable: %v", err)
	}
	defer clear(before.Entry.Value)
	if _, err := f.store.Put(t.Context(), key, before.Entry.Value); err != nil {
		t.Fatal(err)
	}
	revision := f.store.revision
	result, err := f.queue.Commit(t.Context(), change)
	if err != nil || result.Succeeded || f.store.revision != revision {
		t.Fatalf("source revision race committed partial settlement: %t, %v", result.Succeeded, err)
	}
	f.state(t, 1, workexecution.Ready, "", backupruntime.BackupKeyRotationPrepared, true)
	view, err := f.queue.ReadWork(t.Context(), f.input.TaskID, f.input.WorkItemID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if stale, err := f.repository.PrepareBackupKeyRotationSettlement(t.Context(), view, workexecution.Succeeded,
		f.input.CreatedAt.Add(time.Second)); err == nil {
		keyvalue.ClearMutationValues(stale.Mutations)
		t.Fatal("repreparation silently selected a newer source for the captured rotation")
	}
}

// Refused/cancelled Controller work must keep the current key, discard its
// unused wrapped replacement and release the lock without inventing an attempt.
func TestNativeKeyRotationDiscardPreservesCurrentKey(t *testing.T) {
	for _, outcome := range []workexecution.Outcome{workexecution.Failed, workexecution.Cancelled, workexecution.Superseded} {
		t.Run(string(outcome), func(t *testing.T) {
			f := newNativeRotationFixture(t)
			keys := []string{
				backuppolicy.BackupKeyKey(f.input.EnvironmentID),
				backuppolicy.BackupKeyValueKey(f.input.EnvironmentID),
			}
			before, err := f.store.GetMany(t.Context(), keyvalue.GetManyRequest{Keys: keys})
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearValues(before.Values)
			result, err := f.queue.Commit(t.Context(), f.settlement(t, outcome))
			if err != nil || !result.Succeeded {
				t.Fatalf("discard commit=%t: %v", result.Succeeded, err)
			}
			f.state(t, 1, workexecution.Settled, outcome, backupruntime.BackupKeyRotationDiscarded, false)
			after, err := f.store.GetMany(t.Context(), keyvalue.GetManyRequest{Keys: keys})
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearValues(after.Values)
			for index := range keys {
				if before.Values[index].ModRevision != after.Values[index].ModRevision ||
					!bytes.Equal(before.Values[index].Value, after.Values[index].Value) {
					t.Fatal("discard changed the existing encryption key")
				}
			}
		})
	}
}
