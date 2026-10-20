//go:build etcd_acceptance

package app

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// TASK-171: real etcd must atomically retain a key swap, native work outcome and
// operation receipt even when the committing client's response is lost. Reopening
// clients must finish that same Task without repeating the swap. This is not a
// server crash or an installed Controller/Console qualification.
func TestNativeRotationRealEtcdLostCommitAndReopen(t *testing.T) {
	endpoint := os.Getenv("GP_NATIVE_ETCD_ENDPOINT")
	address, err := url.Parse(endpoint)
	if err != nil || address.Scheme != "http" || address.Hostname() != "127.0.0.1" || address.Port() == "" {
		t.Fatal("GP_NATIVE_ETCD_ENDPOINT must select an isolated loopback etcd listener")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	prefix := "/native-task-acceptance/" + ids.New(ids.KindTask)
	open := func() keyvalue.Store {
		t.Helper()
		store, err := etcd.New(ctx, []string{endpoint}, prefix+"/", config.DefaultMaintenanceConfig(), t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return store
	}
	store := open()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	f := newNativeRotationFixture(t)
	seedTask, _, err := f.queue.ReadTask(ctx, f.input.TaskID, 0)
	if err != nil {
		t.Fatal(err)
	}
	scope := seedTask.Task.Scope
	// Reuse only valid hierarchy and initial encrypted key data. Capture and
	// publication run afresh against real revisions; no memory-store execution
	// record, captured revision, lock, receipt or Task is imported.
	for _, key := range []string{
		hierarchy.TenantKey(scope.TenantID), hierarchy.ProjectKey(scope.ProjectID),
		hierarchy.EnvironmentKey(scope.EnvironmentID),
		hierarchydeletion.HierarchyCoordinationKey("tenant", scope.TenantID),
		hierarchydeletion.HierarchyCoordinationKey("project", scope.ProjectID),
		hierarchy.EnvironmentMutationEpochKey(scope.EnvironmentID),
		backuppolicy.BackupKeyKey(scope.EnvironmentID), backuppolicy.BackupKeyValueKey(scope.EnvironmentID),
	} {
		seed, err := f.store.Get(ctx, key)
		if err != nil || seed.Entry == nil {
			t.Fatalf("initial fixture data unavailable: %v", err)
		}
		_, err = store.Put(ctx, key, seed.Entry.Value)
		clear(seed.Entry.Value)
		if err != nil {
			t.Fatal(err)
		}
	}
	f.repository, err = etcd.NewBackupPolicyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := f.repository.PrepareBackupKeyRotation(ctx, f.input,
		backuppolicymutations.BackupPolicyInitialKeyMaterial{
			Recipient: f.nextKey.Recipient, Ciphertext: f.nextCiphertext,
		})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Clear()
	publication, err := f.repository.PublishBackupKeyRotation(ctx, prepared, f.marker)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := publication.Classify()
	if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf("real-etcd publication: outcome=%v conflict=%v error=%v", outcome, conflict, err)
	}
	loss := &nativeRotationCommitLoss{Store: store}
	f.queue, err = workqueue.NewRepository(loss)
	if err != nil {
		t.Fatal(err)
	}
	change := f.settlement(t, workexecution.Succeeded)
	loss.armed = true
	if _, err := f.queue.Commit(ctx, change); err == nil || !loss.committed {
		t.Fatalf("committed-response loss was not exercised: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = open()
	f.queue, err = workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	f.repository, err = etcd.NewBackupPolicyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{
		backuppolicy.BackupKeyKey(scope.EnvironmentID),
		backuppolicy.BackupKeyValueKey(scope.EnvironmentID),
		backupruntime.BackupKeyRotationKey(f.input.TaskID),
		workqueue.ItemKey(f.input.WorkItemID),
		hierarchy.EnvironmentOperationLockKey(scope.EnvironmentID),
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil || read == nil || len(read.Values) != len(keys) {
		t.Fatalf("read committed authority after reconnect: %v", err)
	}
	defer keyvalue.ClearValues(read.Values)
	for index := 0; index < 4; index++ {
		if read.Values[index] == nil || read.Values[index].ModRevision != read.Values[0].ModRevision {
			t.Fatal("key halves, receipt and work result did not commit at one real revision")
		}
	}
	if read.Values[4] != nil {
		t.Fatal("completed rotation retained its operation lock")
	}
	key, err := backuppolicy.DecodeBackupKeyRecord(read.Values[0].Value)
	if err != nil || key.KeyEra != 2 || key.Recipient != f.nextKey.Recipient {
		t.Fatalf("unexpected retained key era/recipient: %v", err)
	}
	value, err := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[1].Value)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value.Ciphertext)
	if value.KeyEra != 2 || !bytes.Equal(value.Ciphertext, f.nextCiphertext) {
		t.Fatal("retained wrapped identity differs from captured key material")
	}
	stale, err := f.queue.Commit(ctx, change)
	defer keyvalue.ClearValues(stale.FailureReads)
	if err != nil || stale.Succeeded || stale.Revision != read.ReadRevision {
		t.Fatalf("stale settlement changed durable state: succeeded=%t error=%v", stale.Succeeded, err)
	}
	finished, err := f.queue.ReadFinishedWork(ctx, f.input.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.repository.PrepareBackupKeyRotationFinish(ctx, finished)
	if err != nil {
		t.Fatal(err)
	}
	at := f.input.CreatedAt.Add(2 * time.Second)
	finish, err := f.queue.PrepareTaskFinish(ctx, finished.Task, at, at.Add(workexecution.TaskHistoryRetention))
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(finish.Mutations)
	result, err := f.queue.Commit(ctx, receipt, finish)
	if err != nil || !result.Succeeded {
		t.Fatalf("same-Task finish after reconnect: %v", err)
	}
	task, _, err := f.queue.ReadTask(ctx, f.input.TaskID, 0)
	if err != nil || task.Task.FinishedAt == nil || task.Task.Outcome != workexecution.Succeeded ||
		task.Task.Progress.Succeeded != 1 {
		t.Fatalf("visible Task lost settled rotation after reconnect: %v", err)
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(f.marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	markerRead, err := store.Get(ctx, markerKey)
	if err != nil || markerRead.Entry == nil {
		t.Fatalf("durable request replay is missing: %v", err)
	}
	defer clear(markerRead.Entry.Value)
	marker, err := idempotency.DecodeIdempotencyMarker(markerRead.Entry.Value, f.marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.TaskID != f.input.TaskID || marker.State != idempotency.IdempotencyMarkerCompleted ||
		!bytes.Equal(marker.Response.Body, f.marker.Response.Body) {
		t.Fatal("finishing changed the captured response or native Task identity")
	}
	t.Logf("real-etcd native rotation retained: prefix=%s task=%s atomic_revision=%d", prefix, f.input.TaskID,
		read.Values[0].ModRevision)
}

type nativeRotationCommitLoss struct {
	keyvalue.Store
	armed, committed bool
}

func (store *nativeRotationCommitLoss) Transact(ctx context.Context, conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	result, err := store.Store.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.armed {
		store.armed, store.committed = false, true
		return keyvalue.TransactionResult{}, errors.New("injected lost real-etcd commit response")
	}
	return result, err
}
