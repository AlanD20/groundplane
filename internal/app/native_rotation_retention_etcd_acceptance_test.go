//go:build etcd_acceptance

package app

import (
	"bytes"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// TASK-174: real collection must prune expired native operation history without
// deleting the Environment's current encryption material or its independently
// retained request receipt. Exercise the actual collector and operation owner.
func TestNativeRotationRealEtcdCollectionKeepsCurrentKeyAndReplay(t *testing.T) {
	ctx, prefix, open := nativeEtcdAcceptanceConnector(t)
	store := open()
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	f := newNativeRotationFixture(t)
	runNativeEtcdRotation(t, ctx, prefix, open, &store, f)
	collector := newNativeRotationEtcdCollector(t, store, f)
	task, _, err := f.queue.ReadTask(ctx, f.input.TaskID, 0)
	if err != nil || task.Task.RetainUntil == nil {
		t.Fatalf("finished retention boundary: %v", err)
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(f.marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	resourceKeys := []string{backuppolicy.BackupKeyKey(f.input.EnvironmentID),
		backuppolicy.BackupKeyValueKey(f.input.EnvironmentID), markerKey}
	before, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: resourceKeys})
	if err != nil || before == nil || len(before.Values) != len(resourceKeys) {
		t.Fatalf("resource baseline: %v", err)
	}
	defer keyvalue.ClearValues(before.Values)
	for _, value := range before.Values {
		if value == nil {
			t.Fatal("resource/replay baseline is incomplete")
		}
	}
	guard, err := f.queue.GuardReplayRetention(ctx, idempotency.PruneGuardInput{
		MarkerKind: idempotency.IdempotencyMarkerTask, TaskID: f.input.TaskID, Revision: before.ReadRevision,
	})
	if err != nil || !guard.Retain {
		t.Fatalf("existing Task failed to protect its replay: %v", err)
	}
	if done, err := collector.Collect(ctx, f.input.TaskID, task.Task.RetainUntil.Add(-time.Nanosecond)); err == nil ||
		done {
		t.Fatalf("early history collection was not refused: done=%t error=%v", done, err)
	}
	unchanged, _, err := f.queue.ReadTask(ctx, f.input.TaskID, 0)
	if err != nil || unchanged.Revision != task.Revision || unchanged.Task.CollectionStartedAt != nil {
		t.Fatalf("early collection changed retained history: %v", err)
	}
	at := task.Task.RetainUntil.Add(time.Second)
	for pass := 0; pass < 8; pass++ {
		done, err := collector.Collect(ctx, f.input.TaskID, at)
		if err != nil {
			t.Fatalf("native collection pass %d: %v", pass, err)
		}
		current, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: resourceKeys})
		if err != nil || current == nil || len(current.Values) != len(resourceKeys) {
			t.Fatalf("resource check after collection: %v", err)
		}
		for index, value := range current.Values {
			if value == nil || value.ModRevision != before.Values[index].ModRevision ||
				!bytes.Equal(value.Value, before.Values[index].Value) {
				keyvalue.ClearValues(current.Values)
				t.Fatal("history collection changed current key material or request replay")
			}
		}
		keyvalue.ClearValues(current.Values)
		if done {
			break
		}
		if pass == 7 {
			t.Fatal("single-item collection exceeded its bounded passes")
		}
	}
	index, err := backupruntime.BackupKeyRotationEnvironmentIndexKey(f.input.EnvironmentID, f.input.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	absent, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		workqueue.TaskKey(f.input.TaskID), workqueue.ItemKey(f.input.WorkItemID),
		backupruntime.BackupKeyRotationKey(f.input.TaskID), index,
	}})
	if err != nil || absent == nil || len(absent.Values) != 4 {
		t.Fatalf("collected operation records: %v", err)
	}
	defer keyvalue.ClearValues(absent.Values)
	for _, value := range absent.Values {
		if value != nil {
			t.Fatal("expired native operation record survived collection")
		}
	}
	keys, err := backuppolicy.NewKeyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	current, found, err := keys.GetBackupKey(ctx, f.input.EnvironmentID)
	defer clear(current.Encrypted.Ciphertext)
	if err != nil || !found || current.Record.KeyEra != 2 || current.Record.Recipient != f.nextKey.Recipient {
		t.Fatalf("current resource read depended on collected Task history: %v", err)
	}
	guard, err = f.queue.GuardReplayRetention(ctx, idempotency.PruneGuardInput{
		MarkerKind: idempotency.IdempotencyMarkerTask, TaskID: f.input.TaskID, Revision: absent.ReadRevision,
	})
	if err != nil || guard.Retain || len(guard.Conditions) == 0 {
		t.Fatalf("collected Task did not release the independent replay-retention guard: %v", err)
	}
}

func newNativeRotationEtcdCollector(
	t *testing.T,
	store nativeEtcdStore,
	f *nativeRotationFixture,
) *operationretention.Collector {
	t.Helper()
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	cipher := nativeVolumeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	markers, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := backuppolicy.NewKeyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := backup.NewAgeBackupPolicyKeyFactory(protector)
	if err != nil {
		t.Fatal(err)
	}
	service, err := backupkey.NewService(f.repository, keys, factory, coordinator, markers, protector)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := backupkey.NewWork(service, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	connectors, err := connectorchecks.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	backings, err := backingchecks.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	dns, err := dnschecks.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	edge, err := edgechecks.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	runners, err := runnerchecks.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	images, err := imageoperations.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	hierarchy, err := etcd.NewHierarchyDeletionRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	networks, err := networkwork.NewInputRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	collector, err := operationretention.New(
		f.queue,
		connectors,
		backings,
		dns,
		edge,
		runners,
		images,
		hierarchy,
		networks,
		nil,
		operationretention.WorkRegistration{Operation: workexecution.SecretRotation, Owner: owner},
	)
	if err != nil {
		t.Fatal(err)
	}
	return collector
}
