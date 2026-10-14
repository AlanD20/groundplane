package etcd

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type captureHistoryClient struct {
	fakeClient
	snapshot []byte
	key      string
}

func (client *captureHistoryClient) Get(
	_ context.Context,
	key string,
	_ ...clientv3.OpOption,
) (*clientv3.GetResponse, error) {
	result := &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 100}}
	if strings.HasPrefix(client.key, key) {
		result.Kvs = []*mvccpb.KeyValue{{Key: []byte(client.key), Value: client.snapshot, ModRevision: 40}}
	}
	return result, nil
}

func TestCapturePublicationCannotRaceBehindCompactionReservation(t *testing.T) {
	// HOST-17: an old-view capture prepared before the reservation must not
	// publish afterward, and an unfinished captured view must limit collection.
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	snapshot := backupconfiguration.BackupConfigSnapshotRecord{SnapshotID: ids.NewAt(ids.KindTask, now, 1),
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2), SourceID: ids.NewAt(ids.KindBackupSource, now, 3),
		State: backupconfiguration.BackupConfigSnapshotBuilding, ReadRevision: 20, CreatedAt: now, UpdatedAt: now}
	value, err := backupconfiguration.EncodeBackupConfigSnapshotRecord(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	key := backupconfiguration.BackupConfigSnapshotKey(snapshot.SnapshotID)
	backend := &captureHistoryClient{snapshot: value, key: "/groundplane" + key}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatal(err)
	}
	pin, err := store.protectedHistoryFloor(t.Context(), 100)
	if err != nil || pin == nil || pin.Revision != 20 || pin.OwnerID != snapshot.SnapshotID {
		t.Fatalf("capture floor: %+v %v", pin, err)
	}
	store.historyGate.Lock()
	result := make(chan error, 1)
	go func() {
		_, err := store.Transact(
			t.Context(),
			nil,
			[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
		)
		result <- err
	}()
	store.compactedThrough.Store(50)
	store.historyGate.Unlock()
	select {
	case err := <-result:
		if !errors.Is(err, errs.New(errs.KindCursorExpired, "")) {
			t.Fatalf("stale capture published: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publication stayed blocked")
	}
	if backend.transaction != nil {
		t.Fatal("stale captured input reached etcd mutation")
	}
	snapshot.State, snapshot.StoredManifestSHA256 = backupconfiguration.BackupConfigSnapshotSealed, strings.Repeat(
		"a",
		64,
	)
	backend.snapshot, err = backupconfiguration.EncodeBackupConfigSnapshotRecord(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	pin, err = store.protectedHistoryFloor(t.Context(), 100)
	if err != nil || pin != nil {
		t.Fatalf("sealed immutable bytes still pinned old MVCC: %+v %v", pin, err)
	}
}
