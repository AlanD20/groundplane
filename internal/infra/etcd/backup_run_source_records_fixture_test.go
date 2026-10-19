package etcd

import (
	context "context"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func newBackupRuntimeID(kind ids.Kind, at time.Time, seed int64) string {
	return ids.NewAt(kind, at, seed)
}

func backupRuntimeSourceRecord(
	t *testing.T,
	sourceID string,
	environmentID string,
	kind string,
	targetID string,
	createdAt time.Time,
) testbackuppolicy.BackupSourceRecord {
	t.Helper()
	value, err := testrecordcodec.Encode("backup-source", map[string]any{
		"id": sourceID, "environment_id": environmentID, "kind": kind,
		"target_id": targetID, "created_at": createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	record, err := testbackuppolicy.DecodeBackupSourceRecord(value)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func putBackupRuntimeLock(
	t *testing.T,
	store *memoryHierarchyStore,
	lock testbackupruntime.BackupOperationLockRecord,
) {
	t.Helper()
	value, err := testbackupruntime.EncodeBackupOperationLockRecord(lock)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	result, err := store.Transact(
		context.Background(),
		[]testkeyvalue.Condition{{Key: testhierarchy.EnvironmentOperationLockKey(lock.EnvironmentID)}},
		[]testkeyvalue.Mutation{
			{
				Type:  testkeyvalue.MutationPut,
				Key:   testhierarchy.EnvironmentOperationLockKey(lock.EnvironmentID),
				Value: value,
			},
		},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("put backup runtime lock = %#v/%v", result, err)
	}
}

type backupRuntimeUnknownOutcomeStore struct {
	hierarchyStore
	failNext bool
}

func (store *backupRuntimeUnknownOutcomeStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	result, err := store.hierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.failNext {
		store.failNext = false
		return testkeyvalue.TransactionResult{}, errs.New(
			errs.KindStorageUnavailable,
			"unknown backup runtime outcome",
		)
	}
	return result, err
}
