package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testenvironmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func environmentMutationFenceTestOwner(at time.Time, seed int64) testenvironmentfence.Owner {
	return testenvironmentfence.Owner{
		Kind:        testbackupruntime.BackupOperationBackup,
		OperationID: ids.NewAt(ids.KindOperation, at, seed),
		TaskID:      ids.NewAt(ids.KindTask, at, seed+1),
	}
}

func putEnvironmentMutationFenceTestLock(
	t *testing.T,
	store *memoryHierarchyStore,
	environmentID string,
	owner testenvironmentfence.Owner,
) int64 {
	t.Helper()
	at := time.Date(2026, 8, 24, 20, 20, 0, 0, time.UTC)
	value, err := testbackupruntime.EncodeBackupOperationLockRecord(testbackupruntime.BackupOperationLockRecord{
		EnvironmentID: environmentID,
		OperationID:   owner.OperationID,
		TaskID:        owner.TaskID,
		Kind:          owner.Kind,
		CreatedAt:     at,
		UpdatedAt:     at,
	})
	if err != nil {
		t.Fatalf("encodeBackupOperationLockRecord() error = %v", err)
	}
	defer clear(value)
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentOperationLockKey(environmentID), Value: value,
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("put operation lock = %#v, %v", result, err)
	}
	return result.Revision
}

func advanceEnvironmentMutationFenceEpoch(
	t *testing.T,
	store *memoryHierarchyStore,
	environmentID string,
) {
	t.Helper()
	value, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(
		testbackupruntime.EnvironmentMutationEpochRecord{EnvironmentID: environmentID},
	)
	if err != nil {
		t.Fatalf("encodeEnvironmentMutationEpochRecord() error = %v", err)
	}
	defer clear(value)
	result, err := store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentMutationEpochKey(environmentID), Value: value,
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("advance mutation epoch = %#v, %v", result, err)
	}
}

func mustEnvironmentMutationEpochRevision(
	t *testing.T,
	store interface {
		Get(context.Context, string) (*testkeyvalue.GetResult, error)
	},
	environmentID string,
) int64 {
	t.Helper()
	result, err := store.Get(context.Background(), testhierarchy.EnvironmentMutationEpochKey(environmentID))
	if err != nil || result == nil || result.Entry == nil {
		t.Fatalf("get mutation epoch = %#v, %v", result, err)
	}
	if _, err := testbackupruntime.DecodeEnvironmentMutationEpochRecord(result.Entry.Value); err != nil {
		t.Fatalf("decodeEnvironmentMutationEpochRecord() error = %v", err)
	}
	return result.Entry.ModRevision
}

type entryVolumeEpochRaceStore struct {
	hierarchyStore
	beforeTransact func()
}

func (store *entryVolumeEpochRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if store.beforeTransact != nil {
		before := store.beforeTransact
		store.beforeTransact = nil
		before()
	}
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}

type entryMutationRevisionAuditStore struct {
	hierarchyStore
	anchorRevision int64
	fixedRevisions []int64
}

func (store *entryMutationRevisionAuditStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	result, err := store.hierarchyStore.GetMany(ctx, request)
	if err == nil && request.Revision == 0 && store.anchorRevision == 0 {
		store.anchorRevision = result.ReadRevision
	} else if err == nil && request.Revision > 0 {
		store.fixedRevisions = append(store.fixedRevisions, request.Revision)
	}
	return result, err
}
