package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumBackupRuntimeTransactionBytes = 768 << 10

type BackupRuntimeRepository struct {
	store hierarchyStore
}

type backupRuntimeOwnedEvidence struct {
	fence environmentMutationFenceEvidence
}

func NewBackupRuntimeRepository(store etcdstore.Store) (*BackupRuntimeRepository, error) {
	return newBackupRuntimeRepository(store)
}

func newBackupRuntimeRepository(store hierarchyStore) (*BackupRuntimeRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backup runtime store is required")
	}
	return &BackupRuntimeRepository{store: store}, nil
}

func (repository *BackupRuntimeRepository) GetBackupRun(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[BackupRunRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[BackupRunRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, taskID); err != nil {
		return etcdstore.Versioned[BackupRunRecord]{}, err
	}
	primaryKey := backupRunKey(taskID)
	result, err := repository.store.Get(ctx, primaryKey)
	if err != nil {
		return etcdstore.Versioned[BackupRunRecord]{}, err
	}
	if result == nil {
		return etcdstore.Versioned[BackupRunRecord]{}, errs.New(errs.KindInternal, "backup run read is empty")
	}
	if result.Entry == nil {
		return etcdstore.Versioned[BackupRunRecord]{}, errs.New(
			errs.KindTaskNotFound,
			"backup run was not found",
		)
	}
	defer clear(result.Entry.Value)
	record, err := decodeBackupRunRecord(result.Entry.Value)
	if err != nil || record.TaskID != taskID {
		return etcdstore.Versioned[BackupRunRecord]{}, corruptBackupRuntimeRecord()
	}
	membershipKey, err := backupRunEnvironmentIndexKey(record.EnvironmentID, taskID)
	if err != nil {
		return etcdstore.Versioned[BackupRunRecord]{}, corruptBackupRuntimeRecord()
	}
	authority, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{primaryKey, membershipKey}, Revision: result.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[BackupRunRecord]{}, err
	}
	if authority == nil || authority.ReadRevision != result.ReadRevision ||
		len(authority.Values) != 2 || authority.Values[0] == nil || authority.Values[1] == nil {
		return etcdstore.Versioned[BackupRunRecord]{}, corruptBackupRuntimeRecord()
	}
	defer clearKeyValues(authority.Values)
	stored, err := decodeBackupRunRecord(authority.Values[0].Value)
	if err != nil || !backupRunRecordsEqual(stored, record) ||
		authority.Values[0].ModRevision != result.Entry.ModRevision ||
		authority.Values[1].Key != membershipKey || authority.Values[1].Version != 1 ||
		authority.Values[1].ModRevision > authority.Values[0].ModRevision ||
		string(authority.Values[1].Value) != taskID {
		return etcdstore.Versioned[BackupRunRecord]{}, corruptBackupRuntimeRecord()
	}
	return etcdstore.Versioned[BackupRunRecord]{
		Record:       stored,
		Revision:     authority.Values[0].ModRevision,
		ReadRevision: authority.ReadRevision,
	}, nil
}
