package etcd

import (
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BackupRuntimeRepository struct {
	*backupruntime.Writer
	store hierarchyStore
}

type backupRuntimeOwnedEvidence struct {
	fence environmentfence.Evidence
}

func NewBackupRuntimeRepository(store etcdstore.Store) (*BackupRuntimeRepository, error) {
	return newBackupRuntimeRepository(store)
}

func newBackupRuntimeRepository(store hierarchyStore) (*BackupRuntimeRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backup runtime store is required")
	}
	return &BackupRuntimeRepository{store: store, Writer: backupruntime.NewWriter(store)}, nil
}
