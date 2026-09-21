package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupretention"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BackupRuntimeRepository struct {
	*backupruntime.Writer
	*backupplanning.Planner
	*backupretention.Repository
	store hierarchyStore
}

func NewBackupRuntimeRepository(store etcdstore.Store) (*BackupRuntimeRepository, error) {
	return newBackupRuntimeRepository(store)
}

func newBackupRuntimeRepository(store hierarchyStore) (*BackupRuntimeRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backup runtime store is required")
	}
	return &BackupRuntimeRepository{store: store, Writer: backupruntime.NewWriter(store), Planner: backupplanning.NewPlanner(store), Repository: backupretention.NewRepository(store)}, nil
}
