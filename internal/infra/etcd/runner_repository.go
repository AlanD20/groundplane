package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RunnerFilter struct {
	TenantID  string
	ProjectID string
}

type RunnerRepository struct {
	store hierarchyStore
}

func NewRunnerRepository(store etcdstore.Store) (*RunnerRepository, error) {
	return newRunnerRepository(store)
}

func newRunnerRepository(store hierarchyStore) (*RunnerRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "runner store is required")
	}
	return &RunnerRepository{store: store}, nil
}
