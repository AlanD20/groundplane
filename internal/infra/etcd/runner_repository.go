package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RunnerRepository struct {
	*runnerrecord.Repository
	store hierarchyStore
}

func NewRunnerRepository(store etcdstore.Store) (*RunnerRepository, error) {
	return newRunnerRepository(store)
}

func newRunnerRepository(store hierarchyStore) (*RunnerRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "runner store is required")
	}
	return composeRunnerRepository(store), nil
}

func composeRunnerRepository(store hierarchyStore) *RunnerRepository {
	return &RunnerRepository{Repository: runnerrecord.NewRepository(store), store: store}
}
