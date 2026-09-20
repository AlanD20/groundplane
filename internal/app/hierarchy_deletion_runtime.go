package app

import (
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func newHierarchyDeletionRuntime(
	store etcdstore.Store,
	idempotency *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
) (*hierarchydeletion.Service, error) {
	journal, err := etcd.NewHierarchyDeletionRepository(store)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	clock := hierarchydeletion.SystemClock{}
	repository, err := hierarchydeletion.NewEtcdRepository(
		journal,
		idempotency,
		coordinator,
		clock,
	)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return hierarchydeletion.NewService(
		repository,
		repository,
		hierarchydeletion.StableIDGenerator{},
		clock,
	), nil
}
