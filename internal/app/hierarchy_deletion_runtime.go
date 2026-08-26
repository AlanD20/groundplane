package app

import (
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func newHierarchyDeletionRuntime(
	store etcd.Store,
	idempotency *etcd.IdempotencyRepository,
	coordinator *idempotentintent.Coordinator,
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
