package app

import (
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func newHierarchyDeletionRuntime(
	store etcdstore.Store,
	authority controllerAuthorityComposition,
	backupCleanup hierarchydeletion.BackupCleanupExecutor,
) (*hierarchydeletion.Service, error) {
	journal, err := etcd.NewHierarchyDeletionRepository(store)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	clock := hierarchydeletion.SystemClock{}
	repository, err := hierarchydeletion.NewEtcdRepository(
		journal,
		authority.idempotency,
		authority.intentCoordinator,
		clock,
		backupCleanup,
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
