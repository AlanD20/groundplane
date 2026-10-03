package app

import (
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/controller/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func newHierarchyDeletionRuntime(
	store etcdstore.Store,
	authority controllerAuthorityComposition,
	execution controllerExecutionComposition,
	backupCleanup hierarchydeletion.BackupCleanupExecutor,
	cfg config.ControllerConfig,
) (*hierarchydeletion.Service, error) {
	return hierarchydeletion.NewRuntime(store, authority.idempotency, authority.intentCoordinator,
		backupCleanup, cfg.Storage.VolumeRoot, execution.attachFactValues, execution.planResolver)
}
