package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *HierarchyDeletionRepository) LoadHierarchyAttachRecovery(
	ctx context.Context,
	view workqueue.View,
) (attachremoval.RecoverySnapshot, error) {
	return attachremoval.LoadRecovery(ctx, repository.store, view)
}
