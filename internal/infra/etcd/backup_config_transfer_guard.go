package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) configTransferGuard(ctx context.Context,
	owner backupconfiguration.ConfigTransferOwner, revision int64,
) ([]etcdstore.Condition, error) {
	if backupconfiguration.ValidateConfigTransferOwner(owner) != nil || revision <= 0 {
		return nil, configTransferAuthorityConflict()
	}
	inputs, err := backupwork.NewInputRepository(repository.store)
	if err != nil {
		return nil, err
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return nil, err
	}
	_, conditions, err := inputs.LoadConfigTransfer(ctx, queue, owner.Binding, revision)
	return conditions, err
}

func configTransferAuthorityConflict() error {
	return errs.New(errs.KindStateConflict, "Config transfer work attempt or captured source changed")
}
