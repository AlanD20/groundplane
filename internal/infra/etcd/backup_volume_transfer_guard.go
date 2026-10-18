package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *BackupRuntimeRepository) VolumeManifestRepository() *backupvolumemanifest.Repository {
	return backupvolumemanifest.NewRepository(repository.store, repository.volumeManifestGuard)
}

func (repository *BackupRuntimeRepository) volumeManifestGuard(ctx context.Context,
	owner backupvolumemanifest.Owner, revision int64,
) ([]keyvalue.Condition, error) {
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return nil, err
	}
	inputs, err := backupwork.NewInputRepository(repository.store)
	if err != nil {
		return nil, err
	}
	return inputs.VolumeTransferGuard(queue)(ctx, owner, revision)
}
