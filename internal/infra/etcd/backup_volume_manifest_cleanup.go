package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumecleanup"
)

func (repository *BackupRuntimeRepository) PruneVolumeManifestBatch(ctx context.Context,
	startExclusive string,
) (string, error) {
	return backupvolumecleanup.PruneBatch(ctx, repository.store, startExclusive)
}
