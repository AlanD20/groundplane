package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/volumework"
)

func (repository *HierarchyRepository) ReadAppliedVolumes(
	ctx context.Context,
	volumeIDs []string,
	revision int64,
) (map[string]volumework.AppliedVolume, error) {
	volumes, err := volumework.NewInputRepository(repository.store)
	if err != nil {
		return nil, err
	}
	return volumes.ReadAppliedVolumes(ctx, volumeIDs, revision)
}
