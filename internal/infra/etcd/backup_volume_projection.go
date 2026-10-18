package etcd

import (
	"context"
	"time"

	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
)

func (repository *HierarchyRepository) ResolveVolumeRemovalImpactAtRevision(
	ctx context.Context,
	environmentID string,
	volumeID string,
	revision int64,
	policyNow time.Time,
) (backupruntime.BackupVolumeRemovalImpact, error) {
	backup, err := newBackupRuntimeRepository(repository.store)
	if err != nil {
		return backupruntime.BackupVolumeRemovalImpact{}, err
	}
	return backup.ResolveVolumeRemovalImpactAtRevision(ctx, environmentID, volumeID, revision, policyNow)
}
