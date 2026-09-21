package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The root's coordination CAS fences this fixed-revision scan: initial Volume
// removal publication advances both ancestor epochs atomically with its lock.
// Retained locks are authoritative even when no active Task index remains.
func (repository *HierarchyDeletionRepository) requireHierarchyVolumeRemovalAbsent(
	ctx context.Context, revision int64, kind hierarchydeletion.HierarchyDeletionTargetKind, targetID string,
) error {
	switch kind {
	case HierarchyDeletionTargetEnvironment:
		return repository.requireEnvironmentVolumeRemovalAbsent(ctx, revision, targetID)
	case hierarchydeletion.HierarchyDeletionTargetProject, HierarchyDeletionTargetBacking:
		return repository.requireProjectVolumeRemovalAbsent(ctx, revision, targetID)
	case HierarchyDeletionTargetTenant:
		projects, err := repository.hierarchyDeletionIndexedTargets(ctx, revision,
			hierarchyrecord.ProjectTenantOwnerPrefix(targetID), hierarchyrecord.ProjectKey, ids.KindProject,
			func(value []byte, id, owner string) error {
				record, err := hierarchyrecord.DecodeProject(value)
				if err != nil || record.ID != id || record.TenantID != owner || record.Kind != hierarchyrecord.ProjectKindTenant {
					return hierarchydeletion.CorruptHierarchyDeletion()
				}
				return nil
			}, targetID)
		if err != nil {
			return err
		}
		for _, project := range projects {
			if err := repository.requireProjectVolumeRemovalAbsent(ctx, revision, project.id); err != nil {
				return err
			}
		}
		return nil
	default:
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
}

func (repository *HierarchyDeletionRepository) requireProjectVolumeRemovalAbsent(
	ctx context.Context, revision int64, projectID string,
) error {
	environments, err := repository.hierarchyDeletionIndexedTargets(ctx, revision,
		hierarchyrecord.EnvironmentOwnerPrefix(projectID), hierarchyrecord.EnvironmentKey, ids.KindEnvironment,
		func(value []byte, id, owner string) error {
			record, err := hierarchyrecord.DecodeEnvironment(value)
			if err != nil || record.ID != id || record.ProjectID != owner {
				return hierarchydeletion.CorruptHierarchyDeletion()
			}
			return nil
		}, projectID)
	if err != nil {
		return err
	}
	for _, environment := range environments {
		if err := repository.requireEnvironmentVolumeRemovalAbsent(ctx, revision, environment.id); err != nil {
			return err
		}
	}
	return nil
}

func (repository *HierarchyDeletionRepository) requireEnvironmentVolumeRemovalAbsent(
	ctx context.Context, revision int64, environmentID string,
) error {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{removalrecord.EnvironmentLockKey(environmentID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] != nil {
		return errs.New(errs.KindResourceInUse, "descendant Volume removal is still owned")
	}
	return nil
}
