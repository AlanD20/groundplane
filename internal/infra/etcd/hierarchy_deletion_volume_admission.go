package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The root's coordination CAS fences this fixed-revision scan: initial Volume
// removal publication advances both ancestor epochs atomically with its lock.
// Retained locks are authoritative even when no active Task index remains.
func (repository *HierarchyDeletionRepository) requireHierarchyVolumeRemovalAbsent(
	ctx context.Context, revision int64, kind HierarchyDeletionTargetKind, targetID string,
) error {
	switch kind {
	case HierarchyDeletionTargetEnvironment:
		return repository.requireEnvironmentVolumeRemovalAbsent(ctx, revision, targetID)
	case HierarchyDeletionTargetProject, HierarchyDeletionTargetBacking:
		return repository.requireProjectVolumeRemovalAbsent(ctx, revision, targetID)
	case HierarchyDeletionTargetTenant:
		projects, err := repository.hierarchyDeletionIndexedTargets(ctx, revision,
			projectTenantOwnerPrefix(targetID), projectKey, ids.KindProject,
			func(value []byte, id, owner string) error {
				record, err := decodeProject(value)
				if err != nil || record.ID != id || record.TenantID != owner || record.Kind != ProjectKindTenant {
					return corruptHierarchyDeletion()
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
		return corruptHierarchyDeletion()
	}
}

func (repository *HierarchyDeletionRepository) requireProjectVolumeRemovalAbsent(
	ctx context.Context, revision int64, projectID string,
) error {
	environments, err := repository.hierarchyDeletionIndexedTargets(ctx, revision,
		environmentOwnerPrefix(projectID), environmentKey, ids.KindEnvironment,
		func(value []byte, id, owner string) error {
			record, err := decodeEnvironment(value)
			if err != nil || record.ID != id || record.ProjectID != owner {
				return corruptHierarchyDeletion()
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
		return corruptHierarchyDeletion()
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] != nil {
		return errs.New(errs.KindResourceInUse, "descendant Volume removal is still owned")
	}
	return nil
}
