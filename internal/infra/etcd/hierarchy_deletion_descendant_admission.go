package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) requireHierarchyDeletionDescendantsAvailable(
	ctx context.Context,
	revision int64,
	targetKind HierarchyDeletionTargetKind,
	targetID string,
) error {
	switch targetKind {
	case HierarchyDeletionTargetTenant:
		projects, err := repository.hierarchyDeletionIndexedTargets(
			ctx,
			revision,
			projectTenantOwnerPrefix(targetID),
			projectKey,
			ids.KindProject,
			func(value []byte, id, owner string) error {
				record, decodeErr := decodeProject(value)
				if decodeErr != nil || record.ID != id || record.TenantID != owner ||
					record.Kind != ProjectKindTenant {
					return corruptHierarchyDeletion()
				}
				if record.DeletionTaskID != "" {
					return hierarchyDeletionDescendantUnavailable()
				}
				return nil
			},
			targetID,
		)
		if err != nil {
			return err
		}
		for _, project := range projects {
			if err := repository.requireProjectDeletionDescendantsAvailable(ctx, revision, project.id); err != nil {
				return err
			}
		}
		return nil
	case HierarchyDeletionTargetProject, HierarchyDeletionTargetBacking:
		return repository.requireProjectDeletionDescendantsAvailable(ctx, revision, targetID)
	case HierarchyDeletionTargetEnvironment:
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "hierarchy deletion target kind is invalid")
	}
}

func (repository *HierarchyDeletionRepository) requireProjectDeletionDescendantsAvailable(
	ctx context.Context,
	revision int64,
	projectID string,
) error {
	_, err := repository.hierarchyDeletionIndexedTargets(
		ctx,
		revision,
		environmentOwnerPrefix(projectID),
		environmentKey,
		ids.KindEnvironment,
		func(value []byte, id, owner string) error {
			record, decodeErr := decodeEnvironment(value)
			if decodeErr != nil || record.ID != id || record.ProjectID != owner {
				return corruptHierarchyDeletion()
			}
			if record.DeletionTaskID != "" {
				return hierarchyDeletionDescendantUnavailable()
			}
			return nil
		},
		projectID,
	)
	return err
}

func hierarchyDeletionDescendantUnavailable() error {
	return errs.New(errs.KindResourceInUse, "descendant hierarchy deletion is already in progress")
}
