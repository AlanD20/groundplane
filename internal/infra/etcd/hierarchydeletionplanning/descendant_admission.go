package hierarchydeletionplanning

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Planner) RequireDescendantsAvailable(
	ctx context.Context,
	revision int64,
	targetKind hierarchydeletion.HierarchyDeletionTargetKind,
	targetID string,
) error {
	switch targetKind {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		projects, err := repository.hierarchyDeletionIndexedTargets(
			ctx,
			revision,
			hierarchyrecord.ProjectTenantOwnerPrefix(targetID),
			hierarchyrecord.ProjectKey,
			ids.KindProject,
			func(value []byte, id, owner string) error {
				record, decodeErr := hierarchyrecord.DecodeProject(value)
				if decodeErr != nil || record.ID != id || record.TenantID != owner ||
					record.Kind != hierarchyrecord.ProjectKindTenant {
					return hierarchydeletion.CorruptHierarchyDeletion()
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
	case hierarchydeletion.HierarchyDeletionTargetProject, hierarchydeletion.HierarchyDeletionTargetBacking:
		return repository.requireProjectDeletionDescendantsAvailable(ctx, revision, targetID)
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "hierarchy deletion target kind is invalid")
	}
}

func (repository *Planner) requireProjectDeletionDescendantsAvailable(
	ctx context.Context,
	revision int64,
	projectID string,
) error {
	_, err := repository.hierarchyDeletionIndexedTargets(
		ctx,
		revision,
		hierarchyrecord.EnvironmentOwnerPrefix(projectID),
		hierarchyrecord.EnvironmentKey,
		ids.KindEnvironment,
		func(value []byte, id, owner string) error {
			record, decodeErr := hierarchyrecord.DecodeEnvironment(value)
			if decodeErr != nil || record.ID != id || record.ProjectID != owner {
				return hierarchydeletion.CorruptHierarchyDeletion()
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
