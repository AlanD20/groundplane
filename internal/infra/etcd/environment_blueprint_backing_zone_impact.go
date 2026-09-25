package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintBackingZoneHasExternalAttaches is an admission check, not removal
// authority. The eventual Zone removal must repeat the dependency check at
// its own publication boundary so a newly created Attach cannot race Apply.
func (repository *EnvironmentBlueprintRepository) BlueprintBackingZoneHasExternalAttaches(
	ctx context.Context,
	projectID string,
	environmentID string,
	networkID string,
) (bool, error) {
	if repository == nil || repository.HierarchyRepository == nil || ctx == nil ||
		ids.Validate(ids.KindProject, projectID) != nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindNetwork, networkID) != nil {
		return false, errs.New(errs.KindValidationFailed, "Blueprint Backing Zone impact scope is invalid")
	}
	project, err := repository.GetProject(ctx, projectID)
	if err != nil {
		return false, err
	}
	environment, err := repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return false, err
	}
	if project.Record.Kind != hierarchy.ProjectKindBacking ||
		project.Record.TenantID != "" || environment.Record.ProjectID != projectID {
		return false, errs.New(errs.KindStateConflict, "Blueprint Zone is not in the Backing Project")
	}
	reader := attachrecord.NewReader(repository.store)
	attaches, err := reader.ListAttachesByBackingNetworkAtRevision(
		ctx, projectID, networkID, project.ReadRevision,
	)
	if err != nil {
		return false, err
	}
	for _, attached := range attaches {
		if attached.Record.EnvironmentID != environmentID {
			return true, nil
		}
	}
	return false, nil
}
