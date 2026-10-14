package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

// Reserve declared network identities at admission, before their Agent ensures.
// Subnet ownership is not evidence that a network has already been created.
func authoredZoneReservations(
	ctx context.Context,
	desired environmentprojection.EnvironmentDesiredInput,
	owned environmentprojection.EnvironmentOwnedIdentities,
	project hierarchy.ProjectRecord,
) ([]zones.Record, error) {
	input, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, desired, owned)
	if err != nil {
		return nil, err
	}
	identities, err := authoredOwnedIdentitySnapshot(ctx, desired, owned)
	if err != nil {
		return nil, err
	}
	ownerKind, ownerID := core.ZoneOwnerEnvironment, desired.EnvironmentID
	if project.Kind == hierarchy.ProjectKindBacking {
		ownerKind, ownerID = core.ZoneOwnerBackingProject, project.ID
	}
	declared, err := taskplanning.ProjectZoneProjection(input, identities, ownerKind, ownerID)
	if err != nil {
		return nil, err
	}
	result := make([]zones.Record, len(declared))
	for index, zone := range declared {
		result[index], err = zones.NewRecord(desired.EnvironmentID, zone)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
