package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Environment Component slots survive omission. Reintroduction must retain the
// slot's original birth, not publish another initial-absence proof for its ID.
// Compare the immutable birth proof beside the new head publication.
func restoreOmittedComponentBirths(
	ctx context.Context,
	store hierarchyStore,
	current projectionrecord.EnvironmentOwnedIdentities,
	readRevision int64,
) (projectionrecord.EnvironmentOwnedIdentities, []etcdstore.Condition, error) {
	result := projectionrecord.CloneEnvironmentOwnedIdentities(current)
	if len(result.Components) == 0 {
		return result, nil, nil
	}
	componentIDs := make([]string, len(result.Components))
	for index, identity := range result.Components {
		componentIDs[index] = identity.ID
	}
	ledger, err := blueprintunits.NewRepository(store)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, nil, err
	}
	births, conditions, err := ledger.CaptureComponentBirths(ctx, current.EnvironmentID, componentIDs, readRevision)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, nil, err
	}
	for index, identity := range result.Components {
		if birth, found := births[identity.ID]; found {
			result.Components[index].BirthRevisionID = birth
		}
	}
	return result, conditions, nil
}
