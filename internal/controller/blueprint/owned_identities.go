package blueprint

import (
	"slices"

	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// authoredOwnedIdentities pins birth evidence for each owned Compose resource.
// A retained ID keeps its original birth revision even if newer desired input
// supersedes a still-running Apply. Newly allocated IDs are born at this head.
func authoredOwnedIdentities(
	environmentID, revisionID string,
	generation uint64,
	current composeidentity.Snapshot,
	previous projectionrecord.EnvironmentOwnedIdentities,
	volumeSlugs map[string]string,
	entries []entryrecord.Record,
	routes []projectionrecord.EnvironmentRouteProjection,
	attaches []composeidentity.Resource,
	components []composeidentity.Resource,
	scripts []composeidentity.Resource,
) (projectionrecord.EnvironmentOwnedIdentities, error) {
	result := projectionrecord.EnvironmentOwnedIdentities{
		EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: generation,
	}
	var err error
	result.Services, err = carryOwnedIdentities(current.Services, previous.Services, revisionID, nil, nil)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	result.Networks, err = carryOwnedIdentities(current.Networks, previous.Networks, revisionID, nil, nil)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	result.Volumes, err = carryOwnedIdentities(current.Volumes, previous.Volumes, revisionID, volumeSlugs, nil)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	entryResources := make([]composeidentity.Resource, len(entries))
	entryGenerations := make(map[string]string, len(entries))
	for index, record := range entries {
		if record.EnvironmentID != environmentID {
			return projectionrecord.EnvironmentOwnedIdentities{}, errs.New(
				errs.KindInternal,
				"Entry identity has wrong Environment",
			)
		}
		entryResources[index] = composeidentity.Resource{
			ID: record.Entry.ID, Name: entryrecord.BlueprintIdentityKey(record),
		}
		entryGenerations[entryResources[index].Name] = record.CurrentValueGenerationID
	}
	result.Entries, err = carryOwnedIdentities(entryResources, previous.Entries, revisionID, nil, entryGenerations)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	routeResources := make([]composeidentity.Resource, len(routes))
	for index, route := range routes {
		if route.EnvironmentID != environmentID {
			return projectionrecord.EnvironmentOwnedIdentities{}, errs.New(
				errs.KindInternal,
				"Route identity has wrong Environment",
			)
		}
		routeResources[index] = composeidentity.Resource{
			ID:   route.Desired.ID,
			Name: projectionrecord.RouteIdentityName(route.Desired.Host, route.Desired.Path),
		}
	}
	result.Routes, err = carryOwnedIdentities(routeResources, previous.Routes, revisionID, nil, nil)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	result.Attaches, err = carryOwnedIdentities(attaches, previous.Attaches, revisionID, nil, nil)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	result.Components, err = carryOwnedIdentities(components, previous.Components, revisionID, nil, nil)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	result.Scripts, err = carryOwnedIdentities(scripts, previous.Scripts, revisionID, nil, nil)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	if err := projectionrecord.ValidateEnvironmentOwnedIdentities(result); err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	return result, nil
}

func carryOwnedIdentities(
	current []composeidentity.Resource,
	previous []projectionrecord.OwnedIdentity,
	revisionID string,
	slugs map[string]string,
	generations map[string]string,
) ([]projectionrecord.OwnedIdentity, error) {
	priorByID := make(map[string]projectionrecord.OwnedIdentity, len(previous))
	for _, identity := range previous {
		priorByID[identity.ID] = identity
	}
	result := make([]projectionrecord.OwnedIdentity, len(current))
	for index, resource := range current {
		birth := revisionID
		if retained, found := priorByID[resource.ID]; found {
			if retained.Name != resource.Name {
				return nil, errs.New(errs.KindInternal, "owned resource identity changed name")
			}
			birth = retained.BirthRevisionID
		}
		identity := projectionrecord.OwnedIdentity{
			ID: resource.ID, Name: resource.Name, BirthRevisionID: birth,
			PostgresToolsImage: resource.PostgresToolsImage,
		}
		if slugs != nil {
			volumeSlug, found := slugs[resource.Name]
			if !found {
				return nil, errs.New(errs.KindInternal, "owned Volume slug is missing")
			}
			identity.Slug = volumeSlug
		}
		if generations != nil {
			generationID, found := generations[resource.Name]
			if !found {
				return nil, errs.New(errs.KindInternal, "owned Entry generation is missing")
			}
			identity.ValueGenerationID = generationID
		}
		result[index] = identity
	}
	slices.SortFunc(result, func(left, right projectionrecord.OwnedIdentity) int {
		if left.Name < right.Name {
			return -1
		}
		if left.Name > right.Name {
			return 1
		}
		return 0
	})
	return result, nil
}
