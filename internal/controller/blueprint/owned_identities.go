package blueprint

import (
	"slices"

	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
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
	attaches []attachrecord.Record,
	components []componentrecord.Record,
	scripts []scriptrecord.Record,
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
			return projectionrecord.EnvironmentOwnedIdentities{}, errs.New(errs.KindInternal, "Entry identity has wrong Environment")
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
			return projectionrecord.EnvironmentOwnedIdentities{}, errs.New(errs.KindInternal, "Route identity has wrong Environment")
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
	attachResources := make([]composeidentity.Resource, len(attaches))
	for index, attach := range attaches {
		if attach.EnvironmentID != environmentID {
			return projectionrecord.EnvironmentOwnedIdentities{}, errs.New(errs.KindInternal, "Attach identity has wrong Environment")
		}
		attachResources[index] = composeidentity.Resource{ID: attach.ID, Name: attach.Name}
	}
	result.Attaches, err = carryOwnedIdentities(attachResources, previous.Attaches, revisionID, nil, nil)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	componentResources := make([]composeidentity.Resource, len(components))
	for index, component := range components {
		if component.Desired.Owner != core.ComponentOwnerEnvironment ||
			component.Desired.OwnerID != environmentID {
			return projectionrecord.EnvironmentOwnedIdentities{}, errs.New(errs.KindInternal, "Component identity has wrong Environment")
		}
		componentResources[index] = composeidentity.Resource{
			ID: component.Desired.ID, Name: string(component.Desired.Kind),
		}
	}
	result.Components, err = carryOwnedIdentities(componentResources, previous.Components, revisionID, nil, nil)
	if err != nil {
		return projectionrecord.EnvironmentOwnedIdentities{}, err
	}
	scriptResources := make([]composeidentity.Resource, len(scripts))
	for index, script := range scripts {
		if script.EnvironmentID != environmentID {
			return projectionrecord.EnvironmentOwnedIdentities{}, errs.New(errs.KindInternal, "Script identity has wrong Environment")
		}
		name, keyErr := scriptrecord.BlueprintAuthoringKey(script)
		if keyErr != nil {
			return projectionrecord.EnvironmentOwnedIdentities{}, keyErr
		}
		scriptResources[index] = composeidentity.Resource{ID: script.Desired.ID, Name: name}
	}
	result.Scripts, err = carryOwnedIdentities(scriptResources, previous.Scripts, revisionID, nil, nil)
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
