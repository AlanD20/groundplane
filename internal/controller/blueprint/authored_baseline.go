package blueprint

import (
	"context"
	"math"
	"slices"

	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// authoredApplyBaseline never requires a rendered predecessor. The current
// head, complete normalized input and stable owned identities must all name
// the same revision; publication rechecks the head's etcd revision.
type authoredApplyBaseline struct {
	environment          etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	project              etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	tenant               etcdstore.Versioned[hierarchyrecord.TenantRecord]
	taskOwner            taskjournal.TaskOwner
	previousInput        etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]
	previousIdentities   etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities]
	hasHead              bool
	expectedHeadRevision int64
	previous             composeidentity.Snapshot
	generation           uint64
}

func (service *Service) loadAuthoredApplyBaseline(
	ctx context.Context, environmentID, expectedRevision string,
) (authoredApplyBaseline, error) {
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return authoredApplyBaseline{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return authoredApplyBaseline{}, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return authoredApplyBaseline{}, err
	}
	owner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return authoredApplyBaseline{}, err
	}
	if environment.Record.ProjectID != project.Record.ID || project.Record.TenantID != tenant.Record.ID ||
		project.Record.Kind != hierarchyrecord.ProjectKindTenant {
		return authoredApplyBaseline{}, errs.New(errs.KindInternal, "Environment hierarchy is inconsistent")
	}
	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, environmentID)
	if err != nil {
		return authoredApplyBaseline{}, err
	}
	if expectedRevision != "" && expectedRevision != environmentBlueprintRevision(head, hasHead) {
		return authoredApplyBaseline{}, errs.New(
			errs.KindStateConflict, "Environment Blueprint changed after the authoring revision was loaded",
		)
	}
	baseline := authoredApplyBaseline{
		environment: environment, project: project, tenant: tenant, taskOwner: owner,
		hasHead: hasHead, expectedHeadRevision: head.Revision, generation: 1,
	}
	if !hasHead {
		return baseline, nil
	}
	if head.Record.EnvironmentID != environmentID || head.Revision <= 0 {
		return authoredApplyBaseline{}, errs.New(errs.KindInternal, "Environment desired head is corrupt")
	}
	desired, found, err := service.repository.GetEnvironmentDesiredInput(ctx, environmentID)
	if err != nil {
		return authoredApplyBaseline{}, err
	}
	if !found {
		return authoredApplyBaseline{}, errs.New(errs.KindStateConflict, "Environment desired input is missing")
	}
	identities, found, err := service.repository.GetEnvironmentOwnedIdentities(ctx, environmentID)
	if err != nil {
		return authoredApplyBaseline{}, err
	}
	if !found || desired.Revision != head.Revision || identities.Revision != head.Revision ||
		desired.Record.EnvironmentID != environmentID || identities.Record.EnvironmentID != environmentID ||
		desired.Record.RevisionID != head.Record.RevisionID ||
		identities.Record.RevisionID != head.Record.RevisionID ||
		desired.Record.RenderGeneration != identities.Record.RenderGeneration ||
		desired.Record.RenderGeneration >= math.MaxInt32 {
		return authoredApplyBaseline{}, errs.New(errs.KindStateConflict, "Environment desired baseline changed")
	}
	previous, err := authoredOwnedIdentitySnapshot(ctx, desired.Record, identities.Record)
	if err != nil {
		return authoredApplyBaseline{}, err
	}
	baseline.previousInput = desired
	baseline.previousIdentities = identities
	baseline.previous = previous
	baseline.generation = desired.Record.RenderGeneration + 1
	return baseline, nil
}

func authoredOwnedIdentitySnapshot(
	ctx context.Context,
	desired projectionrecord.EnvironmentDesiredInput,
	identities projectionrecord.EnvironmentOwnedIdentities,
) (composeidentity.Snapshot, error) {
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(ctx, desired, identities)
	if err != nil {
		return composeidentity.Snapshot{}, err
	}
	serviceNames, err := composeidentity.OwnedServiceNames(project)
	if err != nil {
		return composeidentity.Snapshot{}, err
	}
	networkNames, err := composeidentity.OwnedNetworkNames(project)
	if err != nil {
		return composeidentity.Snapshot{}, err
	}
	volumeNames, err := composeidentity.OwnedVolumeNames(project)
	if err != nil {
		return composeidentity.Snapshot{}, err
	}
	entryNames := make([]string, 0, len(desired.Input.Entries))
	for name := range desired.Input.Entries {
		entryNames = append(entryNames, name)
	}
	slices.Sort(entryNames)
	routeNames := make([]string, len(desired.Input.Routes))
	for index, route := range desired.Input.Routes {
		routeNames[index] = projectionrecord.RouteIdentityName(route.Hostname, route.Path)
	}
	slices.Sort(routeNames)
	if !sameOwnedNames(serviceNames, identities.Services) ||
		!sameOwnedNames(networkNames, identities.Networks) ||
		!sameOwnedNames(volumeNames, identities.Volumes) ||
		!sameOwnedNames(entryNames, identities.Entries) ||
		!sameOwnedNames(routeNames, identities.Routes) {
		return composeidentity.Snapshot{}, errs.New(errs.KindInternal, "Environment desired identity names are inconsistent")
	}
	previous := composeidentity.Snapshot{
		Services: make([]composeidentity.Resource, len(identities.Services)),
		Networks: make([]composeidentity.Resource, len(identities.Networks)),
		Volumes:  make([]composeidentity.Resource, len(identities.Volumes)),
	}
	for index, identity := range identities.Services {
		previous.Services[index] = composeidentity.Resource{ID: identity.ID, Name: identity.Name}
	}
	for index, identity := range identities.Networks {
		previous.Networks[index] = composeidentity.Resource{ID: identity.ID, Name: identity.Name}
	}
	for index, identity := range identities.Volumes {
		previous.Volumes[index] = composeidentity.Resource{ID: identity.ID, Name: identity.Name}
	}
	return previous, nil
}

func sameOwnedNames(names []string, identities []projectionrecord.OwnedIdentity) bool {
	return slices.EqualFunc(names, identities, func(name string, identity projectionrecord.OwnedIdentity) bool {
		return name == identity.Name
	})
}
