package taskplanning

import (
	"context"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveComponentFile regenerates one exact generated Component file from
// the immutable Blueprint revision and durable render projection.
func (resolver *TaskPlanResolver) ResolveComponentFile(
	ctx context.Context,
	environmentID string,
	reference materializationrecord.ComponentFileValueReference,
) ([]byte, error) {
	if ctx == nil || resolver == nil || resolver.blueprints == nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindTask, reference.RevisionID) != nil ||
		ids.Validate(ids.KindComponent, reference.ComponentID) != nil ||
		(reference.RouteTaskID != "" && ids.Validate(ids.KindTask, reference.RouteTaskID) != nil) {
		return nil, errs.New(errs.KindInternal, "Component file resolver input is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if reference.RouteTaskID != "" {
		reader, ok := resolver.blueprints.(routeRemovalPlanStateReader)
		if ok {
			stored, found, err := reader.GetRouteRemovalIntent(ctx, reference.RouteTaskID)
			if err != nil {
				return nil, err
			}
			intent := stored.Record
			if found {
				if intent.TaskID != reference.RouteTaskID || intent.EnvironmentID != environmentID ||
					intent.Status != etcd.TaskStatusPending || intent.CandidateProjection == nil ||
					intent.CandidateProjection.RevisionID != reference.RevisionID || intent.Provider == nil {
					return nil, taskmaterialization.CorruptSource()
				}
				return resolver.routeProviderFile(*intent.Provider, *intent.CandidateProjection)
			}
		}
		mutationReader, ok := resolver.blueprints.(routeMutationPlanStateReader)
		if !ok {
			return nil, errs.New(errs.KindInternal, "Route plan state reader is unavailable")
		}
		stored, found, err := mutationReader.GetRouteMutationIntent(ctx, reference.RouteTaskID)
		if err != nil {
			return nil, err
		}
		intent := stored.Record
		if !found || intent.TaskID != reference.RouteTaskID || intent.EnvironmentID != environmentID ||
			intent.Status != etcd.TaskStatusPending || intent.CandidateProjection == nil ||
			intent.CandidateProjection.RevisionID != reference.RevisionID || intent.Provider == nil {
			return nil, taskmaterialization.CorruptSource()
		}
		return resolver.routeProviderFile(*intent.Provider, *intent.CandidateProjection)
	}
	projection, found, err := resolver.blueprints.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	if !found || projection.Record.RevisionID != reference.RevisionID {
		return nil, taskmaterialization.CorruptSource()
	}
	return resolver.resolveComponentFileFromProjection(ctx, environmentID, reference, projection.Record)
}

func (resolver *TaskPlanResolver) resolveComponentFileFromProjection(
	ctx context.Context,
	environmentID string,
	reference materializationrecord.ComponentFileValueReference,
	projection projectionrecord.EnvironmentComposeProjection,
) ([]byte, error) {
	if projection.EnvironmentID != environmentID || projection.RevisionID != reference.RevisionID {
		return nil, taskmaterialization.CorruptSource()
	}
	environment, err := resolver.blueprints.GetEnvironment(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return nil, err
	}
	tenant, err := resolver.blueprints.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return nil, err
	}
	if environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		project.Record.Kind != hierarchyrecord.ProjectKindTenant {
		return nil, taskmaterialization.CorruptSource()
	}
	identity := pinnedEnvironmentIdentity{
		TenantID: tenant.Record.ID, TenantSlug: tenant.Record.Slug,
		ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug,
		EnvironmentID: environment.Record.ID, EnvironmentName: environment.Record.Name,
		AuthorizedVolumeDir: environment.Record.VolumeDir,
	}
	parsed, err := resolver.parsePinnedEnvironmentBlueprint(ctx, identity, reference.RevisionID)
	if err != nil {
		return nil, err
	}
	// The upload is an audit input, not the complete retained topology. Use
	// the same pinned normalized desired revision as runtime artifact replay.
	normalized, err := composerender.LoadNormalizedEnvironmentProject(ctx, projection)
	if err != nil {
		return nil, err
	}
	componentProjection, err := projectPinnedEnvironmentComponents(
		normalized,
		parsed.ServiceExtensions,
		identity,
		projection,
		parsed.Extensions.Routes,
		parsed.Extensions.Components,
		projectedEnvironmentEntries(projection.Entries),
		resolver.componentCatalog,
	)
	if err != nil {
		return nil, err
	}
	for _, file := range componentProjection.PlainFiles {
		if file.ComponentID == reference.ComponentID && file.Path == reference.Path {
			return append([]byte(nil), file.Content...), nil
		}
	}
	return nil, taskmaterialization.CorruptSource()
}
