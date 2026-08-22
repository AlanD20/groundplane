package controller

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveComponentFile regenerates one exact generated Component file from
// the immutable Blueprint revision and durable render projection.
func (resolver *TaskPlanResolver) ResolveComponentFile(
	ctx context.Context,
	environmentID string,
	reference etcd.TaskComponentFileValueReference,
) ([]byte, error) {
	if ctx == nil || resolver == nil || resolver.blueprints == nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindTask, reference.RevisionID) != nil ||
		ids.Validate(ids.KindComponent, reference.ComponentID) != nil {
		return nil, errs.New(errs.KindInternal, "Component file resolver input is invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
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
	projection, found, err := resolver.blueprints.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	if !found || projection.Record.BlueprintRevisionID != reference.RevisionID ||
		environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady ||
		project.Record.Kind != etcd.ProjectKindTenant {
		return nil, corruptMaterializationSource()
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
	componentProjection, err := projectPinnedEnvironmentComponents(
		parsed.Project,
		identity,
		projection.Record,
		parsed.Extensions.Routes,
		parsed.Extensions.Components,
		projectedEnvironmentEntries(projection.Record.Entries),
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
	return nil, corruptMaterializationSource()
}
