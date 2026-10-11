package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
)

// Both running-router children and disabled-router Controller settlements pin
// the same complete membership, predecessor sources and replacement images.
func (service *Service) prepareAuthoredProxyServiceCapture(ctx context.Context,
	parent etcd.TaskRecord, kind core.ComponentKind,
	authority componentplanning.FrozenProjectionAuthority, services []core.Service,
) (componentplanning.FrozenProjectionAuthority, error) {
	authority, err := service.repository.PrepareFrozenProxyServicePreflight(ctx, authority, parent.ID, kind, services)
	if err != nil {
		return componentplanning.FrozenProjectionAuthority{}, err
	}
	images, err := service.blueprintReleases.PrepareProxyReplacementImages(
		ctx,
		services,
		kind,
		authority.ProxyReplacementServices(),
	)
	if err != nil {
		return componentplanning.FrozenProjectionAuthority{}, err
	}
	return authority.WithProxyReplacementImages(services, images)
}

func (service *Service) prepareDisabledProxyServiceCapture(ctx context.Context,
	parent etcd.TaskRecord, snapshot blueprintunits.Snapshot, source authoredRuntimeProjectionSource,
	current etcd.BlueprintRuntimeComponent, kind core.ComponentKind,
) (componentplanning.FrozenProjectionAuthority, error) {
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(
		ctx,
		source.desired.Record,
		source.identities.Record,
	)
	if err != nil {
		return componentplanning.FrozenProjectionAuthority{}, err
	}
	identities, err := authoredOwnedIdentitySnapshot(ctx, source.desired.Record, source.identities.Record)
	if err != nil {
		return componentplanning.FrozenProjectionAuthority{}, err
	}
	services, err := taskplanning.ProjectServiceProjection(
		project,
		identities,
		source.desired.Record.Input.ServiceExtensions,
	)
	if err != nil {
		return componentplanning.FrozenProjectionAuthority{}, err
	}
	authority, err := service.repository.PrepareFrozenComponentUnitAuthority(ctx,
		componentplanning.FrozenProjectionAuthorityInput{
			EnvironmentID: parent.Owner.EnvironmentID, ReadRevision: snapshot.ReadRevision,
			DesiredHeadRevision: snapshot.HeadRevision, DesiredRevisionID: parent.ID,
			RuntimeSourceAuthority: componentplanning.ManagedRuntimeSourceAuthorityApplied,
		}, current.Component)
	if err != nil {
		return componentplanning.FrozenProjectionAuthority{}, err
	}
	return service.prepareAuthoredProxyServiceCapture(ctx, parent, kind, authority, services)
}
