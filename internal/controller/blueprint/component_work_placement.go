package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

type componentWorkPlacement struct {
	Selection componentWorkSelection
	Candidate componentrecord.Record
	Authority componentplanning.FrozenProjectionAuthority
	Zones     []keyvalue.Versioned[zonerecord.Record]
	Services  []core.Service
	Change    workqueue.Change
}

// prepareComponentWorkPlacement captures desired topology and the acknowledged
// predecessor separately. Existing reservations are kept through host execution;
// the returned fragment does not mutate durable state until input publication.
func (service *Service) prepareComponentWorkPlacement(ctx context.Context, work workqueue.View,
	unit blueprintunits.Unit, snapshot blueprintunits.Snapshot,
) (componentWorkPlacement, error) {
	selected, err := service.selectComponentWork(ctx, work, unit, snapshot)
	if err != nil {
		return componentWorkPlacement{}, err
	}
	source := selected.Source
	project, err := composerender.LoadNormalizedEnvironmentDesiredProject(
		ctx,
		source.desired.Record,
		source.identities.Record,
	)
	if err != nil {
		return componentWorkPlacement{}, err
	}
	identities, err := authoredOwnedIdentitySnapshot(ctx, source.desired.Record, source.identities.Record)
	if err != nil {
		return componentWorkPlacement{}, err
	}
	ownerKind, ownerID := core.ZoneOwnerEnvironment, work.Task.Task.Scope.EnvironmentID
	if source.project.Record.Kind == hierarchyrecord.ProjectKindBacking {
		ownerKind, ownerID = core.ZoneOwnerBackingProject, source.project.Record.ID
	}
	zones, err := taskplanning.ProjectZoneProjection(project, identities, ownerKind, ownerID)
	if err != nil {
		return componentWorkPlacement{}, err
	}
	versions := make([]keyvalue.Versioned[zonerecord.Record], len(zones))
	for index, zone := range zones {
		record, err := zonerecord.NewRecord(work.Task.Task.Scope.EnvironmentID, zone)
		if err != nil {
			return componentWorkPlacement{}, err
		}
		versions[index] = keyvalue.Versioned[zonerecord.Record]{Record: record,
			Revision: source.desired.Revision, ReadRevision: work.ReadRevision}
	}
	authority, err := service.repository.PrepareFrozenComponentUnitAuthority(ctx,
		componentplanning.FrozenProjectionAuthorityInput{
			EnvironmentID: work.Task.Task.Scope.EnvironmentID, ReadRevision: work.ReadRevision,
			DesiredHeadRevision: snapshot.HeadRevision, DesiredRevisionID: work.Task.Task.ID,
			DesiredZones: versions, RuntimeSourceAuthority: componentplanning.ManagedRuntimeSourceAuthorityApplied,
		}, selected.Current.Component)
	if err != nil {
		return componentWorkPlacement{}, err
	}
	services, err := taskplanning.ProjectServiceProjection(
		project,
		identities,
		source.desired.Record.Input.ServiceExtensions,
	)
	if err != nil {
		return componentWorkPlacement{}, err
	}
	if selected.Candidate.Kind != selected.Current.Component.Record.Desired.Kind &&
		selected.Candidate.Kind.IsEnvironmentProxy() {
		authority, err = service.prepareAuthoredProxyServiceCapture(
			ctx,
			work.Task.Task,
			selected.Candidate.Kind,
			authority,
			services,
		)
		if err != nil {
			return componentWorkPlacement{}, err
		}
	}
	candidate := selected.Candidate
	candidate.PinnedIPv4, candidate.Healthy = "", false
	record, change, err := service.repository.PrepareWorkPlacement(
		ctx,
		work,
		selected.Current.Component,
		candidate,
		authority,
	)
	if err != nil {
		return componentWorkPlacement{}, err
	}
	return componentWorkPlacement{Selection: selected, Candidate: record, Authority: authority,
		Zones: versions, Services: services, Change: change}, nil
}
