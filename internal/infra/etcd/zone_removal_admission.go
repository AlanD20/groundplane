package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func selectedByEnabledComponent(components []componentrecord.Record, zoneID string) bool {
	for _, record := range components {
		if !record.Desired.Enabled {
			continue
		}
		switch record.Desired.Kind {
		case core.ComponentKindIngressCaddy, core.ComponentKindIngressEnvoy:
			component := core.Component{Kind: record.Desired.Kind, Config: record.Desired.Config}
			if slices.Contains(component.HTTPRouterZoneIDs(), zoneID) {
				return true
			}
		case core.ComponentKindEdgeCloudflare:
			if record.Desired.Config.CloudflareTunnel != nil &&
				slices.Contains(record.Desired.Config.CloudflareTunnel.ZoneIDs, zoneID) {
				return true
			}
		}
	}
	return false
}

func validateZoneDeletionHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	zone etcdstore.Versioned[zonerecord.Record],
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || zone.Record.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID {
		return errs.New(errs.KindValidationFailed, "Zone hierarchy ownership or revisions are invalid")
	}
	return nil
}
