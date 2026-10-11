package componentplanning

import (
	"cmp"
	"encoding/hex"
	"slices"

	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// A successful direct Route applies a new router artifact even when the
// Component's desired settings are unchanged. Retain that exact applied plan,
// rather than leaving the earlier Component Task as runtime authority.
func EncodeRouteAcknowledgedRuntime(
	taskID string, plan *agentpb.ExecutionPlan, provider *environmentchanges.RouteProviderPin,
	projection projectionrecord.EnvironmentComposeProjection,
) ([]byte, error) {
	if plan == nil || provider == nil || len(plan.GetArtifacts()) < 1 ||
		plan.GetRenderGeneration() != projection.RenderGeneration {
		return nil, errs.New(errs.KindStateConflict, "Route acknowledged runtime has no captured plan")
	}
	var selected *componentrecord.Record
	for index := range projection.Components {
		component := &projection.Components[index]
		if component.Desired.ID != provider.ComponentID {
			continue
		}
		if selected != nil || !component.Desired.Enabled ||
			len(
				component.Runtime.GeneratedServices,
			) != 1 || component.Runtime.GeneratedServices[0] != provider.ServiceID {
			return nil, errs.New(errs.KindStateConflict, "Route acknowledged router identity is ambiguous")
		}
		selected = component
	}
	if selected == nil {
		return nil, errs.New(errs.KindStateConflict, "Route acknowledged router is missing")
	}
	component, err := componentrecord.SetRuntime(*selected, selected.Runtime.GeneratedServices,
		selected.Runtime.PinnedIPv4, true)
	if err != nil {
		return nil, err
	}
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(plan.GetArtifacts()[0])
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	zones := make([]zonerecord.Record, 0, len(projection.DesiredZones))
	for _, zone := range projection.DesiredZones {
		zones = append(zones, zonerecord.Record{EnvironmentID: zone.EnvironmentID, Desired: zone.Desired})
	}
	slices.SortFunc(
		zones,
		func(left, right zonerecord.Record) int { return cmp.Compare(left.Desired.ID, right.Desired.ID) },
	)
	return EncodeRuntime(Runtime{
		EnvironmentID: projection.EnvironmentID, Component: component, RevisionID: projection.RevisionID,
		RenderGeneration: plan.GetRenderGeneration(), Artifact: artifact, Zones: zones,
		TaskID: taskID, PlanID: plan.GetPlanId(), PlanHash: hex.EncodeToString(plan.GetPlanHash()),
	})
}
