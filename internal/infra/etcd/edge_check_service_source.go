package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/netip"
	"slices"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadTunnelCheckSource captures an ordinary Service origin without inventing a
// GP Route or inspecting remote provider ingress. The operator supplies the
// public hostname/path; the origin is selected from acknowledged GP runtime.
func (repository *RouteRepository) LoadTunnelCheckSource(ctx context.Context, environmentID, connectorID string,
	connectorRevision int64, originServiceID string, port uint16, target contract.PublicTarget,
) (EdgeCheckSource, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindComponent, connectorID) != nil ||
		ids.Validate(
			ids.KindService,
			originServiceID,
		) != nil || connectorRevision <= 0 || port == 0 || target.Validate() != nil {
		return EdgeCheckSource{}, errs.New(
			errs.KindValidationFailed,
			"Tunnel check requires its current revision, an origin Service HTTP port and a concrete public hostname/path",
		)
	}
	source, err := repository.loadEdgeCheckEnvironment(ctx, environmentID, target.Path)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	revision := source.fence.ReadRevision()
	origin, err := services.ReadJoined(ctx, repository.store, services.DesiredSelection{
		Services: environmentqueries.OrdinaryServices(
			source.projection.Record,
		), Revision: source.projection.Revision, ReadRevision: revision,
	}, originServiceID, blueprints.EnvironmentBlueprintHeadKey(environmentID))
	if err != nil {
		return EdgeCheckSource{}, err
	}
	if origin.Record.BackingNetworkID != "" || origin.Record.Desired.Adapter != "" ||
		origin.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
		return EdgeCheckSource{}, errs.New(
			errs.KindStateConflict,
			"select a running ordinary Service as the Tunnel origin",
		)
	}
	keys := []string{
		components.RecordKey(connectorID),
		components.EnvironmentCapabilityKey(environmentID, core.ComponentCapabilityEdgeTunnel),
		localagents.LocalAgentSingletonKey,
		serviceruntimerecord.Key(originServiceID),
		services.ServiceLifecycleActiveKey(originServiceID),
		deletions.TombstoneKey(
			string(deletions.DeletionTargetService),
			originServiceID,
		),
		edgechecks.LatestKey(connectorID),
	}
	read, err := repository.edgeCheckRead(ctx, keys, revision)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	for index := 0; index < 4; index++ {
		if read.Values[index] == nil {
			return EdgeCheckSource{}, errs.New(
				errs.KindStateConflict,
				"apply the Tunnel, Deploy its origin and wait for the installed Agent before checking access",
			)
		}
	}
	if read.Values[0].ModRevision != connectorRevision {
		return EdgeCheckSource{}, errs.New(
			errs.KindStateConflict,
			"Tunnel changed; refresh its configuration before checking access",
		)
	}
	if read.Values[4] != nil || read.Values[5] != nil {
		return EdgeCheckSource{}, errs.New(
			errs.KindResourceInUse,
			"origin Service has an active lifecycle operation; wait for it before checking access",
		)
	}
	connector, err := components.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	if !edgeCheckComponent(connector, environmentID, connectorID, core.ComponentKindEdgeCloudflare) ||
		string(read.Values[1].Value) != connectorID {
		return EdgeCheckSource{}, errs.New(
			errs.KindValidationFailed,
			"select the enabled Tunnel belonging to this Environment",
		)
	}
	agentID, err := localagents.DecodeLocalAgentReference(read.Values[2].Value)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	source.configuration = edgechecks.Source{
		EnvironmentRevision:    source.environment.Revision,
		BlueprintRevision:      source.projection.Revision,
		ComponentRevision:      connectorRevision,
		AgentID:                agentID,
		AgentSingletonRevision: read.Values[2].ModRevision,
		OriginRuntimeRevision:  read.Values[3].ModRevision,
		OriginIntentRevision:   services.ServiceRuntimeCondition(origin).ModRevision,
	}
	if err := repository.captureEdgeCheckConnector(ctx, &source, connector); err != nil {
		return EdgeCheckSource{}, err
	}
	runtime, err := decodeAcknowledgedServiceRuntime(read.Values[3].Value, environmentID, originServiceID)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	if runtime.Source.AgentID != agentID {
		return EdgeCheckSource{}, errs.New(errs.KindStateConflict, "Tunnel origin belongs to another Agent")
	}
	connectorZones := source.connector.Record.Component.Desired.Config.CloudflareTunnel.ZoneIDs
	address, zoneID, observation, replicas, err := serviceruntimerecord.TunnelOrigin(runtime, port, connectorZones)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	source.configuration.Zones, err = edgeCheckServiceZones(source, address, zoneID)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	digest := sha256.Sum256(runtime.Runtime.CurrentArtifact)
	source.configuration.OriginRuntimeSHA256 = hex.EncodeToString(digest[:])
	source.configuration.Request.OriginServiceID, source.configuration.Request.OriginAddress, source.configuration.Request.Target = originServiceID, address, target
	source.originTarget, source.originReplicas = observation, replicas
	source.conditions = append(
		source.conditions,
		services.ServiceDesiredCondition(origin),
		services.ServiceRuntimeCondition(origin),
	)
	for index, key := range keys {
		source.conditions = append(
			source.conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	return source, nil
}

func edgeCheckServiceZones(
	source EdgeCheckSource,
	originAddress, sharedZoneID string,
) ([]edgechecks.ZoneSource, error) {
	address, err := netip.ParseAddrPort(originAddress)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "acknowledged origin address is invalid")
	}
	ids := slices.Clone(source.connector.Record.Component.Desired.Config.CloudflareTunnel.ZoneIDs)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	zones := make([]edgechecks.ZoneSource, 0, len(ids))
	shared := false
	for _, id := range ids {
		var desired *core.Zone
		for index := range source.projection.Record.DesiredZones {
			if source.projection.Record.DesiredZones[index].Desired.ID == id {
				desired = &source.projection.Record.DesiredZones[index].Desired
				break
			}
		}
		if desired == nil {
			return nil, errs.New(
				errs.KindStateConflict,
				"serving Tunnel Zone was removed; apply its current configuration before checking access",
			)
		}
		matched := false
		for _, zone := range source.connector.Record.Zones {
			if zone.Desired == *desired {
				matched = true
				break
			}
		}
		if !matched {
			return nil, errs.New(
				errs.KindStateConflict,
				"serving Tunnel Zone differs from desired state; apply the Zone change before checking access",
			)
		}
		if id == sharedZoneID {
			subnet, err := netip.ParsePrefix(desired.Subnet)
			if err != nil || !subnet.Contains(address.Addr()) {
				return nil, errs.New(
					errs.KindStateConflict,
					"serving origin address is outside the current shared Zone; Deploy its current network configuration before checking access",
				)
			}
			shared = true
		}
		zones = append(zones, edgechecks.ZoneSource{ID: id, Revision: source.projection.Revision})
	}
	if !shared {
		return nil, errs.New(errs.KindStateConflict, "Tunnel origin has no shared applied Zone")
	}
	return zones, nil
}
