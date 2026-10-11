package etcd

import (
	"context"
	"encoding/hex"
	"net/netip"
	"slices"
	"strings"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *RouteRepository) captureEdgeCheckRuntime(
	ctx context.Context,
	source *EdgeCheckSource,
	desiredConnector components.Record,
	route environmentprojection.EnvironmentRouteProjection,
	observation routes.ObservationRecord,
) error {
	if err := repository.captureEdgeCheckConnector(ctx, source, desiredConnector); err != nil {
		return err
	}
	revision := source.fence.ReadRevision()
	environmentID := source.environment.Record.ID
	connector := source.connector
	routerID := source.configuration.RouterComponentID
	router, found, err := componentplanning.ReadRuntime(ctx, repository.store, routerID, revision)
	if err != nil {
		return err
	}
	kind := router.Record.Component.Desired.Kind
	if !found || !kind.IsEnvironmentProxy() ||
		!edgeCheckComponent(router.Record.Component, environmentID, routerID, kind) ||
		router.Record.RenderGeneration != observation.Observation.Provider.InputGeneration {
		return errs.New(errs.KindStateConflict, "Route router does not match its acknowledged serving configuration")
	}
	routerComponent, err := components.ProjectRecord(router.Record.Component)
	if err != nil {
		return err
	}
	if connector.Record.Component.Desired.Config.CloudflareTunnel == nil ||
		len(routerComponent.HTTPRouterZoneIDs()) == 0 {
		return errs.New(errs.KindStateConflict, "serving Tunnel and router need explicit Zone configuration")
	}
	keys := []string{
		components.RecordKey(routerID),
		components.EnvironmentCapabilityKey(environmentID, core.ComponentCapabilityHTTPRouter),
	}
	read, err := repository.edgeCheckRead(ctx, keys, revision)
	if err != nil {
		return err
	}
	defer keyvalue.ClearValues(read.Values)
	for index := range keys {
		if read.Values[index] == nil {
			return errs.New(errs.KindStateConflict, "Tunnel check router, Agent or token reference is unavailable")
		}
	}
	desiredRouter, err := components.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return err
	}
	if !edgeCheckComponent(desiredRouter, environmentID, routerID, kind) || string(read.Values[1].Value) != routerID {
		return errs.New(errs.KindStateConflict, "Route router is no longer enabled in this Environment")
	}
	origin, zones, err := edgeCheckOrigin(connector.Record, router.Record, source.projection)
	if err != nil {
		return err
	}
	path := source.path
	if path == "" {
		path = route.Desired.Path
	}
	if !edgeCheckRoutePath(route.Desired.Path, path) {
		return errs.New(
			errs.KindValidationFailed,
			"check path must match the selected Route; choose a concrete path for wildcard Routes",
		)
	}
	target := contract.PublicTarget{Hostname: route.Desired.Host, Path: path}
	if err := target.Validate(); err != nil {
		return err
	}
	source.configuration.Request.RouteID, source.configuration.Request.OriginAddress, source.configuration.Request.Target = route.Desired.ID, origin, target
	source.configuration.RouterRevision, source.configuration.RouterAppliedRevision = read.Values[0].ModRevision, router.Revision
	source.configuration.Zones, source.router = zones, router
	source.conditions = append(
		source.conditions,
		keyvalue.Condition{Key: componentplanning.RuntimeKey(routerID), ModRevision: router.Revision},
	)
	for index, key := range keys {
		source.conditions = append(
			source.conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	return nil
}

// Both managed Routes and standalone origins capture the same connector,
// installed helper and token-reference metadata. Token bytes are never decoded.
func (repository *RouteRepository) captureEdgeCheckConnector(
	ctx context.Context,
	source *EdgeCheckSource,
	desired components.Record,
) error {
	revision, environmentID, connectorID := source.fence.ReadRevision(), source.environment.Record.ID, desired.Desired.ID
	connector, found, err := componentplanning.ReadRuntime(ctx, repository.store, connectorID, revision)
	if err != nil {
		return err
	}
	if !found ||
		!edgeCheckComponent(connector.Record.Component, environmentID, connectorID, core.ComponentKindEdgeCloudflare) ||
		connector.Record.Component.Desired.Config.CloudflareTunnel == nil {
		return errs.New(
			errs.KindStateConflict,
			"Tunnel has no acknowledged running configuration; apply it before checking access",
		)
	}
	secretID := connector.Record.Component.Desired.Config.CloudflareTunnel.SecretID
	keys := []string{
		localagents.LocalAgentPrimaryKey(source.configuration.AgentID),
		secrets.RecordKey(secretID),
		secrets.ValueKey(secretID),
		deletions.TombstoneKey(string(deletions.DeletionTargetSecret), secretID),
	}
	read, err := repository.edgeCheckRead(ctx, keys, revision)
	if err != nil {
		return err
	}
	defer keyvalue.ClearValues(read.Values)
	for index := 0; index < 3; index++ {
		if read.Values[index] == nil {
			return errs.New(errs.KindStateConflict, "Tunnel check Agent or token reference is unavailable")
		}
	}
	if read.Values[3] != nil {
		return errs.New(errs.KindResourceInUse, "Tunnel token deletion is in progress")
	}
	agent, err := localagents.DecodeLocalAgentPrimary(read.Values[0].Value)
	if err != nil {
		return err
	}
	if agent.ID != source.configuration.AgentID || agent.Phase != localagents.LocalAgentPhaseReady {
		return errs.New(
			errs.KindStateConflict,
			"wait for Agent installation or update to finish before checking public access",
		)
	}
	secret, err := secrets.DecodeRecord(read.Values[1].Value)
	if err != nil {
		return err
	}
	if secret.Secret.ID != secretID || secret.Secret.Kind != core.SecretKindEnvVar ||
		secret.Secret.Scope != core.SecretScopePlatform &&
			(secret.Secret.Scope != core.SecretScopeProject || secret.Secret.ProjectID != source.project.Record.ID) {
		return errs.New(errs.KindStateConflict, "Tunnel token reference is unavailable in this Project")
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(connector.Record.Artifact, artifact) != nil {
		return errs.New(errs.KindInternal, "acknowledged Tunnel artifact is unreadable")
	}
	selected, err := edgeCheckConnectorObservation(connector.Record, artifact)
	if err != nil {
		return err
	}
	source.configuration.Request = contract.Request{EnvironmentID: environmentID, ComponentID: connectorID,
		ServiceID: selected.ServiceId, RuntimeSHA256: hex.EncodeToString(artifact.YamlSha256)}
	source.configuration.ComponentAppliedRevision = connector.Revision
	source.configuration.TokenSecretID, source.configuration.TokenSecretRevision = secretID, read.Values[1].ModRevision
	source.configuration.TokenValueRevision = read.Values[2].ModRevision
	source.configuration.AgentRevision, source.configuration.AgentGeneration = read.Values[0].ModRevision, agent.Generation
	source.configuration.HelperImage, source.configuration.Runtime = agent.Image, slices.Clone(
		connector.Record.Artifact,
	)
	source.connector, source.artifact, source.target = connector, artifact, selected
	source.conditions = append(
		source.conditions,
		keyvalue.Condition{Key: componentplanning.RuntimeKey(connectorID), ModRevision: connector.Revision},
	)
	for index, key := range keys {
		source.conditions = append(
			source.conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	return nil
}

func edgeCheckConnectorObservation(
	runtime componentplanning.Runtime,
	artifact *agentpb.ComposeArtifact,
) (*agentpb.ServiceObservationTarget, error) {
	if len(runtime.Component.Runtime.GeneratedServices) != 1 {
		return nil, errs.New(errs.KindInternal, "acknowledged Tunnel singleton ownership is invalid")
	}
	var selected *agentpb.ComposeService
	for _, service := range artifact.Services {
		if service.GetOwnerComponentId() != runtime.Component.Desired.ID {
			continue
		}
		if selected != nil || service.GetServiceId() != runtime.Component.Runtime.GeneratedServices[0] ||
			service.ExpectedReplicas != 1 {
			return nil, errs.New(errs.KindInternal, "acknowledged Tunnel singleton ownership is invalid")
		}
		selected = service
	}
	if selected == nil {
		return nil, errs.New(errs.KindStateConflict, "acknowledged Tunnel container is unavailable")
	}
	member := &agentpb.ServiceWorkloadObservation{
		RenderGeneration: runtime.RenderGeneration,
		ComposeName:      selected.ComposeName,
	}
	target := &agentpb.ServiceObservationTarget{EnvironmentId: runtime.EnvironmentID, ServiceId: selected.ServiceId,
		Workloads: []*agentpb.ServiceWorkloadObservation{member}, RuntimeRole: "component",
		ComponentId: runtime.Component.Desired.ID, ComposeProject: artifact.ProjectName}
	for _, pair := range selected.ExpectedLabels {
		if pair.GetKey() == "com.groundplane.plan-id" {
			member.PlanId = pair.GetValue()
		}
	}
	if serviceobservation.ValidateTarget(target) != nil {
		return nil, errs.New(errs.KindInternal, "acknowledged Tunnel observation target is invalid")
	}
	return target, nil
}

func edgeCheckOrigin(
	connector, router componentplanning.Runtime,
	projection keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection],
) (string, []edgechecks.ZoneSource, error) {
	address, err := netip.ParseAddr(router.Component.Runtime.PinnedIPv4)
	if err != nil || !address.Is4() || !address.IsPrivate() {
		return "", nil, errs.New(errs.KindStateConflict, "Route router has no acknowledged private IPv4 address")
	}
	connectorZones := connector.Component.Desired.Config.CloudflareTunnel.ZoneIDs
	routerComponent, err := components.ProjectRecord(router.Component)
	if err != nil {
		return "", nil, err
	}
	if !routerComponent.Kind.IsEnvironmentProxy() || len(routerComponent.HTTPRouterZoneIDs()) == 0 {
		return "", nil, errs.New(errs.KindStateConflict, "Route router has no selected Zone configuration")
	}
	routerZones := routerComponent.HTTPRouterZoneIDs()
	ids := slices.Clone(connectorZones)
	ids = append(ids, routerZones...)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	zones := make([]edgechecks.ZoneSource, 0, len(ids))
	shared := false
	for _, id := range ids {
		var current *core.Zone
		for index := range projection.Record.DesiredZones {
			if projection.Record.DesiredZones[index].Desired.ID == id {
				current = &projection.Record.DesiredZones[index].Desired
				break
			}
		}
		if current == nil {
			return "", nil, errs.New(
				errs.KindStateConflict,
				"serving Tunnel or router Zone was removed; apply its current configuration before checking access",
			)
		}
		for index, runtime := range []componentplanning.Runtime{connector, router} {
			selectedZones := connectorZones
			if index == 1 {
				selectedZones = routerZones
			}
			if !slices.Contains(selectedZones, id) {
				continue
			}
			matches := false
			for _, zone := range runtime.Zones {
				if zone.Desired == *current {
					matches = true
					break
				}
			}
			if !matches {
				return "", nil, errs.New(
					errs.KindStateConflict,
					"serving Tunnel or router Zone differs from desired state; apply the Zone change before checking access",
				)
			}
		}
		subnet, parseErr := netip.ParsePrefix(current.Subnet)
		if parseErr == nil && subnet.Contains(address) && slices.Contains(connectorZones, id) &&
			slices.Contains(routerZones, id) {
			shared = true
		}
		zones = append(zones, edgechecks.ZoneSource{ID: id, Revision: projection.Revision})
	}
	if !shared {
		return "", nil, errs.New(
			errs.KindStateConflict,
			"Tunnel and Route router need a shared Zone containing the router address",
		)
	}
	return netip.AddrPortFrom(address, 80).String(), zones, nil
}

func edgeCheckRoutePath(route, path string) bool {
	if strings.HasSuffix(route, "*") {
		return strings.HasPrefix(path, strings.TrimSuffix(route, "*"))
	}
	return route == path
}
