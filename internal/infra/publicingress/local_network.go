package publicingress

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/netip"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/serviceobserver"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// Verify connects the independently captured Service runtime projection to
// the local isolated bridge. It checks local addresses and TCP only, without
// sending application requests or claiming that Docker routes across bridges.
func (local *Local) Verify(
	ctx context.Context, request ingressexecution.NetworkRequest,
) (map[string]ingressexecution.NetworkEndpoint, error) {
	if ctx == nil || request.HostID != local.config.HostID || ids.Validate(ids.KindAgent, request.AgentID) != nil ||
		len(request.Addresses) == 0 || len(request.Addresses) > 16 || len(request.Routes) > 512 {
		return nil, conflict("public ingress network request is outside its configured Host")
	}
	listeners := localListeners(request.Addresses)
	if err := verifyLocalAddresses(listeners); err != nil {
		return nil, err
	}
	engine, err := client.New(client.WithHost(hostDockerSocket))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer engine.Close()
	bridge, err := inspectBridge(ctx, engine, request.HostID, local.config.Bridge)
	if err != nil {
		return nil, err
	}
	if err := local.verifyRelayBridge(ctx, engine, bridge); err != nil {
		return nil, err
	}
	image, err := engine.ImageInspect(ctx, local.config.Image)
	if err != nil {
		return nil, provisionError(ctx, "verify the pinned local Caddy image", err)
	}
	if err := local.verifyEdgeMembership(ctx, engine, bridge, request.HostID, image.ID, listeners); err != nil {
		return nil, err
	}
	observer, err := serviceobserver.NewWithEngine(engine)
	if err != nil {
		return nil, err
	}
	known := make(map[string]bool)
	addresses := make(map[string]ingressexecution.NetworkEndpoint, len(request.Routes))
	checked := make(map[netip.AddrPort]bool, len(request.Routes))
	for _, route := range request.Routes {
		members, err := findOriginProxy(ctx, engine, route)
		if err != nil {
			return nil, err
		}
		endpoint, err := selectLocalOrigin(route, members)
		if err != nil {
			return nil, err
		}
		if _, duplicate := addresses[route.RouteID]; duplicate {
			return nil, conflict("public ingress network request repeats a Route")
		}
		if !known[route.ServiceID] {
			if err := verifyLiveProxy(ctx, observer, route.Target); err != nil {
				return nil, err
			}
			known[route.ServiceID] = true
		}
		if !checked[endpoint] {
			if err := probeTCP(ctx, endpoint); err != nil {
				return nil, err
			}
			checked[endpoint] = true
		}
		addresses[route.RouteID] = local.endpoint(route.RouteID, endpoint)
	}
	return addresses, nil
}

func (local *Local) endpoint(routeID string, address netip.AddrPort) ingressexecution.NetworkEndpoint {
	relay := local.config.Relay
	key, _ := hex.DecodeString(relay.Key) // Validated by NewLocal.
	defer clear(key)
	digest := hmac.New(sha256.New, key)
	_, _ = digest.Write([]byte("groundplane.public-ingress.relay.v1\x00" + local.config.HostID + "\x00" +
		routeID + "\x00" + address.String()))
	return ingressexecution.NetworkEndpoint{
		Address: address, Relay: relay.Listen(), Proof: hex.EncodeToString(digest.Sum(nil)),
	}
}

func findOriginProxy(ctx context.Context, engine *client.Client,
	route ingressexecution.NetworkRoute,
) (map[netip.Addr]container.InspectResponse, error) {
	if ids.Validate(ids.KindEnvironment, route.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, route.ServiceID) != nil {
		return nil, conflict("public ingress has an invalid Service proxy identity")
	}
	listed, err := engine.ContainerList(ctx, client.ContainerListOptions{
		All: true, Limit: 2, Filters: client.Filters{}.
			Add("label", "com.groundplane.environment-id="+route.EnvironmentID).
			Add("label", "com.groundplane.service-id="+route.ServiceID).
			Add("label", "com.groundplane.runtime-role=proxy"),
	})
	if err != nil || len(listed.Items) != 1 {
		return nil, conflict("public ingress cannot identify one live Service proxy")
	}
	id := listed.Items[0].ID
	if !validRuntimeID(id) {
		return nil, conflict("public ingress cannot verify the Service proxy identity")
	}
	observed, err := engine.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	proxy := observed.Container
	if err != nil || proxy.ID != id || !isManagedProxy(proxy) || proxy.NetworkSettings == nil {
		return nil, conflict("public ingress cannot verify one exact Service proxy")
	}
	members := make(map[netip.Addr]container.InspectResponse)
	for _, active := range proxy.NetworkSettings.Networks {
		if active != nil && active.IPAddress.Is4() && active.IPAddress.IsPrivate() && validRuntimeID(active.NetworkID) {
			members[active.IPAddress] = proxy
		}
	}
	return members, nil
}

func inspectBridge(ctx context.Context, engine *client.Client, host, wanted string) (network.Inspect, error) {
	observed, err := engine.NetworkInspect(ctx, wanted, client.NetworkInspectOptions{})
	if err != nil {
		return network.Inspect{}, provisionError(ctx, "observe the dedicated bridge", err)
	}
	bridge := observed.Network
	if bridge.ID != wanted || bridge.Driver != "bridge" || bridge.Scope != "local" ||
		bridge.Internal || bridge.Ingress || bridge.ConfigOnly ||
		bridge.Labels[hostLabel] != host || bridge.Labels[roleLabel] != RuntimeRole ||
		bridge.Options["com.docker.network.bridge.enable_icc"] == "false" || len(bridge.Containers) > 513 {
		return network.Inspect{}, conflict("public ingress dedicated bridge does not match its Host ownership")
	}
	return bridge, nil
}

func (local *Local) verifyEdgeMembership(ctx context.Context, engine *client.Client, bridge network.Inspect,
	host, imageID string, listeners []netip.AddrPort,
) error {
	name := edgeName(host)
	identity := DockerIdentity{ImageID: imageID, HostID: host, NetworkID: local.config.Bridge,
		StateDirectory: localStateRoot + "/" + host, CertVolume: name + "-tls", Listeners: listeners,
		EdgeAddress: local.config.Relay.EdgeIP,
	}
	edge, err := engine.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if err == nil {
		identity.RuntimeID = edge.Container.ID
		if !validDockerIdentity(identity) ||
			!matchesInstallation(edge.Container, edgeContainerOptions(name, identity), identity) {
			return conflict("public ingress cannot change or reuse different runtime port bindings")
		}
	} else if !errdefs.IsNotFound(err) {
		return provisionError(ctx, "check existing public ingress", err)
	}
	for id, member := range bridge.Containers {
		if id != identity.RuntimeID || member.IPv4Address.Addr() != identity.EdgeAddress {
			return conflict("public ingress relay bridge contains a container other than the approved edge")
		}
	}
	if identity.RuntimeID != "" && edge.Container.State.Running {
		return nil // The owned edge's exact live port bindings were observed.
	}
	return verifyPortsAvailable(ctx, engine, identity.Listeners)
}

func isManagedProxy(observed container.InspectResponse) bool {
	if observed.Config == nil || observed.HostConfig == nil || observed.State == nil ||
		!observed.State.Running || observed.State.Paused || observed.HostConfig.Privileged ||
		observed.HostConfig.NetworkMode == container.NetworkMode("host") {
		return false
	}
	labels := observed.Config.Labels
	return labels["com.groundplane.managed"] == "true" && labels["com.groundplane.kind"] == "service" &&
		labels["com.groundplane.component-id"] == "" && labels["com.groundplane.runtime-role"] == "proxy" &&
		labels["com.groundplane.release-id"] == "" && labels["com.groundplane.slot"] == "" &&
		labels["com.docker.compose.oneoff"] == "False"
}

func selectLocalOrigin(
	route ingressexecution.NetworkRoute, members map[netip.Addr]container.InspectResponse,
) (netip.AddrPort, error) {
	target := route.Target
	if target == nil || route.AgentGeneration == 0 || route.RouteID == "" ||
		route.EnvironmentID != target.EnvironmentId || route.ServiceID != target.ServiceId ||
		route.ReleaseID != target.ReleaseId || route.ProxyComposeName != target.ProxyComposeName {
		return netip.AddrPort{}, conflict("public ingress has inconsistent Service proxy authority")
	}
	var chosen netip.AddrPort
	for _, address := range route.Addresses {
		member, found := members[address.Addr()]
		if !found || !validPrivateOrigin(address) {
			continue
		}
		labels := member.Config.Labels
		if labels["com.groundplane.environment-id"] != target.EnvironmentId ||
			labels["com.groundplane.service-id"] != target.ServiceId ||
			labels["com.groundplane.plan-id"] != target.ProxyPlanId ||
			labels["com.groundplane.render-generation"] != strconv.FormatUint(target.ProxyRenderGeneration, 10) ||
			labels["com.docker.compose.service"] != target.ProxyComposeName ||
			labels["com.docker.compose.container-number"] != "1" {
			return netip.AddrPort{}, conflict("public ingress proxy address belongs to a different serving runtime")
		}
		if chosen.IsValid() && chosen != address && chosen.Addr() == address.Addr() {
			return netip.AddrPort{}, conflict("public ingress proxy has multiple eligible bridge addresses")
		}
		if !chosen.IsValid() || address.Compare(chosen) < 0 {
			chosen = address
		}
	}
	if !chosen.IsValid() {
		return netip.AddrPort{}, conflict("acknowledged Service proxy is unavailable on the dedicated ingress bridge")
	}
	return chosen, nil
}

func verifyLiveProxy(
	ctx context.Context,
	observer *serviceobserver.Observer,
	target *agentpb.ServiceObservationTarget,
) error {
	observation, err := observer.Observe(ctx, &agentpb.ObserveServices{
		RequestId: ids.NewULID(), Targets: []*agentpb.ServiceObservationTarget{target},
	})
	if err != nil || len(observation.GetObservations()) != 1 ||
		observation.Observations[0].GetProxyState() != agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MATCHING {
		return conflict("public ingress proxy does not serve its acknowledged configuration")
	}
	return nil
}

func probeTCP(ctx context.Context, endpoint netip.AddrPort) error {
	dialer := net.Dialer{Timeout: probeTimeout}
	connection, err := dialer.DialContext(ctx, "tcp", endpoint.String())
	if err != nil {
		return conflict("public ingress acknowledged Service proxy TCP listener is unreachable")
	}
	return connection.Close()
}
