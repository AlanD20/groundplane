package publicingress

import (
	"context"
	"net/netip"
	"path/filepath"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

const RelayArgument = "public-ingress-origin-relay"
const RelayBindEnv = "GROUNDPLANE_RELAY_BIND"
const RelayPeerEnv = "GROUNDPLANE_RELAY_PEER"
const RelayStateRoot = "/var/lib/groundplane-ingress"
const relayRole = "public-ingress-origin-relay"

func (local *Local) verifyRelayBridge(ctx context.Context, engine *client.Client, bridge network.Inspect) error {
	relay := local.config.Relay
	if err := verifyLocalAddresses([]netip.AddrPort{relay.Listen()}); err != nil {
		return err
	}
	found := false
	for _, ipam := range bridge.IPAM.Config {
		if ipam.Gateway == relay.Gateway && ipam.Subnet.Contains(relay.EdgeIP) &&
			relay.EdgeIP != ipam.Subnet.Addr() && relay.EdgeIP.Next().IsValid() &&
			ipam.Subnet.Contains(relay.EdgeIP.Next()) {
			found = true
		}
	}
	if !found {
		return conflict("public ingress relay addresses must belong to the exact approved bridge")
	}
	path := filepath.Join(localStateRoot, local.config.HostID)
	runtime, err := local.inspectRelay(ctx, engine, path, false)
	if err != nil {
		return err
	}
	if runtime.State != nil && runtime.State.Running {
		return nil
	}
	return verifyPortsAvailable(ctx, engine, []netip.AddrPort{relay.Listen()})
}

func (local *Local) relayOptions(imageID, state string) client.ContainerCreateOptions {
	config := local.config.Relay
	return client.ContainerCreateOptions{
		Name: edgeName(local.config.HostID) + "-origin",
		Config: &container.Config{
			Image: imageID, User: "0:0", Entrypoint: []string{"/usr/local/bin/groundplane-agent"}, Cmd: []string{RelayArgument},
			Env: []string{
				RelayBindEnv + "=" + config.Listen().String(),
				RelayPeerEnv + "=" + config.EdgeIP.String(),
			},
			Labels: map[string]string{roleLabel: relayRole, hostLabel: local.config.HostID},
		},
		HostConfig: &container.HostConfig{
			NetworkMode: container.NetworkMode("host"), ReadonlyRootfs: true,
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
			CapDrop:       []string{"ALL"}, SecurityOpt: []string{"no-new-privileges"},
			Mounts: []mount.Mount{{Type: mount.TypeBind, Source: state, Target: RelayStateRoot, ReadOnly: true}},
		},
	}
}

func (local *Local) inspectRelay(ctx context.Context, engine *client.Client, state string, requireRunning bool,
) (container.InspectResponse, error) {
	image, err := engine.ImageInspect(ctx, local.config.Relay.Image)
	if err != nil || !strings.HasPrefix(image.ID, "sha256:") ||
		!validRuntimeID(strings.TrimPrefix(image.ID, "sha256:")) {
		return container.InspectResponse{}, conflict("public ingress relay requires the exact pinned local image")
	}
	wanted := local.relayOptions(image.ID, state)
	result, err := engine.ContainerInspect(ctx, wanted.Name, client.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) && !requireRunning {
		return container.InspectResponse{}, nil
	}
	if err != nil || !matchingRelay(result.Container, wanted) || requireRunning && !result.Container.State.Running {
		return container.InspectResponse{}, conflict(
			"public ingress private relay differs from its captured installation",
		)
	}
	return result.Container, nil
}

func matchingRelay(observed container.InspectResponse, wanted client.ContainerCreateOptions) bool {
	if observed.Config == nil || observed.HostConfig == nil || observed.State == nil ||
		!validRuntimeID(observed.ID) || observed.Image != wanted.Config.Image || observed.Name != "/"+wanted.Name ||
		observed.State.Paused || observed.Config.User != wanted.Config.User || observed.Path != wanted.Config.Entrypoint[0] ||
		!slices.Equal(observed.Args, wanted.Config.Cmd) || observed.Config.Labels[roleLabel] != relayRole ||
		observed.Config.Labels[hostLabel] != wanted.Config.Labels[hostLabel] {
		return false
	}
	for _, variable := range wanted.Config.Env {
		if !slices.Contains(observed.Config.Env, variable) {
			return false
		}
	}
	config := observed.HostConfig
	if !config.ReadonlyRootfs || config.Privileged || config.PublishAllPorts ||
		config.NetworkMode != wanted.HostConfig.NetworkMode || config.RestartPolicy != wanted.HostConfig.RestartPolicy ||
		len(config.PortBindings) != 0 || len(config.CapAdd) != 0 ||
		!slices.Equal(
			config.CapDrop,
			wanted.HostConfig.CapDrop,
		) || !slices.Equal(config.SecurityOpt, wanted.HostConfig.SecurityOpt) ||
		len(observed.Mounts) != 1 {
		return false
	}
	volume := observed.Mounts[0]
	bind := wanted.HostConfig.Mounts[0]
	return volume.Type == mount.TypeBind && volume.Source == bind.Source && volume.Destination == bind.Target &&
		!volume.RW
}

func (local *Local) ensureRelay(ctx context.Context, engine *client.Client, state string) error {
	relay, err := local.inspectRelay(ctx, engine, state, false)
	if err != nil {
		return err
	}
	if relay.ID == "" {
		image, err := engine.ImageInspect(ctx, local.config.Relay.Image)
		if err != nil {
			return provisionError(ctx, "observe the local relay image", err)
		}
		options := local.relayOptions(image.ID, state)
		result, err := engine.ContainerCreate(ctx, options)
		if err != nil {
			return provisionError(ctx, "create the private relay", err)
		}
		relay, err = local.inspectRelay(ctx, engine, state, false)
		if err != nil || relay.ID != result.ID || relay.ID == "" {
			return conflict("created public ingress relay does not have captured ownership")
		}
	}
	if !relay.State.Running {
		if _, err := engine.ContainerStart(ctx, relay.ID, client.ContainerStartOptions{}); err != nil {
			return provisionError(ctx, "start the private relay", err)
		}
	}
	return nil
}

func RelayAddresses(bind, peer string) (netip.AddrPort, netip.Addr, error) {
	listen, bindErr := netip.ParseAddrPort(bind)
	source, peerErr := netip.ParseAddr(peer)
	if bindErr != nil || peerErr != nil || !listen.Addr().Is4() || !listen.Addr().IsPrivate() ||
		listen.Port() != ingressexecution.RelayPort || !source.Is4() || !source.IsPrivate() || source == listen.Addr() {
		return netip.AddrPort{}, netip.Addr{}, conflict("public ingress private relay addresses are invalid")
	}
	return listen, source, nil
}
