package publicingress

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/api/types/volume"
	"github.com/moby/moby/client"
)

const (
	hostDockerSocket = "unix:///var/run/docker.sock"
	roleLabel        = "io.groundplane.role"
	hostLabel        = "io.groundplane.host"
)

// ProvisionOptions authorizes first installation on an exact, previously
// provisioned private transport bridge. StateDirectory is a host-owned 0700
// directory. Image is the trusted, digest-pinned local Caddy release reference.
// InitialJSON and Listeners are the authorized initial configuration and
// exact bindings from the host plan.
type ProvisionOptions struct {
	StateDirectory string
	Image          string
	NetworkID      string
	HostID         string
	Listeners      []netip.AddrPort
	EdgeAddress    netip.Addr
	InitialJSON    []byte
}

// Provision installs or resumes only the selected edge. It preserves a
// matching running container and refuses to rebind or replace an existing one.
// Call Store.Bootstrap after first provision succeeds. After receipt ownership
// exists, use Store.Apply for routing changes and leave this runtime in place.
func Provision(ctx context.Context, options ProvisionOptions) (DockerIdentity, error) {
	if ctx == nil || !validProvisionInputs(options) {
		return DockerIdentity{}, errs.New(errs.KindValidationFailed, "public ingress provisioning inputs are invalid")
	}
	startup, err := caddyConfiguration(options.InitialJSON)
	if err != nil {
		return DockerIdentity{}, err
	}
	if err := verifyLocalAddresses(options.Listeners); err != nil {
		return DockerIdentity{}, err
	}
	bounded, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	dir, err := (&Store{stateRoot: options.StateDirectory}).open(bounded)
	if err != nil {
		return DockerIdentity{}, err
	}
	defer dir.close()
	engine, err := client.New(client.WithHost(hostDockerSocket))
	if err != nil {
		return DockerIdentity{}, provisionError(bounded, "open local Docker", err)
	}
	defer engine.Close()
	return provision(bounded, engine, dir, options, startup)
}

func provision(
	ctx context.Context,
	engine *client.Client,
	dir *stateDirectory,
	options ProvisionOptions,
	startup []byte,
) (DockerIdentity, error) {
	host := options.HostID
	name := "gp-public-ingress-" + strings.ToLower(host)
	ownership := DockerIdentity{
		HostID: host, NetworkID: options.NetworkID, StateDirectory: options.StateDirectory,
		CertVolume: name + "-tls", Listeners: slices.Clone(options.Listeners), EdgeAddress: options.EdgeAddress,
	}
	image, err := engine.ImageInspect(ctx, options.Image)
	if err != nil {
		return DockerIdentity{}, provisionError(ctx, "verify local Caddy image", err)
	}
	ownership.ImageID = image.ID
	bridge, err := engine.NetworkInspect(ctx, ownership.NetworkID, client.NetworkInspectOptions{})
	if err != nil {
		return DockerIdentity{}, provisionError(ctx, "inspect private ingress bridge", err)
	}
	if bridge.Network.ID != ownership.NetworkID || bridge.Network.Driver != "bridge" ||
		bridge.Network.Scope != "local" || bridge.Network.Internal ||
		bridge.Network.Labels[hostLabel] != host || bridge.Network.Labels[roleLabel] != RuntimeRole {
		return DockerIdentity{}, conflict("public ingress bridge does not match captured ownership")
	}
	wanted := edgeContainerOptions(name, ownership)
	state, recorded, err := dir.load()
	if err != nil {
		return DockerIdentity{}, err
	}
	observed, err := engine.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	haveContainer := err == nil
	if err != nil && !errdefs.IsNotFound(err) {
		return DockerIdentity{}, provisionError(ctx, "inspect edge container", err)
	}
	if recorded && (!haveContainer || observed.Container.ID != state.Current.RuntimeID) {
		return DockerIdentity{}, conflict("recorded public ingress container is missing or replaced")
	}
	if haveContainer && !matchesInstallation(observed.Container, wanted, ownership) {
		return DockerIdentity{}, conflict("existing public ingress container does not match the installation")
	}
	if err := ensureCertificateVolume(ctx, engine, ownership, haveContainer); err != nil {
		return DockerIdentity{}, err
	}
	if !recorded {
		existing, found, err := dir.read(StartupFilename, maximumConfigBytes)
		if err != nil {
			return DockerIdentity{}, err
		}
		if found && !bytes.Equal(existing, startup) || haveContainer && !found {
			return DockerIdentity{}, conflict("public ingress bootstrap configuration changed")
		}
		if !found {
			if err := dir.replace(StartupFilename, startup); err != nil {
				return DockerIdentity{}, err
			}
		}
	}
	if !haveContainer {
		created, err := engine.ContainerCreate(ctx, wanted)
		if err != nil {
			return DockerIdentity{}, provisionError(ctx, "create edge container", err)
		}
		observed, err = engine.ContainerInspect(ctx, created.ID, client.ContainerInspectOptions{})
		if err != nil {
			return DockerIdentity{}, provisionError(ctx, "inspect created edge container", err)
		}
		if !matchesInstallation(observed.Container, wanted, ownership) {
			return DockerIdentity{}, conflict("created public ingress container failed ownership verification")
		}
	}
	ownership.RuntimeID = observed.Container.ID
	if observed.Container.State.Running {
		return ownership, nil
	}
	if recorded && state.Candidate != nil {
		return DockerIdentity{}, conflict("public ingress recovery must settle before a stopped runtime can restart")
	}
	if _, err := engine.ContainerStart(ctx, ownership.RuntimeID, client.ContainerStartOptions{}); err != nil {
		return DockerIdentity{}, provisionError(ctx, "start edge container", err)
	}
	active, err := engine.ContainerInspect(ctx, ownership.RuntimeID, client.ContainerInspectOptions{})
	if err != nil {
		return DockerIdentity{}, provisionError(ctx, "verify started edge container", err)
	}
	if active.Container.ID != ownership.RuntimeID || !matchesInstallation(active.Container, wanted, ownership) ||
		!active.Container.State.Running {
		return DockerIdentity{}, conflict("started public ingress container did not expose the captured listeners")
	}
	return ownership, nil
}

func ensureCertificateVolume(
	ctx context.Context,
	engine *client.Client,
	identity DockerIdentity,
	haveContainer bool,
) error {
	observed, err := engine.VolumeInspect(ctx, identity.CertVolume, client.VolumeInspectOptions{})
	if err == nil {
		return checkCertificateVolume(observed.Volume, identity)
	}
	if !errdefs.IsNotFound(err) {
		return provisionError(ctx, "inspect certificate volume", err)
	}
	if haveContainer {
		return conflict("existing public ingress certificate volume is missing")
	}
	if _, err := engine.VolumeCreate(ctx, client.VolumeCreateOptions{
		Name: identity.CertVolume, Driver: "local", Labels: ownerLabels(identity.HostID),
	}); err != nil {
		return provisionError(ctx, "create certificate volume", err)
	}
	observed, err = engine.VolumeInspect(ctx, identity.CertVolume, client.VolumeInspectOptions{})
	if err != nil {
		return provisionError(ctx, "inspect provisioned certificate volume", err)
	}
	return checkCertificateVolume(observed.Volume, identity)
}

func checkCertificateVolume(actual volume.Volume, identity DockerIdentity) error {
	if actual.Name != identity.CertVolume || actual.Driver != "local" || actual.Scope != "local" ||
		len(
			actual.Options,
		) != 0 || actual.Labels[roleLabel] != RuntimeRole || actual.Labels[hostLabel] != identity.HostID {
		return conflict("public ingress certificate volume does not match captured ownership")
	}
	return nil
}

func edgeContainerOptions(name string, identity DockerIdentity) client.ContainerCreateOptions {
	bindings := make(network.PortMap)
	exposed := make(network.PortSet)
	for _, listener := range identity.Listeners {
		key, _ := network.PortFrom(listener.Port(), network.TCP)
		bindings[key] = append(
			bindings[key],
			network.PortBinding{HostIP: listener.Addr(), HostPort: strconv.Itoa(int(listener.Port()))},
		)
		exposed[key] = struct{}{}
	}
	options := client.ContainerCreateOptions{
		Name: name,
		Config: &container.Config{
			Image: identity.ImageID, User: "0:0", Labels: ownerLabels(identity.HostID), ExposedPorts: exposed,
			Entrypoint: []string{"caddy"}, Cmd: startupCommand(),
		},
		HostConfig: &container.HostConfig{
			NetworkMode: container.NetworkMode(identity.NetworkID), PortBindings: bindings, ReadonlyRootfs: true,
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
			CapDrop: []string{
				"ALL",
			}, CapAdd: []string{"NET_BIND_SERVICE"}, SecurityOpt: []string{"no-new-privileges"},
			Mounts: []mount.Mount{
				{Type: mount.TypeBind, Source: identity.StateDirectory, Target: StartupMountTarget, ReadOnly: true},
				{Type: mount.TypeVolume, Source: identity.CertVolume, Target: "/data"},
			},
		},
	}
	if identity.EdgeAddress.IsValid() {
		options.NetworkingConfig = &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{
			identity.NetworkID: {IPAMConfig: &network.EndpointIPAMConfig{IPv4Address: identity.EdgeAddress}},
		}}
	}
	return options
}

func startupCommand() []string {
	return []string{"run", "--config", StartupMountTarget + "/" + StartupFilename}
}

func ownerLabels(host string) map[string]string {
	return map[string]string{roleLabel: RuntimeRole, hostLabel: host}
}

func matchesInstallation(
	actual container.InspectResponse,
	want client.ContainerCreateOptions,
	identity DockerIdentity,
) bool {
	identity.RuntimeID = actual.ID
	if !validDockerIdentity(identity) || actual.Image != identity.ImageID || actual.Config == nil ||
		actual.HostConfig == nil || actual.State == nil || actual.State.Paused || actual.Name != "/"+want.Name ||
		actual.Path != "caddy" || !slices.Equal(actual.Args, startupCommand()) ||
		actual.Config.User != want.Config.User || actual.Config.Labels[hostLabel] != identity.HostID ||
		actual.Config.Labels[roleLabel] != RuntimeRole {
		return false
	}
	host := actual.HostConfig
	if !host.ReadonlyRootfs || host.Privileged || host.PublishAllPorts ||
		host.NetworkMode != want.HostConfig.NetworkMode ||
		host.RestartPolicy != want.HostConfig.RestartPolicy ||
		!slices.Equal(host.CapAdd, want.HostConfig.CapAdd) ||
		!slices.Equal(host.CapDrop, want.HostConfig.CapDrop) ||
		!slices.Equal(host.SecurityOpt, want.HostConfig.SecurityOpt) ||
		!bindingsMatch(host.PortBindings, want.HostConfig.PortBindings) {
		return false
	}
	mounts := make([]dockerMount, 0, len(actual.Mounts))
	for _, item := range actual.Mounts {
		mounts = append(mounts, dockerMount{string(item.Type), item.Name, item.Source, item.Destination, item.RW})
	}
	if !mountsMatch(mounts, identity) {
		return false
	}
	if !actual.State.Running {
		// Docker completes the network attachment on ContainerStart.
		return true
	}
	if actual.NetworkSettings == nil || len(actual.NetworkSettings.Networks) != 1 ||
		!bindingsMatch(actual.NetworkSettings.Ports, want.HostConfig.PortBindings) {
		return false
	}
	for _, endpoint := range actual.NetworkSettings.Networks {
		return endpoint != nil && endpoint.NetworkID == identity.NetworkID &&
			(!identity.EdgeAddress.IsValid() || endpoint.IPAddress == identity.EdgeAddress)
	}
	return false
}

func bindingsMatch(actual, wanted network.PortMap) bool {
	for port, bindings := range actual {
		// Stock images may report unbound EXPOSE entries.
		if len(bindings) != len(wanted[port]) {
			return false
		}
	}
	for port, expected := range wanted {
		matching := slices.Clone(actual[port])
		if len(matching) != len(expected) {
			return false
		}
		for _, binding := range expected {
			index := slices.Index(matching, binding)
			if index < 0 {
				return false
			}
			matching = slices.Delete(matching, index, index+1)
		}
	}
	return true
}

func validProvisionInputs(options ProvisionOptions) bool {
	digest, pinned := strings.CutPrefix(options.Image, "docker.io/library/caddy@sha256:")
	shape := DockerIdentity{
		RuntimeID: options.NetworkID, NetworkID: options.NetworkID, ImageID: "sha256:" + options.NetworkID,
		HostID: options.HostID, StateDirectory: options.StateDirectory, CertVolume: RuntimeRole,
		Listeners: options.Listeners, EdgeAddress: options.EdgeAddress,
	}
	return pinned && validRuntimeID(digest) && validDockerIdentity(shape)
}

func verifyLocalAddresses(endpoints []netip.AddrPort) error {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return errs.New(errs.KindRequestFailed, "public ingress could not observe host addresses")
	}
	local := make(map[netip.Addr]bool, len(addresses))
	for _, address := range addresses {
		if prefix, err := netip.ParsePrefix(address.String()); err == nil {
			local[prefix.Addr()] = true
		}
	}
	for _, endpoint := range endpoints {
		if !local[endpoint.Addr()] {
			return conflict("public ingress address is not currently assigned to the Host")
		}
	}
	return nil
}

func provisionError(ctx context.Context, action string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errs.Wrap(errs.KindRequestFailed, fmt.Errorf("public ingress could not %s: %w", action, err))
}
