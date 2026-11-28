package publicingress

import (
	"context"
	"encoding/json"
	"net/netip"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	RuntimeRole        = "public-ingress"
	StartupMountTarget = "/etc/groundplane/public-ingress"
)

// DockerIdentity captures the exact installed edge selected by the host owner.
// The snapshot comes from installed ownership and the approved host bindings.
type DockerIdentity struct {
	RuntimeID      string
	ImageID        string
	HostID         string
	NetworkID      string
	StateDirectory string
	CertVolume     string
	Listeners      []netip.AddrPort
	EdgeAddress    netip.Addr
}

type DockerOwner struct {
	runner runner.Runner
	owned  DockerIdentity
	fence  func(context.Context, string) error
}

func NewDockerOwner(
	taskRunner runner.Runner, owned DockerIdentity, quiescent func(context.Context, string) error,
) (*DockerOwner, error) {
	if taskRunner == nil || quiescent == nil || !validDockerIdentity(owned) {
		return nil, errs.New(errs.KindValidationFailed, "public ingress Docker ownership is invalid")
	}
	owned.Listeners = append([]netip.AddrPort(nil), owned.Listeners...)
	return &DockerOwner{runner: taskRunner, owned: owned, fence: quiescent}, nil
}

func (owner *DockerOwner) Quiescent(ctx context.Context, runtimeID string) error {
	if err := owner.Verify(ctx, runtimeID); err != nil {
		return err
	}
	return owner.fence(ctx, runtimeID)
}

func (owner *DockerOwner) Verify(ctx context.Context, runtimeID string) error {
	if owner == nil || ctx == nil || runtimeID != owner.owned.RuntimeID {
		return errs.New(errs.KindValidationFailed, "public ingress exact container is unavailable")
	}
	// Project the owned fields only. Inspect must not capture application
	// environment data or pass through operator-supplied Docker arguments.
	const format = `{"id":{{json .Id}},"image":{{json .Image}},"running":{{json .State.Running}},"paused":{{json .State.Paused}},"network_mode":{{json .HostConfig.NetworkMode}},"privileged":{{json .HostConfig.Privileged}},"host_ports":{{json .HostConfig.PortBindings}},"live_ports":{{json .NetworkSettings.Ports}},"networks":{{json .NetworkSettings.Networks}},"mounts":{{json .Mounts}},"path":{{json .Path}},"args":{{json .Args}},"role":{{json (index .Config.Labels "io.groundplane.role")}},"host":{{json (index .Config.Labels "io.groundplane.host")}}}`
	result, err := owner.runner.Run(ctx, runner.RunCmdOpts{
		Name: "docker", Args: []string{"container", "inspect", "--format", format, runtimeID},
		Env: dockerEnvironment, ReplaceEnv: true, Timeout: dockerTimeout,
		CaptureLimitBytes: maxOutput,
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil || result.ExitCode != 0 || len(result.Stdout)+len(result.Stderr) >= maxOutput {
		return errs.New(errs.KindRequestFailed, "public ingress container inspection did not complete")
	}
	var observed dockerObservation
	if err := json.Unmarshal(result.Stdout, &observed); err != nil || !observed.matches(owner.owned) {
		return conflict("public ingress container does not match its captured ownership")
	}
	return nil
}

type dockerObservation struct {
	ID          string               `json:"id"`
	ImageID     string               `json:"image"`
	Running     bool                 `json:"running"`
	Paused      bool                 `json:"paused"`
	Privileged  bool                 `json:"privileged"`
	NetworkMode string               `json:"network_mode"`
	HostPorts   dockerPorts          `json:"host_ports"`
	LivePorts   dockerPorts          `json:"live_ports"`
	Networks    map[string]networkID `json:"networks"`
	Mounts      []dockerMount        `json:"mounts"`
	Path        string               `json:"path"`
	Args        []string             `json:"args"`
	Role        string               `json:"role"`
	Host        string               `json:"host"`
}

type networkID struct {
	ID        string     `json:"NetworkID"`
	IPAddress netip.Addr `json:"IPAddress"`
}

type dockerPorts map[string][]struct {
	IP   string `json:"HostIp"`
	Port string `json:"HostPort"`
}

type dockerMount struct {
	Type        string `json:"Type"`
	Name        string `json:"Name"`
	Source      string `json:"Source"`
	Destination string `json:"Destination"`
	Writable    bool   `json:"RW"`
}

func (actual dockerObservation) matches(want DockerIdentity) bool {
	if actual.ID != want.RuntimeID || actual.ImageID != want.ImageID || !actual.Running || actual.Paused ||
		actual.Privileged || actual.Role != RuntimeRole || actual.Host != want.HostID ||
		actual.NetworkMode == "host" || actual.NetworkMode == "none" ||
		strings.HasPrefix(actual.NetworkMode, "container:") || len(actual.Networks) != 1 {
		return false
	}
	for _, network := range actual.Networks {
		if network.ID != want.NetworkID || want.EdgeAddress.IsValid() && network.IPAddress != want.EdgeAddress {
			return false
		}
	}
	return (actual.Path == "caddy" || actual.Path == "/usr/bin/caddy") &&
		slices.Equal(actual.Args, []string{"run", "--config", StartupMountTarget + "/" + StartupFilename}) &&
		actual.HostPorts.matches(want.Listeners) && actual.LivePorts.matches(want.Listeners) &&
		mountsMatch(actual.Mounts, want)
}

func mountsMatch(mounts []dockerMount, want DockerIdentity) bool {
	startup, certs, runtimeConfig := false, false, false
	for _, mount := range mounts {
		switch mount.Destination {
		case StartupMountTarget:
			if startup || mount.Type != "bind" || mount.Source != want.StateDirectory || mount.Writable {
				return false
			}
			startup = true
		case "/data":
			if certs || mount.Type != "volume" || mount.Name != want.CertVolume || !mount.Writable {
				return false
			}
			certs = true
		case "/config":
			// The upstream image may supply its ordinary config volume. It
			// remains disconnected from verified startup and admin autosave.
			if runtimeConfig || mount.Type != "volume" {
				return false
			}
			runtimeConfig = true
		default:
			return false
		}
	}
	return startup && certs
}

func (actual dockerPorts) matches(want []netip.AddrPort) bool {
	addrs := make(map[netip.AddrPort]bool, len(want))
	for containerPort, bindings := range actual {
		if len(bindings) == 0 {
			continue // Docker reports unpublished image EXPOSE entries.
		}
		for _, binding := range bindings {
			if containerPort != binding.Port+"/tcp" || (binding.Port != "80" && binding.Port != "443") {
				return false
			}
			ip, err := netip.ParseAddr(binding.IP)
			if err != nil || !ip.IsGlobalUnicast() || ip.Is4In6() || ip.Zone() != "" {
				return false
			}
			port, err := strconv.ParseUint(binding.Port, 10, 16)
			if err != nil {
				return false
			}
			addr := netip.AddrPortFrom(ip, uint16(port))
			if addrs[addr] {
				return false
			}
			addrs[addr] = true
		}
	}
	if len(addrs) != len(want) {
		return false
	}
	for _, endpoint := range want {
		if !addrs[endpoint] {
			return false
		}
	}
	return true
}

func validDockerIdentity(owned DockerIdentity) bool {
	if owned.EdgeAddress.IsValid() && (!owned.EdgeAddress.Is4() || !owned.EdgeAddress.IsPrivate()) {
		return false
	}
	if !validRuntimeID(owned.RuntimeID) || !validRuntimeID(owned.NetworkID) ||
		!strings.HasPrefix(owned.ImageID, "sha256:") || !validRuntimeID(strings.TrimPrefix(owned.ImageID, "sha256:")) ||
		ids.Validate(ids.KindHost, owned.HostID) != nil || owned.CertVolume == "" ||
		!filepath.IsAbs(owned.StateDirectory) || filepath.Clean(owned.StateDirectory) != owned.StateDirectory ||
		owned.StateDirectory == string(filepath.Separator) || len(owned.Listeners) == 0 || len(owned.Listeners) > 32 {
		return false
	}
	seen := make(map[netip.AddrPort]bool, len(owned.Listeners))
	for _, endpoint := range owned.Listeners {
		ip := endpoint.Addr()
		if !ip.IsGlobalUnicast() || ip.Is4In6() || ip.Zone() != "" ||
			(endpoint.Port() != 80 && endpoint.Port() != 443) || seen[endpoint] {
			return false
		}
		seen[endpoint] = true
	}
	for _, endpoint := range owned.Listeners {
		if !seen[netip.AddrPortFrom(endpoint.Addr(), 80)] || !seen[netip.AddrPortFrom(endpoint.Addr(), 443)] {
			return false
		}
	}
	return true
}
