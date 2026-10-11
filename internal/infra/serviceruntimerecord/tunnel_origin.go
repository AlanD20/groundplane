package serviceruntimerecord

import (
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// TunnelOrigin selects the acknowledged stable proxy, not a mutable Service
// name or a blue-green candidate. Only its applied TCP exposure and reserved
// address on a selected shared Zone are admitted. No host discovery occurs.
func TunnelOrigin(
	record Record,
	port uint16,
	sharedZones []string,
) (string, string, *agentpb.ServiceObservationTarget, uint32, error) {
	target, replicas, err := WorkloadObservation(record)
	if err != nil {
		return "", "", nil, 0, err
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(record.Runtime.CurrentArtifact, artifact) != nil {
		return "", "", nil, 0, invalidRuntimeIdentity()
	}
	var selected *agentpb.ComposeService
	for _, service := range artifact.Services {
		if service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if selected != nil || service.ServiceId != record.Runtime.ServiceID {
			return "", "", nil, 0, invalidRuntimeIdentity()
		}
		selected = service
	}
	if selected == nil {
		return "", "", nil, 0, errs.New(
			errs.KindStateConflict,
			"origin Service has no acknowledged TCP proxy; expose its HTTP port and Deploy it before checking Tunnel access",
		)
	}
	var document struct {
		Services map[string]struct {
			Expose   []string `yaml:"expose"`
			Networks map[string]struct {
				IPv4Address string `yaml:"ipv4_address"`
			} `yaml:"networks"`
		} `yaml:"services"`
	}
	if yaml.Unmarshal(artifact.CanonicalYaml, &document) != nil {
		return "", "", nil, 0, invalidRuntimeIdentity()
	}
	service, found := document.Services[selected.ComposeName]
	if !found {
		return "", "", nil, 0, invalidRuntimeIdentity()
	}
	exposed := false
	for _, value := range service.Expose {
		number, protocol, explicit := strings.Cut(value, "/")
		parsed, err := strconv.ParseUint(number, 10, 16)
		if err == nil && parsed == uint64(port) && (!explicit || protocol == "tcp") {
			exposed = true
		}
	}
	if port == 0 || !exposed {
		return "", "", nil, 0, errs.New(
			errs.KindValidationFailed,
			"origin port is not exposed as TCP by the serving Service; select an applied HTTP port",
		)
	}
	// Artifact order is stable; select one declared shared Zone deterministically.
	for _, network := range artifact.Networks {
		if !slices.Contains(sharedZones, network.NetworkId) {
			continue
		}
		attachment, exists := service.Networks[network.ComposeName]
		if !exists {
			continue
		}
		address, err := netip.ParseAddr(attachment.IPv4Address)
		if err != nil || !address.Is4() || !address.IsPrivate() {
			continue
		}
		return netip.AddrPortFrom(address, port).String(), network.NetworkId, target, replicas, nil
	}
	return "", "", nil, 0, errs.New(
		errs.KindStateConflict,
		"Tunnel and serving origin need a shared Zone with a reserved proxy IPv4; apply the network configuration and Deploy the origin",
	)
}
