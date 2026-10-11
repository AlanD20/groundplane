package release

import (
	"cmp"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/core/serviceports"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumProxyPorts = serviceproxy.MaximumPorts

// ProxyPort retains the transport with its container port. TCP and UDP listeners
// with the same port number are separate endpoints throughout a Release.
type ProxyPort struct {
	Port     uint16                `json:"port"`
	Protocol serviceports.Protocol `json:"protocol"`
}

// ProxyPorts expands authored declarations into a bounded, canonical listener
// set. Parsing a declaration does not establish runtime forwarding support.
func ProxyPorts(exposures []string) ([]ProxyPort, error) {
	ports := make([]ProxyPort, 0, min(len(exposures), MaximumProxyPorts))
	seen := make(map[ProxyPort]struct{}, cap(ports))
	for _, raw := range exposures {
		declaration, err := serviceports.Parse(raw)
		if err != nil {
			return nil, err
		}
		for number := uint32(declaration.First); number <= uint32(declaration.Last); number++ {
			port := ProxyPort{Port: uint16(number), Protocol: declaration.Protocol}
			if _, duplicate := seen[port]; duplicate {
				continue
			}
			if len(ports) == MaximumProxyPorts {
				return nil, errs.Newf(
					errs.KindValidationFailed,
					"Service exposes more than %d distinct port/protocol pairs; reduce the exposed ports or ranges",
					MaximumProxyPorts,
				)
			}
			seen[port] = struct{}{}
			ports = append(ports, port)
		}
	}
	if len(ports) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Service proxy requires at least one exposed container port")
	}
	slices.SortFunc(ports, compareProxyPorts)
	return ports, nil
}

// RequireTCPProxyPort preserves the Service DNS contract independently of the
// set of transports accepted by declaration parsing or proxy rendering.
func RequireTCPProxyPort(exposures []string) error {
	ports, err := ProxyPorts(exposures)
	if err != nil {
		return err
	}
	for _, port := range ports {
		if port.Protocol == serviceports.TCP {
			return nil
		}
	}
	return errs.New(errs.KindValidationFailed, "DNS Service target requires an exposed TCP port for its stable proxy")
}

// ValidateProxyForwarding checks sealed listener identity and the current
// runtime's transport limit. Callers must not treat an unsupported transport as
// a portless Service or change the selected deployment strategy to admit it.
func ValidateProxyForwarding(implementation serviceproxy.Implementation, ports []ProxyPort) error {
	return serviceproxy.ValidatePorts(implementation, proxyConfigurationPorts(ports))
}

func proxyConfigurationPorts(ports []ProxyPort) []serviceproxy.Port {
	result := make([]serviceproxy.Port, len(ports))
	for index, port := range ports {
		result[index] = serviceproxy.Port{Number: port.Port, Protocol: serviceproxy.Protocol(port.Protocol)}
	}
	return result
}

func compareProxyPorts(left, right ProxyPort) int {
	if order := cmp.Compare(left.Port, right.Port); order != 0 {
		return order
	}
	return cmp.Compare(left.Protocol, right.Protocol)
}
