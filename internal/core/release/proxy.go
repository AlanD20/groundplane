package release

import (
	"crypto/sha256"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ProxyConfig struct {
	Implementation serviceproxy.Implementation
	JSON           []byte
	SHA256         [sha256.Size]byte
}

func RenderProxyConfig(
	implementation serviceproxy.Implementation,
	serviceName, releaseID string,
	target WorkloadTarget,
	generation uint64,
	ports []ProxyPort,
) (ProxyConfig, error) {
	if serviceName == "" || strings.ContainsAny(serviceName, "/\\\x00") ||
		target.Validate() != nil || generation == 0 || releaseID == "" {
		return ProxyConfig{}, errs.New(errs.KindValidationFailed, "release proxy config identity is invalid")
	}
	if err := ValidateProxyForwarding(implementation, ports); err != nil {
		return ProxyConfig{}, err
	}
	workloadName, err := WorkloadComposeName(serviceName, target)
	if err != nil {
		return ProxyConfig{}, err
	}
	return renderProxyUpstreams(implementation, releaseID, generation, ports, []string{workloadName})
}

// RenderRollingProxyConfig seals one mixed old/new routing set. Readiness and
// current-stage checks belong to execution; rendering is not promotion proof.
func RenderRollingProxyConfig(
	implementation serviceproxy.Implementation,
	plan rollingrelease.Plan,
	changeOrdinal uint32,
	generation uint64,
	ports []ProxyPort,
) (ProxyConfig, error) {
	changes, err := plan.Changes()
	if err != nil {
		return ProxyConfig{}, err
	}
	if changeOrdinal == 0 || changeOrdinal > uint32(len(changes)) || generation == 0 {
		return ProxyConfig{}, errs.New(errs.KindValidationFailed, "rolling proxy change is invalid")
	}
	if err := serviceproxy.ValidateTrafficSwitchPorts(implementation, proxyConfigurationPorts(ports)); err != nil {
		return ProxyConfig{}, err
	}
	active := changes[changeOrdinal-1].After
	names := make([]string, len(active))
	for index, replica := range active {
		names[index] = replica.RoutingName()
	}
	slices.Sort(names)
	return renderProxyUpstreams(implementation, plan.ReleaseID, generation, ports, names)
}

// RenderRollingInitialProxyConfig does not make an unproven first candidate
// addressable. With no predecessor the listener returns 503 until promotion.
func RenderRollingInitialProxyConfig(
	implementation serviceproxy.Implementation,
	plan rollingrelease.Plan,
	generation uint64,
	ports []ProxyPort,
) (ProxyConfig, error) {
	if err := plan.Validate(); err != nil {
		return ProxyConfig{}, err
	}
	if generation == 0 {
		return ProxyConfig{}, errs.New(errs.KindValidationFailed, "rolling initial proxy generation is invalid")
	}
	if err := serviceproxy.ValidateTrafficSwitchPorts(implementation, proxyConfigurationPorts(ports)); err != nil {
		return ProxyConfig{}, err
	}
	names := make([]string, len(plan.Prior))
	for index, replica := range plan.Prior {
		names[index] = replica.RoutingName()
	}
	slices.Sort(names)
	return renderProxyUpstreams(implementation, plan.ReleaseID, generation, ports, names)
}

func renderProxyUpstreams(
	implementation serviceproxy.Implementation,
	releaseID string,
	generation uint64,
	ports []ProxyPort,
	names []string,
) (ProxyConfig, error) {
	if len(names) > rollingrelease.MaximumReplicas {
		return ProxyConfig{}, errs.New(
			errs.KindValidationFailed,
			"Service proxy upstream count exceeds the replica limit",
		)
	}
	encoded, err := serviceproxy.Render(serviceproxy.Configuration{
		Implementation: implementation, ReleaseID: releaseID, Generation: generation,
		Ports: proxyConfigurationPorts(ports), Upstreams: names,
	})
	if err != nil {
		return ProxyConfig{}, err
	}
	return ProxyConfig{Implementation: implementation, JSON: encoded, SHA256: sha256.Sum256(encoded)}, nil
}
