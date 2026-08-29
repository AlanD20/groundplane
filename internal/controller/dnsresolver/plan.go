package dnsresolver

import (
	"crypto/sha256"
	"net/netip"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ValidateComponent(renderer componentdns.Renderer, component core.Component) error {
	if renderer == nil {
		return invalid("coredns: renderer is required")
	}
	if component.Kind != core.ComponentKindCoreDNS || component.Owner != core.ComponentOwnerPlatform || component.OwnerID != "" {
		return invalid("coredns: component must be the singleton platform CoreDNS component")
	}
	config, err := DecodeConfig(component.Config)
	if err != nil { return err }
	input, err := buildRenderInput(nil, config, validationResolvers(config))
	if err != nil { return err }
	if _, err := renderer.Digest(input); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return nil
}

func BuildTaskPlan(renderer componentdns.Renderer, component core.Component, hosts []componentdns.Host, baseline []componentdns.ResolverEndpoint) (componentdns.TaskPlan, error) {
	if err := ValidateComponent(renderer, component); err != nil {
		return componentdns.TaskPlan{}, err
	}
	if !component.Enabled {
		return componentdns.TaskPlan{}, invalid("coredns: disabled component cannot receive an update plan")
	}
	if len(component.GeneratedServices) != 1 || component.GeneratedServices[0] == "" {
		return componentdns.TaskPlan{}, invalid("coredns: exactly one generated service is required")
	}
	config, err := DecodeConfig(component.Config)
	if err != nil { return componentdns.TaskPlan{}, err }
	input, err := buildRenderInput(hosts, config, baseline)
	if err != nil { return componentdns.TaskPlan{}, err }
	corefile, err := renderer.Render(input)
	if err != nil { return componentdns.TaskPlan{}, errs.Wrap(errs.KindValidationFailed, err) }
	inputDigest, err := renderer.Digest(input)
	if err != nil { return componentdns.TaskPlan{}, errs.Wrap(errs.KindValidationFailed, err) }
	plan := componentdns.TaskPlan{
		ComponentID: component.ID, ServiceID: component.GeneratedServices[0],
		Corefile: append([]byte(nil), corefile...), CorefileSHA256: sha256.Sum256(corefile), InputSHA256: inputDigest,
		Steps: []componentdns.TaskStep{componentdns.TaskStepValidateConfig, componentdns.TaskStepRender, componentdns.TaskStepApply, componentdns.TaskStepObserve},
	}
	if err := plan.Validate(); err != nil { return componentdns.TaskPlan{}, errs.Wrap(errs.KindValidationFailed, err) }
	return plan, nil
}

func buildRenderInput(hosts []componentdns.Host, config Config, baseline []componentdns.ResolverEndpoint) (componentdns.RenderInput, error) {
	catchAll := append([]componentdns.ResolverEndpoint(nil), baseline...)
	if !config.UpstreamAuto {
		catchAll = append([]componentdns.ResolverEndpoint(nil), config.UpstreamResolvers...)
	}
	if len(catchAll) == 0 {
		return componentdns.RenderInput{}, invalid("coredns: resolver baseline is required when upstream_auto is enabled")
	}
	forwarders := append([]componentdns.Forwarder(nil), config.Forwarders...)
	if config.TailnetDelegation {
		forwarders = append(forwarders, componentdns.Forwarder{
			Domain: "ts.net", Resolvers: []componentdns.ResolverEndpoint{{Address: netip.MustParseAddr("100.100.100.100")}},
		})
	}
	return componentdns.RenderInput{Hosts: cloneHosts(hosts), Forwarders: forwarders, CatchAll: catchAll}, nil
}

func BuildRenderInput(
	hosts []componentdns.Host,
	config Config,
	baseline []componentdns.ResolverEndpoint,
) (componentdns.RenderInput, error) {
	return buildRenderInput(hosts, config, baseline)
}

func validationResolvers(config Config) []componentdns.ResolverEndpoint {
	if config.UpstreamAuto {
		return []componentdns.ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}}
	}
	return config.UpstreamResolvers
}

func cloneHosts(hosts []componentdns.Host) []componentdns.Host {
	cloned := make([]componentdns.Host, len(hosts))
	for index, host := range hosts {
		cloned[index] = componentdns.Host{Address: host.Address, Hostnames: append([]string(nil), host.Hostnames...)}
	}
	return cloned
}
