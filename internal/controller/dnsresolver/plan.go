package dnsresolver

import (
	"crypto/sha256"
	"net/netip"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentPlanner is the consumer-owned port for a registered Component
// planner. The controller depends only on typed DNS input and a generic
// immutable EnvironmentPlan result.
type EnvironmentPlanner interface {
	Plan(componentsdk.ImplementationKey, string, componentdns.RenderInput) (componentsdk.EnvironmentPlan, error)
}

func ValidateComponent(renderer componentdns.Renderer, component core.Component) error {
	if renderer == nil {
		return invalid("dns-resolver: renderer is required")
	}
	if component.Owner != core.ComponentOwnerPlatform || component.OwnerID != "" {
		return invalid("dns-resolver: component must be platform-owned")
	}
	config, err := DecodeConfig(component.Config)
	if err != nil {
		return err
	}
	input, err := buildRenderInput(nil, config, validationResolvers(config))
	if err != nil {
		return err
	}
	if _, err := renderer.Digest(input); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return nil
}

func BuildIntent(
	renderer componentdns.Renderer,
	environmentPlanner EnvironmentPlanner,
	component core.Component,
	resolverInput componentdns.ResolverInput,
	implementation componentsdk.ImplementationKey,
) (componentdns.Intent, error) {
	if err := ValidateComponent(renderer, component); err != nil {
		return componentdns.Intent{}, err
	}
	if environmentPlanner == nil {
		return componentdns.Intent{}, invalid("dns-resolver: environment planner is required")
	}
	if !component.Enabled {
		return componentdns.Intent{}, invalid("dns-resolver: disabled component cannot receive an update intent")
	}
	if len(component.GeneratedServices) != 1 || component.GeneratedServices[0] == "" {
		return componentdns.Intent{}, invalid("dns-resolver: exactly one generated service is required")
	}
	if implementation == "" {
		return componentdns.Intent{}, invalid("dns-resolver: implementation is required")
	}
	if err := resolverInput.Validate(); err != nil {
		return componentdns.Intent{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	config, err := DecodeConfig(component.Config)
	if err != nil {
		return componentdns.Intent{}, err
	}
	input, err := buildRenderInput(
		resolverInput.HostResolution.Hosts,
		config,
		resolverInput.Baseline.Resolvers,
	)
	if err != nil {
		return componentdns.Intent{}, err
	}
	inputDigest, err := renderer.Digest(input)
	if err != nil {
		return componentdns.Intent{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	plan, err := environmentPlanner.Plan(
		implementation, component.GeneratedServices[0], input,
	)
	if err != nil {
		return componentdns.Intent{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	if len(plan.Files) != 1 || len(plan.Files[0].Content) == 0 {
		clearEnvironmentPlan(plan)
		return componentdns.Intent{}, invalid("dns-resolver: environment planner must return one managed artifact")
	}
	artifactDigest := sha256.Sum256(plan.Files[0].Content)
	artifactLength := uint64(len(plan.Files[0].Content))
	planDigest := componentsdk.DigestEnvironmentPlan(plan)
	clearEnvironmentPlan(plan)
	intent := componentdns.Intent{
		ComponentID: component.ID, ServiceID: component.GeneratedServices[0],
		ArtifactSHA256: artifactDigest, ArtifactLength: artifactLength,
		InputSHA256: inputDigest, PlanSHA256: planDigest,
	}
	if err := intent.Validate(); err != nil {
		return componentdns.Intent{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	return intent, nil
}

func clearEnvironmentPlan(plan componentsdk.EnvironmentPlan) {
	for index := range plan.Files {
		clear(plan.Files[index].Content)
	}
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
	return componentdns.RenderInput{
		CorefileTemplate: config.CorefileTemplate,
		Hosts:            cloneHosts(hosts), Forwarders: forwarders, CatchAll: catchAll,
	}, nil
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
