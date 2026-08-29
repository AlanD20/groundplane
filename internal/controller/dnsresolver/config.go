package dnsresolver

import (
	"net/netip"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Config struct {
	UpstreamAuto      bool
	UpstreamResolvers []componentdns.ResolverEndpoint
	Forwarders        []componentdns.Forwarder
	TailnetDelegation bool
}

func DecodeConfig(raw core.ComponentConfig) (Config, error) {
	if raw.CoreDNS == nil || raw.Caddy != nil || raw.CloudflareTunnel != nil {
		return Config{}, invalid("coredns: complete typed desired config is required")
	}
	upstreamResolvers, err := projectResolvers(raw.CoreDNS.UpstreamResolvers)
	if err != nil {
		return Config{}, err
	}
	forwarders := make([]componentdns.Forwarder, len(raw.CoreDNS.Forwarders))
	for index, forwarder := range raw.CoreDNS.Forwarders {
		resolvers, err := projectResolvers(forwarder.Resolvers)
		if err != nil {
			return Config{}, err
		}
		forwarders[index] = componentdns.Forwarder{
			Domain: forwarder.Domain,
			Resolvers: resolvers,
		}
	}
	config := Config{
		UpstreamAuto: raw.CoreDNS.UpstreamAuto,
		UpstreamResolvers: upstreamResolvers,
		Forwarders: forwarders,
		TailnetDelegation: raw.CoreDNS.TailnetDelegation,
	}
	if err := config.validateMode(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (config Config) validateMode() error {
	if config.UpstreamAuto && len(config.UpstreamResolvers) != 0 {
		return invalid("coredns: upstream_resolvers must be empty when upstream_auto is enabled")
	}
	if !config.UpstreamAuto && len(config.UpstreamResolvers) == 0 {
		return invalid("coredns: explicit upstream_resolvers are required when upstream_auto is disabled")
	}
	if config.TailnetDelegation {
		for _, forwarder := range config.Forwarders {
			if forwarder.Domain == "ts.net" {
				return invalid("coredns: ts.net is managed when tailnet_delegation is enabled")
			}
		}
	}
	return nil
}

func projectResolvers(values []core.DNSResolverEndpoint) ([]componentdns.ResolverEndpoint, error) {
	result := make([]componentdns.ResolverEndpoint, len(values))
	for index, value := range values {
		address, err := netip.ParseAddr(value.Address)
		if err != nil || address.String() != value.Address || address.Is4In6() || address.Zone() != "" ||
			address.IsUnspecified() || address.IsMulticast() {
			return nil, invalid("coredns: resolver address is invalid")
		}
		result[index] = componentdns.ResolverEndpoint{Address: address, Port: value.Port}
	}
	return result, nil
}

func invalid(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}
