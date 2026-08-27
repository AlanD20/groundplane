// Package coredns registers the platform-owned host DNS resolver component.
package coredns

import (
	"net/netip"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/dnsname"
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maxHostGroups        = 128
	maxHostnames         = 1024
	maxForwarders        = 8
	maxResolverEndpoints = 15
)

// ResolverEndpoint is one already-resolved CoreDNS upstream. Port zero and
// port 53 both render as the canonical address-only form.
type ResolverEndpoint struct {
	Address netip.Addr
	Port    uint16
}

// CoreDNSHost maps exact Route hostnames to one Environment's applied Caddy
// address. The renderer never chooses an address or walks Environments.
type CoreDNSHost struct {
	Address   netip.Addr
	Hostnames []string
}

// CoreDNSForwarder delegates one exact DNS domain to explicit resolvers.
type CoreDNSForwarder struct {
	Domain    string
	Resolvers []ResolverEndpoint
}

// CoreDNSRenderInput is the complete pure input to Corefile rendering.
// Resolver discovery and Route/Caddy projection happen before this boundary.
type CoreDNSRenderInput struct {
	Hosts      []CoreDNSHost
	Forwarders []CoreDNSForwarder
	CatchAll   []ResolverEndpoint
}

// Register adds CoreDNS metadata to the shared component registry.
func Register() {
	components.Register(components.Registration{
		Kind:          core.ComponentKindCoreDNS,
		Label:         "CoreDNS (host resolver)",
		AllowedOwners: []core.ComponentOwner{core.ComponentOwnerPlatform},
		ApplyStrategy: components.PlatformUpdate,
		ConfigSchema: []string{
			"upstream_auto",
			"upstream_resolvers",
			"forwarders",
			"tailnet_delegation",
		},
	})
}

// RenderCorefile validates and renders a byte-deterministic Corefile. It has no
// filesystem, Agent, observation, or apply side effects; callers can reject an
// invalid candidate without disturbing the currently serving CoreDNS config.
func RenderCorefile(input CoreDNSRenderInput) ([]byte, error) {
	hosts, err := normalizeHosts(input.Hosts)
	if err != nil {
		return nil, err
	}
	forwarders, err := normalizeForwarders(input.Forwarders)
	if err != nil {
		return nil, err
	}
	catchAll, err := normalizeResolvers(input.CatchAll)
	if err != nil {
		return nil, err
	}

	var output strings.Builder
	output.WriteString(".:53 {\n")
	output.WriteString("    bind 127.0.0.1\n")
	if len(hosts) > 0 {
		output.WriteString("    hosts {\n")
		for _, host := range hosts {
			output.WriteString("        ")
			output.WriteString(host.Address.String())
			for _, hostname := range host.Hostnames {
				output.WriteByte(' ')
				output.WriteString(hostname)
			}
			output.WriteByte('\n')
		}
		output.WriteString("        no_reverse\n")
		output.WriteString("        fallthrough\n")
		output.WriteString("    }\n")
	}
	for _, forwarder := range forwarders {
		writeForward(&output, forwarder.Domain, forwarder.Resolvers)
	}
	writeForward(&output, ".", catchAll)
	output.WriteString("    reload\n")
	output.WriteString("    prometheus 127.0.0.1:9153\n")
	output.WriteString("    log\n")
	output.WriteString("    errors\n")
	output.WriteString("}\n")
	return []byte(output.String()), nil
}

func normalizeHosts(input []CoreDNSHost) ([]CoreDNSHost, error) {
	if len(input) > maxHostGroups {
		return nil, invalid("coredns: too many static host address groups")
	}
	byAddress := make(map[netip.Addr]map[string]struct{}, len(input))
	addressByHostname := make(map[string]netip.Addr)
	for _, host := range input {
		if !host.Address.IsValid() || !host.Address.Is4() || host.Address.Is4In6() ||
			host.Address.IsUnspecified() || host.Address.IsMulticast() || len(host.Hostnames) == 0 {
			return nil, invalid("coredns: static host group is invalid")
		}
		names := byAddress[host.Address]
		if names == nil {
			names = make(map[string]struct{}, len(host.Hostnames))
			byAddress[host.Address] = names
		}
		for _, hostname := range host.Hostnames {
			if !validDNSName(hostname) {
				return nil, invalid("coredns: static hostname is not a canonical Route DNS name")
			}
			if address, exists := addressByHostname[hostname]; exists && address != host.Address {
				return nil, invalid("coredns: hostname maps to more than one Caddy address")
			}
			addressByHostname[hostname] = host.Address
			names[hostname] = struct{}{}
		}
	}
	if len(byAddress) > maxHostGroups || len(addressByHostname) > maxHostnames {
		return nil, invalid("coredns: static host limits exceeded")
	}

	hosts := make([]CoreDNSHost, 0, len(byAddress))
	for address, names := range byAddress {
		hostnames := make([]string, 0, len(names))
		for hostname := range names {
			hostnames = append(hostnames, hostname)
		}
		sort.Strings(hostnames)
		hosts = append(hosts, CoreDNSHost{Address: address, Hostnames: hostnames})
	}
	sort.Slice(hosts, func(left, right int) bool {
		return hosts[left].Address.Compare(hosts[right].Address) < 0
	})
	return hosts, nil
}

func normalizeForwarders(input []CoreDNSForwarder) ([]CoreDNSForwarder, error) {
	if len(input) > maxForwarders {
		return nil, invalid("coredns: too many domain forwarders")
	}
	seen := make(map[string]struct{}, len(input))
	forwarders := make([]CoreDNSForwarder, 0, len(input))
	for _, forwarder := range input {
		if !validDNSNameWithin(forwarder.Domain, 253) {
			return nil, invalid("coredns: forwarder domain is not canonical")
		}
		if _, duplicate := seen[forwarder.Domain]; duplicate {
			return nil, invalid("coredns: forwarder domain is duplicated")
		}
		seen[forwarder.Domain] = struct{}{}
		resolvers, err := normalizeResolvers(forwarder.Resolvers)
		if err != nil {
			return nil, err
		}
		forwarders = append(forwarders, CoreDNSForwarder{Domain: forwarder.Domain, Resolvers: resolvers})
	}
	sort.Slice(forwarders, func(left, right int) bool {
		leftLabels := strings.Count(forwarders[left].Domain, ".") + 1
		rightLabels := strings.Count(forwarders[right].Domain, ".") + 1
		if leftLabels != rightLabels {
			return leftLabels > rightLabels
		}
		return forwarders[left].Domain < forwarders[right].Domain
	})
	return forwarders, nil
}

func normalizeResolvers(input []ResolverEndpoint) ([]ResolverEndpoint, error) {
	if len(input) == 0 || len(input) > maxResolverEndpoints {
		return nil, invalid("coredns: resolver list must contain between 1 and 15 endpoints")
	}
	seen := make(map[netip.AddrPort]struct{}, len(input))
	resolvers := make([]ResolverEndpoint, 0, len(input))
	for _, endpoint := range input {
		address := endpoint.Address
		if !address.IsValid() || address.Is4In6() || address.Zone() != "" || address.IsUnspecified() ||
			(address.IsLoopback() && address != netip.MustParseAddr("127.0.0.53")) || address.IsMulticast() {
			return nil, invalid("coredns: resolver endpoint is not usable")
		}
		port := endpoint.Port
		if port == 0 {
			port = 53
		}
		key := netip.AddrPortFrom(address, port)
		if _, duplicate := seen[key]; duplicate {
			return nil, invalid("coredns: resolver endpoint is duplicated")
		}
		seen[key] = struct{}{}
		resolvers = append(resolvers, ResolverEndpoint{Address: address, Port: port})
	}
	sort.Slice(resolvers, func(left, right int) bool {
		if comparison := resolvers[left].Address.Compare(resolvers[right].Address); comparison != 0 {
			return comparison < 0
		}
		return resolvers[left].Port < resolvers[right].Port
	})
	return resolvers, nil
}

func validDNSName(value string) bool {
	if _, err := netip.ParseAddr(value); err == nil {
		return false
	}
	return dnsname.Valid(value)
}

func validDNSNameWithin(value string, maxLength int) bool {
	if _, err := netip.ParseAddr(value); err == nil {
		return false
	}
	return dnsname.ValidWithin(value, maxLength)
}

func writeForward(output *strings.Builder, domain string, resolvers []ResolverEndpoint) {
	output.WriteString("    forward ")
	output.WriteString(domain)
	for _, endpoint := range resolvers {
		output.WriteByte(' ')
		if endpoint.Port == 53 {
			output.WriteString(endpoint.Address.String())
		} else {
			output.WriteString(netip.AddrPortFrom(endpoint.Address, endpoint.Port).String())
		}
	}
	output.WriteByte('\n')
}

func invalid(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}
