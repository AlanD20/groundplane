package coredns

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"

	"github.com/AlanD20/groundplane/internal/core"
)

// BuildRenderInput materializes the pure renderer input from authoritative
// environment state. Only enabled environment Caddy components with a pinned
// applied IPv4 contribute public Route hostnames; the renderer itself never
// walks this state.
func BuildRenderInput(environments []core.Environment, config Config, baseline []ResolverEndpoint) (CoreDNSRenderInput, error) {
	if err := config.Validate(); err != nil {
		return CoreDNSRenderInput{}, err
	}

	catchAll := baseline
	if !config.UpstreamAuto {
		catchAll = config.UpstreamResolvers
	}
	catchAll, err := normalizeResolvers(append([]ResolverEndpoint(nil), catchAll...))
	if err != nil {
		return CoreDNSRenderInput{}, err
	}

	forwarders := append([]CoreDNSForwarder(nil), config.Forwarders...)
	if config.TailnetDelegation {
		forwarders = append(forwarders, CoreDNSForwarder{
			Domain: "ts.net",
			Resolvers: []ResolverEndpoint{{
				Address: netip.MustParseAddr("100.100.100.100"),
			}},
		})
	}

	hosts := make([]CoreDNSHost, 0)
	for _, environment := range environments {
		for _, component := range environment.Components {
			if component.Kind != core.ComponentKindIngressCaddy {
				continue
			}
			if component.Owner != core.ComponentOwnerEnvironment || component.OwnerID != environment.ID {
				return CoreDNSRenderInput{}, invalid(fmt.Sprintf("coredns: Caddy %q is not owned by environment %q", component.ID, environment.ID))
			}
			if !component.Enabled {
				continue
			}
			address, err := netip.ParseAddr(component.PinnedIPv4)
			if err != nil || !address.Is4() || address.Is4In6() || address.IsUnspecified() || address.IsMulticast() {
				return CoreDNSRenderInput{}, invalid(fmt.Sprintf("coredns: enabled Caddy %q has no usable pinned IPv4", component.ID))
			}
			for _, route := range environment.Routes {
				if route.Exposure != "public" {
					continue
				}
				if err := route.Validate(); err != nil {
					return CoreDNSRenderInput{}, invalid(fmt.Sprintf("coredns: environment %q has invalid public route: %v", environment.ID, err))
				}
				if !validDNSName(route.Host) {
					return CoreDNSRenderInput{}, invalid(fmt.Sprintf("coredns: environment %q public route host is not a DNS name", environment.ID))
				}
				hosts = append(hosts, CoreDNSHost{Address: address, Hostnames: []string{route.Host}})
			}
		}
	}

	return CoreDNSRenderInput{
		Hosts:      hosts,
		Forwarders: forwarders,
		CatchAll:   catchAll,
	}, nil
}

// DigestRenderInput returns the SHA-256 of the canonical input. Sorting and
// validation use the same path as rendering, so a plan hash describes exactly
// the bytes that may be applied.
func DigestRenderInput(input CoreDNSRenderInput) ([32]byte, error) {
	hosts, err := normalizeHosts(input.Hosts)
	if err != nil {
		return [32]byte{}, err
	}
	forwarders, err := normalizeForwarders(input.Forwarders)
	if err != nil {
		return [32]byte{}, err
	}
	catchAll, err := normalizeResolvers(input.CatchAll)
	if err != nil {
		return [32]byte{}, err
	}
	canonical := struct {
		Hosts      []CoreDNSHost
		Forwarders []CoreDNSForwarder
		CatchAll   []ResolverEndpoint
	}{hosts, forwarders, catchAll}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return [32]byte{}, invalid("coredns: canonical input encoding failed")
	}
	return sha256.Sum256(encoded), nil
}

// SortEnvironments returns a copy ordered by stable environment id for
// callers that want a canonical traversal before materialization.
func SortEnvironments(environments []core.Environment) []core.Environment {
	result := append([]core.Environment(nil), environments...)
	sort.Slice(result, func(left, right int) bool { return result[left].ID < result[right].ID })
	return result
}
