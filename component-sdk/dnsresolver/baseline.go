package dnsresolver

import (
	"crypto/sha256"
	"fmt"
	"net/netip"
	"sort"
)

func NewResolverBaseline(generation uint64, resolvers []ResolverEndpoint) (ResolverBaseline, error) {
	baseline := ResolverBaseline{Generation: generation, Resolvers: append([]ResolverEndpoint(nil), resolvers...)}
	for index := range baseline.Resolvers {
		if baseline.Resolvers[index].Port == 0 {
			baseline.Resolvers[index].Port = 53
		}
	}
	sort.Slice(baseline.Resolvers, func(left, right int) bool {
		if comparison := baseline.Resolvers[left].Address.Compare(baseline.Resolvers[right].Address); comparison != 0 {
			return comparison < 0
		}
		return baseline.Resolvers[left].Port < baseline.Resolvers[right].Port
	})
	if err := baseline.Validate(); err != nil {
		return ResolverBaseline{}, err
	}
	return baseline, nil
}

func (baseline ResolverBaseline) Validate() error {
	if baseline.Generation == 0 || len(baseline.Resolvers) == 0 || len(baseline.Resolvers) > 15 {
		return fmt.Errorf("dns resolver: baseline identity or resolver count is invalid")
	}
	previous := netip.AddrPort{}
	for index, endpoint := range baseline.Resolvers {
		if !endpoint.Address.IsValid() || endpoint.Address.Is4In6() || endpoint.Address.Zone() != "" ||
			endpoint.Address.IsUnspecified() || endpoint.Address.IsMulticast() || endpoint.Port == 0 ||
			(endpoint.Address.IsLoopback() && endpoint.Address != netip.MustParseAddr("127.0.0.53")) {
			return fmt.Errorf("dns resolver: baseline endpoint is invalid")
		}
		current := netip.AddrPortFrom(endpoint.Address, endpoint.Port)
		if index > 0 && current.Compare(previous) <= 0 {
			return fmt.Errorf("dns resolver: baseline endpoints are not canonical")
		}
		previous = current
	}
	return nil
}

type HostResolutionProjection struct {
	InputRevision int64
	InputSHA256   [sha256.Size]byte
	Hosts         []Host
}

func NewHostResolutionProjection(
	inputRevision int64,
	digest [sha256.Size]byte,
	hosts []Host,
) (HostResolutionProjection, error) {
	byAddress := make(map[netip.Addr]map[string]struct{}, len(hosts))
	addressByName := make(map[string]netip.Addr)
	for _, host := range hosts {
		if !validHostAddress(host.Address) || len(host.Hostnames) == 0 {
			return HostResolutionProjection{}, fmt.Errorf("dns resolver: host-resolution input is invalid")
		}
		names := byAddress[host.Address]
		if names == nil {
			names = make(map[string]struct{})
			byAddress[host.Address] = names
		}
		for _, hostname := range host.Hostnames {
			if !validHostName(hostname) {
				return HostResolutionProjection{}, fmt.Errorf("dns resolver: hostname is invalid")
			}
			if address, exists := addressByName[hostname]; exists && address != host.Address {
				return HostResolutionProjection{}, fmt.Errorf("dns resolver: hostname maps to multiple addresses")
			}
			addressByName[hostname] = host.Address
			names[hostname] = struct{}{}
		}
	}
	canonical := make([]Host, 0, len(byAddress))
	for address, names := range byAddress {
		hostnames := make([]string, 0, len(names))
		for name := range names {
			hostnames = append(hostnames, name)
		}
		sort.Strings(hostnames)
		canonical = append(canonical, Host{Address: address, Hostnames: hostnames})
	}
	sort.Slice(canonical, func(left, right int) bool {
		return canonical[left].Address.Compare(canonical[right].Address) < 0
	})
	projection := HostResolutionProjection{InputRevision: inputRevision, InputSHA256: digest, Hosts: canonical}
	if err := projection.Validate(); err != nil {
		return HostResolutionProjection{}, err
	}
	return projection, nil
}

func (projection HostResolutionProjection) Validate() error {
	if projection.InputRevision <= 0 || projection.InputSHA256 == ([sha256.Size]byte{}) {
		return fmt.Errorf("dns resolver: host-resolution identity is invalid")
	}
	previousAddress := netip.Addr{}
	seenNames := make(map[string]netip.Addr)
	for _, host := range projection.Hosts {
		if !validHostAddress(host.Address) || len(host.Hostnames) == 0 ||
			(previousAddress.IsValid() && host.Address.Compare(previousAddress) <= 0) {
			return fmt.Errorf("dns resolver: host-resolution addresses are not canonical")
		}
		previousAddress = host.Address
		previousName := ""
		for _, hostname := range host.Hostnames {
			if !validHostName(hostname) || hostname <= previousName {
				return fmt.Errorf("dns resolver: host-resolution names are not canonical")
			}
			if address, exists := seenNames[hostname]; exists && address != host.Address {
				return fmt.Errorf("dns resolver: hostname maps to multiple addresses")
			}
			seenNames[hostname] = host.Address
			previousName = hostname
		}
	}
	return nil
}

type ResolverInput struct {
	Baseline       ResolverBaseline
	HostResolution HostResolutionProjection
}

func (input ResolverInput) Validate() error {
	if err := input.Baseline.Validate(); err != nil {
		return err
	}
	return input.HostResolution.Validate()
}

func validHostAddress(address netip.Addr) bool {
	return address.IsValid() && address.Is4() && !address.Is4In6() && !address.IsUnspecified() && !address.IsMulticast()
}

func validHostName(value string) bool {
	if value == "" || len(value) > 253 || value[len(value)-1] == '.' {
		return false
	}
	start := 0
	for index := 0; index <= len(value); index++ {
		if index < len(value) && value[index] != '.' {
			continue
		}
		label := value[start:index]
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if !(character == '-' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9') {
				return false
			}
		}
		start = index + 1
	}
	return true
}
