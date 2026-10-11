// Package zonedns owns the opt-in resolver decision for an internal Zone.
package zonedns

import (
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/compose-spec/compose-go/v2/types"
	"sort"
)

const Extension = "x-gp-allow-dns"

// Enabled decodes the only authored Zone resolver extension. Docker retains
// its embedded resolver; this selects the shared CoreDNS as its upstream.
func Enabled(network types.NetworkConfig) (bool, error) {
	value, present := network.Extensions[Extension]
	if !present {
		return false, nil
	}
	enabled, ok := value.(bool)
	if !ok {
		return false, errs.New(errs.KindValidationFailed, "network x-gp-allow-dns must be a boolean")
	}
	if err := Validate(network.Internal, enabled); err != nil {
		return false, err
	}
	return enabled, nil
}

func Validate(internal, enabled bool) error {
	if enabled && !internal {
		return errs.New(errs.KindValidationFailed, "Allow GP DNS requires an internal Zone")
	}
	return nil
}

// Gateway is the bridge's reserved first usable IPv4, reachable without an
// external route. CoreDNS runs in the host namespace and serves port 53 there.
func Gateway(subnet string) (string, error) {
	prefix, err := ipam.ParseIPv4Prefix(subnet)
	if err != nil || prefix.String() != subnet {
		return "", errs.New(errs.KindValidationFailed, "DNS Zone subnet must be a canonical IPv4 CIDR")
	}
	if err := ipam.ValidateBridgeSubnet(prefix); err != nil {
		return "", err
	}
	if !prefix.Addr().IsPrivate() {
		return "", errs.New(errs.KindValidationFailed, "Allow GP DNS requires a private IPv4 Zone subnet")
	}
	return prefix.Addr().Next().String(), nil
}

// Upstreams returns the managed resolver addresses selected by the Service's
// captured Zone memberships. Nil leaves ordinary Compose DNS unchanged.
func Upstreams(service types.ServiceConfig, networks types.Networks) (types.StringList, error) {
	names := make([]string, 0, len(service.Networks))
	for name := range service.Networks {
		names = append(names, name)
	}
	sort.Strings(names)
	upstreams := make(types.StringList, 0)
	for _, name := range names {
		network, found := networks[name]
		if !found {
			return nil, errs.New(errs.KindInternal, "Service DNS Zone is absent from its captured topology")
		}
		enabled, err := Enabled(network)
		if err != nil {
			return nil, err
		}
		if !enabled {
			continue
		}
		if len(network.Ipam.Config) != 1 || network.Ipam.Config[0] == nil {
			return nil, errs.New(errs.KindInternal, "Service DNS Zone has no captured IPv4 subnet")
		}
		gateway, err := Gateway(network.Ipam.Config[0].Subnet)
		if err != nil {
			return nil, err
		}
		upstreams = append(upstreams, gateway)
	}
	if len(upstreams) == 0 {
		return nil, nil
	}
	if len(service.DNS) != 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Service dns cannot override an internal Zone's Allow GP DNS setting; remove dns or disable Allow GP DNS",
		)
	}
	return upstreams, nil
}
