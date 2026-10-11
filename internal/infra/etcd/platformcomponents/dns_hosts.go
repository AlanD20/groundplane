package platformcomponents

import (
	"net/netip"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CloneDNSHosts isolates captured resolver input from mutable planner buffers.
func CloneDNSHosts(hosts []PlatformDNSHost) []PlatformDNSHost {
	if hosts == nil {
		return nil
	}
	clone := make([]PlatformDNSHost, len(hosts))
	for index, host := range hosts {
		clone[index] = PlatformDNSHost{Address: host.Address, Hostnames: append([]string(nil), host.Hostnames...)}
	}
	return clone
}

func validateDNSHosts(hosts []PlatformDNSHost) error {
	previousAddress := netip.Addr{}
	seenNames := make(map[string]netip.Addr)
	for _, host := range hosts {
		address, err := netip.ParseAddr(host.Address)
		if err != nil || !address.Is4() || address.Is4In6() || address.IsUnspecified() || address.IsMulticast() ||
			previousAddress.IsValid() && address.Compare(previousAddress) <= 0 || len(host.Hostnames) == 0 {
			return errs.New(errs.KindValidationFailed, "platform Component DNS hosts are invalid or unsorted")
		}
		previousAddress = address
		previousName := ""
		for _, hostname := range host.Hostnames {
			if !hostresolution.ValidPlatformDNSName(hostname) || hostname <= previousName {
				return errs.New(errs.KindValidationFailed, "platform Component DNS names are invalid or unsorted")
			}
			if existing, found := seenNames[hostname]; found && existing != address {
				return errs.New(errs.KindValidationFailed, "platform Component DNS name maps to multiple addresses")
			}
			seenNames[hostname] = address
			previousName = hostname
		}
	}
	return nil
}
