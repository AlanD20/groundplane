package releaserender

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/netip"
	"slices"
)

func validateProxyAddresses(input ReleaseRenderInput) error {
	if len(input.ProxyAddresses) != 0 && len(input.ProxyPorts) == 0 {
		return errs.New(errs.KindValidationFailed, "portless Service cannot reserve proxy addresses")
	}
	for name, value := range input.ProxyAddresses {
		address, err := netip.ParseAddr(value)
		if err != nil || !address.Is4() || address.String() != value || address.IsUnspecified() ||
			address.IsMulticast() {
			return errs.New(errs.KindValidationFailed, "proxy address is not canonical IPv4")
		}
		found := false
		for _, zone := range input.Projection.DesiredZones {
			if zone.Desired.Name != name {
				continue
			}
			prefix, err := netip.ParsePrefix(zone.Desired.Subnet)
			found = err == nil && prefix.Contains(address) && address != prefix.Addr()
		}
		if !found {
			return errs.New(errs.KindValidationFailed, "proxy address does not belong to its captured Zone")
		}
		for _, service := range input.Projection.DesiredServices {
			if service.Desired.ID == input.ServiceID && !slices.Contains(service.Desired.Zones, name) {
				return errs.New(errs.KindValidationFailed, "proxy Zone does not belong to its Service")
			}
		}
	}
	return nil
}
