package platformcomponents

import (
	"net/netip"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ZoneDNSListenerSelection combines observed bridge addresses with the exact
// authored heads that permit them. It is publication evidence, not desired state.
type ZoneDNSListenerSelection struct {
	ReadRevision int64
	Addresses    []string
	Conditions   []keyvalue.Condition
}

func ParseZoneListeners(listeners []string) ([]netip.Addr, error) {
	if len(listeners) > 256 {
		return nil, errs.New(errs.KindValidationFailed, "platform resolver has more than 256 Zone listeners")
	}
	addresses := make([]netip.Addr, len(listeners))
	for index, listener := range listeners {
		address, err := netip.ParseAddr(listener)
		if err != nil || !address.Is4() || !address.IsPrivate() || address.String() != listener ||
			index > 0 && addresses[index-1].Compare(address) >= 0 {
			return nil, errs.New(
				errs.KindValidationFailed,
				"platform resolver Zone listeners must be canonical, unique private IPv4 addresses in ascending order",
			)
		}
		addresses[index] = address
	}
	return addresses, nil
}

func (selection ZoneDNSListenerSelection) Validate() error {
	if selection.ReadRevision <= 0 {
		return errs.New(errs.KindInternal, "Zone listener selection has no captured revision")
	}
	if _, err := ParseZoneListeners(selection.Addresses); err != nil {
		return err
	}
	for _, condition := range selection.Conditions {
		if condition.Key == "" || condition.Prefix || condition.ModRevision < 0 {
			return errs.New(errs.KindInternal, "Zone listener selection condition is invalid")
		}
	}
	return nil
}
