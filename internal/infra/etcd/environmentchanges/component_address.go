package environmentchanges

import (
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentAddressBinding struct {
	zoneID  string
	address string
}

func (binding componentAddressBinding) ZoneID() string { return binding.zoneID }

func (binding componentAddressBinding) Address() string { return binding.address }

func ComponentAddress(record componentrecord.Record) (componentAddressBinding, bool, error) {
	if !record.Desired.Kind.IsEnvironmentProxy() {
		if record.Runtime.PinnedIPv4 != "" {
			return componentAddressBinding{}, false, errs.New(
				errs.KindValidationFailed,
				"non-router Component candidate has a pinned address",
			)
		}
		return componentAddressBinding{}, false, nil
	}
	if !record.Desired.Enabled {
		if record.Runtime.PinnedIPv4 != "" {
			return componentAddressBinding{}, false, errs.New(
				errs.KindValidationFailed,
				"disabled router Component candidate has a pinned address",
			)
		}
		return componentAddressBinding{}, false, nil
	}
	component := core.Component{Kind: record.Desired.Kind, Config: record.Desired.Config}
	zoneIDs := component.HTTPRouterZoneIDs()
	if len(zoneIDs) == 0 {
		return componentAddressBinding{}, false, errs.New(
			errs.KindValidationFailed,
			"enabled router Component candidate has no primary Zone",
		)
	}
	zoneID := zoneIDs[0]
	if ids.Validate(ids.KindNetwork, zoneID) != nil {
		return componentAddressBinding{}, false, errs.New(
			errs.KindValidationFailed,
			"enabled router Component candidate has an invalid Zone",
		)
	}
	address, err := netip.ParseAddr(record.Runtime.PinnedIPv4)
	if err != nil || !address.Is4() || address.String() != record.Runtime.PinnedIPv4 {
		return componentAddressBinding{}, false, errs.New(
			errs.KindValidationFailed,
			"enabled router Component candidate has an invalid pinned address",
		)
	}
	return componentAddressBinding{zoneID: zoneID, address: address.String()}, true, nil
}
