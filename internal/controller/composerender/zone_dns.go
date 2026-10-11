package composerender

import (
	"github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/compose-spec/compose-go/v2/types"
)

// applyZoneDNS keeps Docker's embedded network-local resolver and selects
// reachable bridge gateways as its only upstreams. It does not add a network,
// route, host-port publication or connectivity outside the existing Zone.
func applyZoneDNS(service *types.ServiceConfig, networks types.Networks) error {
	upstreams, err := zonedns.Upstreams(*service, networks)
	if err != nil {
		return err
	}
	if len(upstreams) != 0 {
		service.DNS = upstreams
	}
	return nil
}
