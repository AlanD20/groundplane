package componentregistration

import (
	"github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
	controllerdns "github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"net/netip"
)

func NewDNSConnectionCheckPlanner(
	components *etcd.ComponentRepository,
	catalog Catalog,
) (*controllerdns.ConnectionCheckPlanner, error) {
	return controllerdns.NewConnectionCheckPlanner(components, catalog, registeredcoredns.ObserveServingAction)
}

func validateRegisteredCoreDNSComponent() error {
	_, err := NewDNSRenderer()
	return err
}

func NewDNSRenderer() (dnsresolver.Renderer, error) {
	if _, err := registeredcoredns.Definition(); err != nil {
		return nil, err
	}
	return registeredcoredns.Renderer{}, nil
}

func NewDNSManagedConfigProjector(
	projections controllerdns.PlatformProjectionReader,
	records controllerdns.DNSRecordsResolver,
	baselines controllerdns.ResolverBaselineReader,
	registryAddress netip.Addr,
	zoneListeners *controllerdns.ZoneListenerSource,
) (*controllerdns.ManagedConfigProjector, error) {
	renderer, err := NewDNSRenderer()
	if err != nil {
		return nil, err
	}
	return controllerdns.NewManagedConfigProjector(
		projections,
		records,
		baselines,
		renderer,
		registeredcoredns.CorefileTarget,
		registryAddress,
		zoneListeners,
	)
}
