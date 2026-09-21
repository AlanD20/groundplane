package componentregistration

import (
	"github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
	controllerdns "github.com/AlanD20/groundplane/internal/controller/dnsresolver"
)

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
	baselines controllerdns.ResolverBaselineReader,
) (*controllerdns.ManagedConfigProjector, error) {
	renderer, err := NewDNSRenderer()
	if err != nil {
		return nil, err
	}
	return controllerdns.NewManagedConfigProjector(
		projections,
		baselines,
		renderer,
		registeredcoredns.CorefileTarget,
	)
}
