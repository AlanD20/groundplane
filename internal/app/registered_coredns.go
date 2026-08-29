package app

import (
	"github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
)

func validateRegisteredCoreDNSComponent() error {
	_, err := registeredCoreDNSRenderer()
	return err
}

func registeredCoreDNSRenderer() (dnsresolver.Renderer, error) {
	if _, err := registeredcoredns.Definition(); err != nil {
		return nil, err
	}
	return registeredcoredns.Renderer{}, nil
}
