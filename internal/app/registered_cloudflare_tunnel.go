package app

import (
	"crypto/sha256"
	"github.com/AlanD20/groundplane-component-sdk/component"
	registeredcaddy "github.com/AlanD20/groundplane-registered-components/caddy"
	registeredtunnel "github.com/AlanD20/groundplane-registered-components/cloudflaretunnel"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func registeredCloudflareTunnelEnvironmentComponent(catalogDigest [sha256.Size]byte) (controller.EnvironmentComponentRegistration, error) {
	definition, err := registeredtunnel.Definition()
	if err != nil {
		return controller.EnvironmentComponentRegistration{}, errs.Wrap(errs.KindInternal, err)
	}
	return controller.EnvironmentComponentRegistration{
		Kind: core.ComponentKindEdgeCloudflare, Definition: definition, CatalogDigest: catalogDigest,
		Plan: planRegisteredCloudflareTunnel,
	}, nil
}

func planRegisteredCloudflareTunnel(environment core.Environment, instance core.Component) (component.EnvironmentPlan, error) {
	if environment.ID == "" || instance.ID == "" || instance.Owner != core.ComponentOwnerEnvironment ||
		instance.OwnerID != environment.ID || instance.Kind != core.ComponentKindEdgeCloudflare {
		return component.EnvironmentPlan{}, errs.New(errs.KindValidationFailed, "cloudflare tunnel: component ownership or kind is invalid")
	}
	if !instance.Enabled { return component.EnvironmentPlan{}, nil }
	if instance.Config.CloudflareTunnel == nil ||
		ids.Validate(ids.KindSecret, instance.Config.CloudflareTunnel.SecretID) != nil {
		return component.EnvironmentPlan{}, errs.New(errs.KindValidationFailed, "cloudflare tunnel: secret_id is invalid")
	}
	secretID := instance.Config.CloudflareTunnel.SecretID
	if len(instance.GeneratedServices) != 1 || ids.Validate(ids.KindService, instance.GeneratedServices[0]) != nil {
		return component.EnvironmentPlan{}, errs.New(errs.KindValidationFailed, "cloudflare tunnel: one stable generated Service id is required")
	}
	zoneName, err := cloudflareTunnelRouterNetwork(environment)
	if err != nil { return component.EnvironmentPlan{}, err }
	planned, err := registeredtunnel.Plan(registeredtunnel.Input{
		GeneratedServiceID: instance.GeneratedServices[0], RouterServiceName: registeredcaddy.ServiceName,
		RouterNetworkName: zoneName, SecretID: secretID,
	})
	if err != nil { return component.EnvironmentPlan{}, errs.Wrap(errs.KindValidationFailed, err) }
	return planned, nil
}

func cloudflareTunnelRouterNetwork(environment core.Environment) (string, error) {
	for _, instance := range environment.Components {
		if instance.Kind != core.ComponentKindIngressCaddy { continue }
		if !instance.Enabled { return "", errs.New(errs.KindValidationFailed, "cloudflare tunnel: HTTP router must be enabled") }
		if instance.Config.Caddy == nil || instance.Config.Caddy.ZoneID == "" { return "", errs.New(errs.KindValidationFailed, "cloudflare tunnel: HTTP router network is invalid") }
		zoneID := instance.Config.Caddy.ZoneID
		for _, zone := range environment.Zones {
			if zone.ID == zoneID { return zone.Name, nil }
		}
		return "", errs.New(errs.KindValidationFailed, "cloudflare tunnel: HTTP router network is missing")
	}
	return "", errs.New(errs.KindValidationFailed, "cloudflare tunnel: enabled HTTP router is required")
}
