package cloudflaretunnel

import (
	"fmt"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

const (
	ServiceName = "cloudflare-tunnel"
	image       = "cloudflare/cloudflared:2026.7.2"
	tokenName   = "TUNNEL_TOKEN"
)

type Input struct {
	GeneratedServiceID string
	RouterServiceName  string
	RouterNetworkName  string
	SecretID           string
}

func Definition() (component.Definition, error) {
	router, err := component.NewGrant(component.CapabilityHTTPRouter, component.OperationRead)
	if err != nil { return component.Definition{}, err }
	services, err := component.NewGrant(component.CapabilityServices, component.OperationRead, component.OperationCreate)
	if err != nil { return component.Definition{}, err }
	networks, err := component.NewGrant(component.CapabilityNetworks, component.OperationRead, component.OperationBind)
	if err != nil { return component.Definition{}, err }
	secrets, err := component.NewGrant(component.CapabilitySecrets, component.OperationRead, component.OperationMaterialize)
	if err != nil { return component.Definition{}, err }
	return component.NewDefinition(component.DefinitionInput{
		Implementation: "cloudflare-tunnel", ConfigVariant: "cloudflare-tunnel-v1",
		Provides: []component.Capability{component.CapabilityHTTPEdgeTransport},
		Grants: []component.Grant{router, services, networks, secrets},
		OwnerScopes: []component.OwnerScope{component.OwnerScopeEnvironment},
	})
}

func Plan(input Input) (component.EnvironmentPlan, error) {
	if input.GeneratedServiceID == "" || input.RouterServiceName == "" || input.RouterNetworkName == "" || input.SecretID == "" {
		return component.EnvironmentPlan{}, fmt.Errorf("cloudflare tunnel: planner input is incomplete")
	}
	return component.EnvironmentPlan{Services: []component.ManagedService{{
		ID: input.GeneratedServiceID, Name: ServiceName, Image: image,
		NetworkMode: component.ManagedNetworkModeZones,
		Command: []string{"tunnel", "--no-autoupdate", "run"},
		Networks: []component.ManagedNetworkAttachment{{Name: input.RouterNetworkName, Aliases: []string{ServiceName}}},
		Restart: "unless-stopped", Replicas: 1,
		Dependencies: []component.ManagedDependency{{ServiceName: input.RouterServiceName, Condition: "service_started"}},
		SecretEnvironment: []component.ManagedSecretEnvironment{{Name: tokenName, SecretID: input.SecretID}},
	}}}, nil
}
