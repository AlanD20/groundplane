package cloudflaretunnel

import (
	"fmt"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

const (
	ServiceName       = "cloudflare-tunnel"
	RouterServiceName = "caddy"
	OriginURL         = "http://caddy:80"
	tokenName         = "TUNNEL_TOKEN"
)

var image = component.OCIImage{
	Repository:  "docker.io/cloudflare/cloudflared",
	IndexDigest: "4f6655284ab3d252b7f28fedb19fe6c8fc82ee5b1295c20ac74d475e5398a52d",
	Platforms: []component.OCIPlatform{
		{
			OS:           "linux",
			Architecture: "amd64",
			ChildDigest:  "18626b1baac4450214535cd5bc40ef44c0635244d585ebf707749c22b6f3408f",
		},
		{
			OS:           "linux",
			Architecture: "arm64",
			Variant:      "v8",
			ChildDigest:  "a85d5a3d6f22cb3c7e78b2f0d05b0f0daeb72566e9426f656c60b357b7b89c95",
		},
	},
}

type Input struct {
	GeneratedServiceID string
	RouterOrigin       component.HTTPRouterOrigin
	RouterNetworkName  string
	SecretID           string
}

func Definition() (component.Definition, error) {
	router, err := component.NewGrant(component.CapabilityHTTPRouter, component.OperationRead)
	if err != nil {
		return component.Definition{}, err
	}
	services, err := component.NewGrant(
		component.CapabilityServices,
		component.OperationRead,
		component.OperationCreate,
	)
	if err != nil {
		return component.Definition{}, err
	}
	networks, err := component.NewGrant(component.CapabilityNetworks, component.OperationRead, component.OperationBind)
	if err != nil {
		return component.Definition{}, err
	}
	secrets, err := component.NewGrant(
		component.CapabilitySecrets,
		component.OperationRead,
		component.OperationMaterialize,
	)
	if err != nil {
		return component.Definition{}, err
	}
	return component.NewDefinition(component.DefinitionInput{
		Implementation: "cloudflare-tunnel", ConfigVariant: "cloudflare-tunnel-v1",
		Provides:    []component.Capability{component.CapabilityHTTPEdgeTransport},
		Grants:      []component.Grant{router, services, networks, secrets},
		OwnerScopes: []component.OwnerScope{component.OwnerScopeEnvironment},
	})
}

func Plan(input Input) (component.EnvironmentPlan, error) {
	if input.GeneratedServiceID == "" ||
		input.RouterOrigin.ServiceName != RouterServiceName || input.RouterOrigin.URL != OriginURL ||
		component.ValidateHTTPRouterOrigin(input.RouterOrigin) != nil ||
		input.RouterNetworkName == "" || input.SecretID == "" {
		return component.EnvironmentPlan{}, fmt.Errorf("cloudflare tunnel: planner input is incomplete")
	}
	return component.EnvironmentPlan{Services: []component.ManagedService{
		{
			ID:          input.GeneratedServiceID,
			Name:        ServiceName,
			Image:       image,
			NetworkMode: component.ManagedNetworkModeZones,
			Command:     []string{"tunnel", "--no-autoupdate", "--url", input.RouterOrigin.URL, "run"},
			Networks: []component.ManagedNetworkAttachment{
				{Name: input.RouterNetworkName, Aliases: []string{ServiceName}},
			},
			Restart:  "unless-stopped",
			Replicas: 1,
			Dependencies: []component.ManagedDependency{
				{ServiceName: input.RouterOrigin.ServiceName, Condition: "service_started"},
			},
			SecretEnvironment: []component.ManagedSecretEnvironment{{Name: tokenName, SecretID: input.SecretID}},
		},
	}}, nil
}
