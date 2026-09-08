package cloudflaretunnel

import (
	"fmt"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

const (
	ServiceName = "cloudflare-tunnel"
	tokenName   = "TUNNEL_TOKEN"
)

var Image = component.OCIImage{
	Repository:  "docker.io/cloudflare/cloudflared",
	IndexDigest: "4f6655284ab3d252b7f28fedb19fe6c8fc82ee5b1295c20ac74d475e5398a52d",
	Platforms: []component.OCIPlatform{
		{
			OS:           "linux",
			Architecture: "amd64",
			ChildDigest:  "18626b1baac4450214535cd5bc40ef44c0635244d585ebf707749c22b6f3408f",
			ConfigDigest: "e871921d7924ab4baa36da9938ecddb86025b5b1aa930500769456bb24f50a75",
		},
		{
			OS:           "linux",
			Architecture: "arm64",
			ChildDigest:  "a85d5a3d6f22cb3c7e78b2f0d05b0f0daeb72566e9426f656c60b357b7b89c95",
			ConfigDigest: "5d249c08c07ddc00eb501917e030874347a8ea786bdb644bb2cff92a4cd2b843",
		},
	},
}

type Input struct {
	GeneratedServiceID string
	SecretID           string
	Zones              []component.NetworkInput
}

func Definition() (component.Definition, error) {
	services, err := component.NewGrant(
		component.CapabilityServices,
		component.OperationRead,
		component.OperationCreate,
	)
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
	networks, err := component.NewGrant(component.CapabilityNetworks, component.OperationRead)
	if err != nil {
		return component.Definition{}, err
	}
	return component.NewDefinition(component.DefinitionInput{
		Implementation: "cloudflare-tunnel", ConfigVariant: "cloudflare-tunnel-v1",
		Provides:    []component.Capability{component.CapabilityEdgeTunnel},
		Grants:      []component.Grant{services, secrets, networks},
		OwnerScopes: []component.OwnerScope{component.OwnerScopeEnvironment},
	})
}

func Plan(input Input) (component.EnvironmentPlan, error) {
	if input.GeneratedServiceID == "" || input.SecretID == "" || len(input.Zones) == 0 {
		return component.EnvironmentPlan{}, fmt.Errorf("cloudflare tunnel: planner input is incomplete")
	}
	networks := make([]component.ManagedNetworkAttachment, len(input.Zones))
	seenIDs := make(map[string]struct{}, len(input.Zones))
	seenNames := make(map[string]struct{}, len(input.Zones))
	gatewaySelected := false
	for index, zone := range input.Zones {
		if !validNetworkIdentity(zone.ID) || !validNetworkIdentity(zone.Name) {
			return component.EnvironmentPlan{}, fmt.Errorf("cloudflare tunnel: Zone identity is invalid")
		}
		if _, duplicate := seenIDs[zone.ID]; duplicate {
			return component.EnvironmentPlan{}, fmt.Errorf("cloudflare tunnel: Zone id is repeated")
		}
		if _, duplicate := seenNames[zone.Name]; duplicate {
			return component.EnvironmentPlan{}, fmt.Errorf("cloudflare tunnel: Zone name is repeated")
		}
		seenIDs[zone.ID] = struct{}{}
		seenNames[zone.Name] = struct{}{}
		networks[index] = component.ManagedNetworkAttachment{Name: zone.Name}
		if !zone.Internal && !gatewaySelected {
			networks[index].GatewayPriority = 1
			gatewaySelected = true
		}
	}
	if !gatewaySelected {
		return component.EnvironmentPlan{}, fmt.Errorf("cloudflare tunnel: at least one non-internal Zone is required")
	}
	return component.EnvironmentPlan{Services: []component.ManagedService{
		{
			ID:          input.GeneratedServiceID,
			Name:        ServiceName,
			Image:       Image,
			NetworkMode: component.ManagedNetworkModeZones,
			Networks:    networks,
			Command:     []string{"tunnel", "--no-autoupdate", "--metrics", "127.0.0.1:2000", "run"},
			// The native ready command checks the local /ready endpoint and
			// rejects non-200 responses; no shell or extra image tool is needed.
			// https://github.com/cloudflare/cloudflared/blob/master/cmd/cloudflared/tunnel/subcommands.go
			Healthcheck: &component.ManagedHealthcheck{
				Command:         []string{"cloudflared", "tunnel", "--metrics", "127.0.0.1:2000", "ready"},
				IntervalSeconds: 5, TimeoutSeconds: 3, StartPeriodSeconds: 10, Retries: 3,
			},
			Restart:           "unless-stopped",
			Replicas:          1,
			SecretEnvironment: []component.ManagedSecretEnvironment{{Name: tokenName, SecretID: input.SecretID}},
		},
	}}, nil
}

func validNetworkIdentity(value string) bool {
	if value == "" {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}
