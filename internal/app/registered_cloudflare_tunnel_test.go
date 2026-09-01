package app

import (
	"slices"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: production composition must start the remotely managed connector
// from its Secret without acquiring HTTP-router configuration authority.
func TestPlanRegisteredCloudflareTunnelIsRouterIndependent(t *testing.T) {
	t.Parallel()
	const (
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		tunnelID      = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		tunnelService = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		secretID      = "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	environment := core.Environment{ID: environmentID}
	tunnel := core.Component{
		ID: tunnelID, Owner: core.ComponentOwnerEnvironment, OwnerID: environmentID,
		Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
		Config: core.ComponentConfig{
			CloudflareTunnel: &core.CloudflareTunnelComponentConfig{SecretID: secretID},
		},
		GeneratedServices: []string{tunnelService},
	}
	plan, err := planRegisteredCloudflareTunnel(environment, tunnel)
	if err != nil {
		t.Fatalf("planRegisteredCloudflareTunnel() error = %v", err)
	}
	service := plan.Services[0]
	if service.NetworkMode != componentsdk.ManagedNetworkModeDefault || len(service.Networks) != 0 ||
		!slices.Equal(service.Command, []string{"tunnel", "--no-autoupdate", "run"}) ||
		len(service.Dependencies) != 0 {
		t.Fatalf("Cloudflare managed Service = %#v", service)
	}
}
