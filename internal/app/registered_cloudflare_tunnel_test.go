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
		privateID     = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		frontendID    = "net_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	)
	environment := core.Environment{ID: environmentID, Zones: map[string]core.Zone{
		"private": {
			ID: privateID, Name: "private", Internal: true,
			OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
		},
		"frontend": {
			ID: frontendID, Name: "frontend",
			OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
		},
	}}
	tunnel := core.Component{
		ID: tunnelID, Owner: core.ComponentOwnerEnvironment, OwnerID: environmentID,
		Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
		Config: core.ComponentConfig{
			CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
				ZoneIDs: []string{privateID, frontendID}, SecretID: secretID,
			},
		},
		GeneratedServices: []string{tunnelService},
	}
	plan, err := planRegisteredCloudflareTunnel(environment, tunnel)
	if err != nil {
		t.Fatalf("planRegisteredCloudflareTunnel() error = %v", err)
	}
	service := plan.Services[0]
	if service.NetworkMode != componentsdk.ManagedNetworkModeZones || len(service.Networks) != 2 ||
		service.Networks[0].Name != "private" || service.Networks[0].GatewayPriority != 0 ||
		service.Networks[1].Name != "frontend" || service.Networks[1].GatewayPriority != 1 ||
		!slices.Equal(service.Command, []string{"tunnel", "--no-autoupdate", "--metrics", "127.0.0.1:2000", "run"}) ||
		len(service.Dependencies) != 0 {
		t.Fatalf("Cloudflare managed Service = %#v", service)
	}
}

// Rationale: Controller projection must fail closed instead of replacing an
// internal-only or missing explicit placement with Docker's default bridge.
func TestPlanRegisteredCloudflareTunnelRejectsUnusablePlacement(t *testing.T) {
	t.Parallel()
	const (
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		zoneID        = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	environment := core.Environment{ID: environmentID, Zones: map[string]core.Zone{
		"private": {
			ID: zoneID, Name: "private", Internal: true,
			OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
		},
	}}
	tunnel := core.Component{
		ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW", Owner: core.ComponentOwnerEnvironment, OwnerID: environmentID,
		Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
		Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
			ZoneIDs: []string{zoneID}, SecretID: "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		}},
		GeneratedServices: []string{"svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"},
	}
	if _, err := planRegisteredCloudflareTunnel(environment, tunnel); err == nil {
		t.Fatal("planRegisteredCloudflareTunnel() accepted internal-only placement")
	}
	tunnel.Config.CloudflareTunnel.ZoneIDs[0] = "net_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	if _, err := planRegisteredCloudflareTunnel(environment, tunnel); err == nil {
		t.Fatal("planRegisteredCloudflareTunnel() accepted a missing Zone")
	}
}
