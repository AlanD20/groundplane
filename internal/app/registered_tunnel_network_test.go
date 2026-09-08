package app

import (
	"testing"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: connector health is insufficient when Docker isolates the Tunnel
// from the router; compiled registrations must honor independently authored
// multiple memberships and keep outbound gateway selection explicit.
func TestRegisteredTunnelSharesExplicitRouterZones(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 6, 6, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	zoneID := ids.NewAt(ids.KindNetwork, at, 2)
	privateZoneID := ids.NewAt(ids.KindNetwork, at, 8)
	tunnelServiceID := ids.NewAt(ids.KindService, at, 3)
	environment := core.Environment{
		ID: environmentID,
		Zones: map[string]core.Zone{
			"public-edge": {
				ID:        zoneID,
				Name:      "public-edge",
				Subnet:    "10.40.10.0/24",
				OwnerKind: core.ZoneOwnerEnvironment,
				OwnerID:   environmentID,
			},
			"private": {
				ID:        privateZoneID,
				Name:      "private",
				Subnet:    "10.40.20.0/24",
				Internal:  true,
				OwnerKind: core.ZoneOwnerEnvironment,
				OwnerID:   environmentID,
			},
		},
		Components: []core.Component{
			{
				ID: ids.NewAt(ids.KindComponent, at, 4), Owner: core.ComponentOwnerEnvironment,
				OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
				GeneratedServices: []string{tunnelServiceID},
				Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
					ZoneIDs:  []string{privateZoneID, zoneID},
					SecretID: ids.NewAt(ids.KindSecret, at, 5),
				}},
			},
			{
				ID: ids.NewAt(ids.KindComponent, at, 6), Owner: core.ComponentOwnerEnvironment,
				OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
				PinnedIPv4: "10.40.10.254", GeneratedServices: []string{ids.NewAt(ids.KindService, at, 7)},
				Config: core.ComponentConfig{
					Caddy: &core.CaddyComponentConfig{ZoneIDs: []string{zoneID, privateZoneID}},
				},
			},
		},
	}
	catalog, err := registeredEnvironmentComponentCatalog()
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := controller.RenderEnvironmentComponents(environment, catalog)
	if err != nil {
		t.Fatal(err)
	}
	for _, generated := range rendered.Services {
		service := generated.Definition
		if service.ID != tunnelServiceID {
			continue
		}
		if service.NetworkMode != componentsdk.ManagedNetworkModeZones || len(service.Networks) != 2 ||
			service.Networks[0].Name != "private" || service.Networks[0].StaticIPv4 != "" ||
			service.Networks[0].GatewayPriority != 0 || service.Networks[1].Name != "public-edge" ||
			service.Networks[1].GatewayPriority != 1 || service.Networks[1].StaticIPv4 != "" ||
			len(service.Dependencies) != 0 {
			t.Fatalf("Tunnel must honor selected Zones and egress without router lifecycle dependency: %#v", service)
		}
		return
	}
	t.Fatal("compiled Tunnel Service is absent")
}
