package componentrender

import (
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	registeredcaddy "github.com/AlanD20/groundplane-registered-components/caddy"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: a Route target may be reached through any explicitly selected
// router Zone while the first selected Zone alone owns the stable address.
func TestProjectRegisteredCaddyInputPreservesOrderedZones(t *testing.T) {
	t.Parallel()
	const (
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		primaryID     = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		secondaryID   = "net_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	environment := core.Environment{
		ID: environmentID,
		Zones: map[string]core.Zone{
			"primary": {
				ID: primaryID, Name: "primary", Subnet: "10.40.0.0/24",
				OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
			},
			"secondary": {
				ID: secondaryID, Name: "secondary", Subnet: "10.40.1.0/24",
				OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
			},
		},
		Services: map[string]core.Service{"app": {
			ID: serviceID, Name: "app", Image: "app:1", Zones: []string{"secondary"}, Expose: []string{"8080"},
		}},
		Routes: []core.Route{{
			ID: "rte_01ARZ3NDEKTSV4RRFFQ69G5FAV", Host: "app.example.com", Path: "/",
			TargetServiceID: serviceID, TargetPort: 8080, Exposure: "public",
		}},
	}
	caddy := core.Component{
		ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Owner: core.ComponentOwnerEnvironment, OwnerID: environmentID,
		Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{primaryID, secondaryID},
		}},
		GeneratedServices: []string{"svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"}, PinnedIPv4: "10.40.0.2",
	}
	input, _, err := ProjectCaddyInput(environment, caddy, componentsdk.HTTPRouterOrigin{
		ServiceName: registeredcaddy.ServiceName,
		URL:         registeredcaddy.OriginURL,
	})
	if err != nil {
		t.Fatalf("ProjectCaddyInput() error = %v", err)
	}
	if len(input.Zones) != 2 || input.Zones[0].ID != primaryID || input.Zones[0].Name != "primary" ||
		input.Zones[0].StaticIPv4 != "10.40.0.2" || input.Zones[1].ID != secondaryID ||
		input.Zones[1].Name != "secondary" || input.Zones[1].StaticIPv4 != "" {
		t.Fatalf("ProjectCaddyInput() Zones = %#v", input.Zones)
	}
}
