package componentregistration

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	registeredtunnel "github.com/AlanD20/groundplane-registered-components/cloudflaretunnel"
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	core "github.com/AlanD20/groundplane/internal/core"
	slices "slices"
	testing "testing"
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
	plan, err := testCloudflareTunnelPlan(environment, tunnel)
	if err != nil {
		t.Fatalf("PlanCloudflareTunnel() error = %v", err)
	}
	// Switching the same Component to its Environment Entry must select only
	// that opaque Entry, without retaining the former Project Secret.
	environment.Entries = []core.EnvEntry{
		{
			ID:       "ev_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Kind:     core.EntryKindEnv,
			Key:      "TOKEN",
			Secret:   true,
			Source:   core.EntrySource{Kind: core.SourceLiteral},
			Exposure: []string{core.EntryExposureEdgeTunnel},
		},
	}
	tunnel.Config.CloudflareTunnel.SecretID = ""
	tunnel.Config.CloudflareTunnel.TokenEntry = "TOKEN"
	entryPlan, err := testCloudflareTunnelPlan(environment, tunnel)
	if err != nil {
		t.Fatal(err)
	}
	binding := entryPlan.Services[0].SecretEnvironment[0]
	if binding.EntryID != environment.Entries[0].ID || binding.SecretID != "" {
		t.Fatal("planner retained another credential source")
	}
	if componentsdk.DigestEnvironmentPlan(entryPlan) == componentsdk.DigestEnvironmentPlan(plan) {
		t.Fatal("credential switch did not change the plan digest")
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
	if _, err := testCloudflareTunnelPlan(environment, tunnel); err == nil {
		t.Fatal("PlanCloudflareTunnel() accepted internal-only placement")
	}
	tunnel.Config.CloudflareTunnel.ZoneIDs[0] = "net_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	if _, err := testCloudflareTunnelPlan(environment, tunnel); err == nil {
		t.Fatal("PlanCloudflareTunnel() accepted a missing Zone")
	}
}

func testCloudflareTunnelPlan(
	environment core.Environment,
	tunnel core.Component,
) (componentsdk.EnvironmentPlan, error) {
	return componentrender.PlanCloudflareTunnel(
		environment,
		tunnel,
		func(serviceID, secretID, entryID string, zones []componentsdk.NetworkInput) (componentsdk.EnvironmentPlan, error) {
			return registeredtunnel.Plan(registeredtunnel.Input{
				GeneratedServiceID: serviceID,
				SecretID:           secretID,
				EntryID:            entryID,
				Zones:              zones,
			})
		},
	)
}
