package cloudflaretunnel

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

// QA: CMP-01, NET-03, HTTP-08; pure Tunnel plan output only, not Secret resolution or provider ingress.
// Rationale: the registered planner must expose only an opaque Secret id and
// start the remotely managed connector without configuring provider routing.
func TestPlanBuildsSecretBoundTunnelConnector(t *testing.T) {
	t.Parallel()
	plan, err := Plan(Input{
		GeneratedServiceID: "svc_tunnel",
		SecretID:           "sec_token",
		Zones: []component.NetworkInput{
			{ID: "net_private", Name: "private", Internal: true},
			{ID: "net_frontend", Name: "frontend"},
			{ID: "net_services", Name: "services"},
		},
	})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(plan.Services) != 1 {
		t.Fatalf("Plan() services = %#v", plan.Services)
	}
	service := plan.Services[0]
	if service.Name != ServiceName || service.NetworkMode != component.ManagedNetworkModeZones ||
		!service.Image.Equal(Image) ||
		!slices.EqualFunc(service.Networks, []component.ManagedNetworkAttachment{
			{Name: "private"},
			{Name: "frontend", GatewayPriority: 1},
			{Name: "services"},
		}, func(actual, expected component.ManagedNetworkAttachment) bool {
			return actual.Name == expected.Name &&
				(actual.Aliases == nil) == (expected.Aliases == nil) &&
				slices.Equal(actual.Aliases, expected.Aliases) &&
				actual.StaticIPv4 == expected.StaticIPv4 &&
				actual.GatewayPriority == expected.GatewayPriority
		}) || !slices.Equal(service.Command, []string{"tunnel", "--no-autoupdate", "--metrics", "127.0.0.1:2000", "run"}) ||
		len(service.Dependencies) != 0 ||
		len(service.SecretEnvironment) != 1 || service.SecretEnvironment[0].Name != tokenName ||
		service.SecretEnvironment[0].SecretID != "sec_token" {
		t.Fatalf("Plan() service = %#v", service)
	}
	if service.Healthcheck == nil || service.Healthcheck.Validate() != nil ||
		!slices.Equal(
			service.Healthcheck.Command,
			[]string{"cloudflared", "tunnel", "--metrics", "127.0.0.1:2000", "ready"},
		) {
		t.Fatal("Tunnel readiness must query its bound loopback metrics endpoint")
	}
}

// QA: CMP-04; pure planner rejection only, not request-boundary validation or publication.
// Rationale: a planner must reject incomplete capability observations before
// it can emit a managed Service intent.
func TestPlanRejectsMissingConnectorIdentity(t *testing.T) {
	t.Parallel()
	if plan, err := Plan(Input{SecretID: "sec_token"}); err == nil || len(plan.Services) != 0 {
		t.Fatalf("Plan() = %#v, %v, want empty plan and error", plan, err)
	}
}

// QA: CMP-04, NET-03, HTTP-08; pure placement rejection only, not network attachment or egress.
// Rationale: the connector must never receive an implicit default bridge or
// silently treat an internal-only placement as internet egress.
func TestPlanRejectsInvalidZonePlacement(t *testing.T) {
	t.Parallel()
	valid := Input{
		GeneratedServiceID: "svc_tunnel", SecretID: "sec_token",
		Zones: []component.NetworkInput{{ID: "net_frontend", Name: "frontend"}},
	}
	for name, mutate := range map[string]func(*Input){
		"missing zones": func(input *Input) { input.Zones = nil },
		"internal only": func(input *Input) { input.Zones[0].Internal = true },
		"duplicate id":  func(input *Input) { input.Zones = append(input.Zones, input.Zones[0]) },
		"duplicate name": func(input *Input) {
			input.Zones = append(input.Zones, component.NetworkInput{ID: "net_other", Name: input.Zones[0].Name})
		},
	} {
		t.Run(name, func(t *testing.T) {
			input := valid
			input.Zones = append([]component.NetworkInput(nil), valid.Zones...)
			mutate(&input)
			if plan, err := Plan(input); err == nil || len(plan.Services) != 0 {
				t.Fatalf("Plan() = %#v, %v, want empty plan and error", plan, err)
			}
		})
	}
}
