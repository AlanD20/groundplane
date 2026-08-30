package cloudflaretunnel

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

// Rationale: the registered planner must expose only an opaque Secret id and
// bind cloudflared exclusively to the selected HTTP-router origin network.
func TestPlanBuildsSecretBoundRouterTransport(t *testing.T) {
	t.Parallel()
	plan, err := Plan(Input{
		GeneratedServiceID: "svc_tunnel",
		RouterOrigin: component.HTTPRouterOrigin{
			ServiceName: "edge-router", URL: "http://edge-router:8080",
		},
		RouterNetworkName: "frontend", SecretID: "sec_token",
	})
	if err != nil { t.Fatalf("Plan() error = %v", err) }
	if len(plan.Services) != 1 { t.Fatalf("Plan() services = %#v", plan.Services) }
	service := plan.Services[0]
	if service.Name != ServiceName || len(service.Networks) != 1 || service.Networks[0].Name != "frontend" ||
		!slices.Equal(service.Command, []string{"tunnel", "--no-autoupdate", "--url", "http://edge-router:8080", "run"}) ||
		len(service.Dependencies) != 1 || service.Dependencies[0].ServiceName != "edge-router" ||
		len(service.SecretEnvironment) != 1 || service.SecretEnvironment[0].SecretID != "sec_token" {
		t.Fatalf("Plan() service = %#v", service)
	}
}

// Rationale: a planner must reject incomplete capability observations before
// it can emit a managed Service intent.
func TestPlanRejectsMissingRouter(t *testing.T) {
	t.Parallel()
	if plan, err := Plan(Input{GeneratedServiceID: "svc_tunnel", SecretID: "sec_token"}); err == nil || len(plan.Services) != 0 {
		t.Fatalf("Plan() = %#v, %v, want empty plan and error", plan, err)
	}
}
