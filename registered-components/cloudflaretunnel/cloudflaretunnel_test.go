package cloudflaretunnel

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

// Rationale: the registered planner must expose only an opaque Secret id and
// start the remotely managed connector without configuring provider routing.
func TestPlanBuildsSecretBoundTunnelConnector(t *testing.T) {
	t.Parallel()
	plan, err := Plan(Input{
		GeneratedServiceID: "svc_tunnel",
		SecretID:           "sec_token",
	})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(plan.Services) != 1 {
		t.Fatalf("Plan() services = %#v", plan.Services)
	}
	service := plan.Services[0]
	if service.Name != ServiceName || service.NetworkMode != component.ManagedNetworkModeDefault ||
		!service.Image.Equal(Image) ||
		len(service.Networks) != 0 || !slices.Equal(service.Command, []string{"tunnel", "--no-autoupdate", "run"}) ||
		len(service.Dependencies) != 0 ||
		len(service.SecretEnvironment) != 1 || service.SecretEnvironment[0].Name != tokenName ||
		service.SecretEnvironment[0].SecretID != "sec_token" {
		t.Fatalf("Plan() service = %#v", service)
	}
}

// Rationale: a planner must reject incomplete capability observations before
// it can emit a managed Service intent.
func TestPlanRejectsMissingConnectorIdentity(t *testing.T) {
	t.Parallel()
	if plan, err := Plan(Input{SecretID: "sec_token"}); err == nil || len(plan.Services) != 0 {
		t.Fatalf("Plan() = %#v, %v, want empty plan and error", plan, err)
	}
}
