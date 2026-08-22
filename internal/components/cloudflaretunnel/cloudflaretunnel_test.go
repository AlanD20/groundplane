package cloudflaretunnel

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the generated service must connect only through Caddy and map
// the durable operator Entry to cloudflared's actual token variable without
// placing token bytes in Compose or component config.
func TestRenderBuildsCaddyBoundServiceWithSecretEntryBinding(t *testing.T) {
	environment, candidate := cloudflareTunnelTestInput()
	services, files, err := (&component{}).Render(environment, candidate)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	generated, ok := services[cloudflaredServiceName]
	if !ok || generated.Service.ID != candidate.GeneratedServices[0] ||
		generated.Service.Image != cloudflaredImage ||
		len(generated.Service.Zones) != 1 || generated.Service.Zones[0] != "frontend" ||
		generated.Service.DependsOn["caddy"].Condition != "service_started" ||
		len(generated.SecretEnvironment) != 1 ||
		generated.SecretEnvironment[0].Name != cloudflaredTokenKey ||
		generated.SecretEnvironment[0].EntryID != "ev_token" || len(files) != 0 {
		t.Fatalf("Render() service/files = %#v / %#v", generated, files)
	}
	if got := generated.Service.Command; len(got) != 3 || got[0] != "tunnel" ||
		got[1] != "--no-autoupdate" || got[2] != "run" {
		t.Fatalf("cloudflared command = %#v", got)
	}
}

// Rationale: Tunnel without Caddy or with a broadly exposed/wrong-key token
// would bypass the MVP's sole ingress path and secret-isolation boundary.
func TestRenderRejectsInvalidDependencyAndTokenScope(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*core.Environment)
	}{
		{
			name: "Caddy disabled",
			mutate: func(environment *core.Environment) {
				environment.Components[0].Enabled = false
			},
		},
		{
			name: "wrong token key",
			mutate: func(environment *core.Environment) {
				environment.Entries[0].Key = "TUNNEL_TOKEN"
			},
		},
		{
			name: "broad token exposure",
			mutate: func(environment *core.Environment) {
				environment.Entries[0].Exposure = []string{"all"}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment, candidate := cloudflareTunnelTestInput()
			test.mutate(&environment)
			_, _, err := (&component{}).Render(environment, candidate)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Render() error = %v, want validation.failed", err)
			}
		})
	}
}

func cloudflareTunnelTestInput() (core.Environment, core.Component) {
	caddy := core.Component{
		ID: "cmp_caddy", Owner: core.ComponentOwnerEnvironment, OwnerID: "env_test",
		Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: map[string]any{"zone_id": "net_frontend"},
	}
	environment := core.Environment{
		ID: "env_test",
		Zones: map[string]core.Zone{
			"frontend": {ID: "net_frontend", Name: "frontend", Subnet: "10.40.10.0/24"},
		},
		Components: []core.Component{caddy},
		Entries: []core.EnvEntry{{
			ID: "ev_token", Kind: core.EntryKindEnv, Key: operatorTokenKey,
			Source:   core.EntrySource{Kind: core.SourceSecretRef, SecretRef: "sec_token"},
			Exposure: []string{cloudflaredServiceName}, Secret: true,
		}},
	}
	component := core.Component{
		ID: "cmp_tunnel", Owner: core.ComponentOwnerEnvironment, OwnerID: environment.ID,
		Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
		Config:            map[string]any{"token_entry_id": "ev_token"},
		GeneratedServices: []string{"svc_cloudflared"}, Healthy: true,
	}
	return environment, component
}
