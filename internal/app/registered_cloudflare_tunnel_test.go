package app

import (
	"crypto/sha256"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: production composition must project the selected router's typed
// managed-Service origin into cloudflared without Cloudflare knowing Caddy.
func TestPlanRegisteredCloudflareTunnelUsesProjectedCaddyOrigin(t *testing.T) {
	t.Parallel()
	const (
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		zoneID        = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		caddyID       = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		caddyService  = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		tunnelID      = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		tunnelService = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		secretID      = "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	digest := sha256.Sum256([]byte("registered catalog"))
	registeredRouter, err := registeredCaddyEnvironmentComponent(digest)
	if err != nil {
		t.Fatalf("registeredCaddyEnvironmentComponent() error = %v", err)
	}
	caddy := core.Component{
		ID: caddyID, Owner: core.ComponentOwnerEnvironment, OwnerID: environmentID,
		Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{ZoneID: zoneID}},
		GeneratedServices: []string{caddyService}, PinnedIPv4: "10.40.0.2",
	}
	environment := core.Environment{
		ID: environmentID,
		Zones: map[string]core.Zone{
			"frontend": {ID: zoneID, Name: "frontend", Subnet: "10.40.0.0/24"},
		},
		Components: []core.Component{caddy},
	}
	tunnel := core.Component{
		ID: tunnelID, Owner: core.ComponentOwnerEnvironment, OwnerID: environmentID,
		Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
		Config: core.ComponentConfig{
			CloudflareTunnel: &core.CloudflareTunnelComponentConfig{SecretID: secretID},
		},
		GeneratedServices: []string{tunnelService},
	}
	plan, err := planRegisteredCloudflareTunnel(
		environment,
		tunnel,
		[]controller.EnvironmentComponentRegistration{registeredRouter},
	)
	if err != nil {
		t.Fatalf("planRegisteredCloudflareTunnel() error = %v", err)
	}
	service := plan.Services[0]
	if !slices.Equal(service.Command, []string{"tunnel", "--no-autoupdate", "--url", "http://caddy:80", "run"}) ||
		len(service.Dependencies) != 1 || service.Dependencies[0].ServiceName != "caddy" {
		t.Fatalf("Cloudflare managed Service = %#v", service)
	}
}
