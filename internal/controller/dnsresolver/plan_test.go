package dnsresolver

import (
	"crypto/sha256"
	"encoding/json"
	"net/netip"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/core"
)

type rendererPin struct{}

type environmentPlannerPin struct {
	image string
}

func (planner environmentPlannerPin) Plan(_ componentsdk.ImplementationKey, serviceID string, input componentdns.RenderInput) (componentsdk.EnvironmentPlan, error) {
	return componentsdk.EnvironmentPlan{
		Services: []componentsdk.ManagedService{{
			ID: serviceID, Name: "resolver", Image: planner.image,
			Command: []string{"--config", "/etc/resolver/config"}, Restart: "unless-stopped",
			Replicas: 1, Mounts: []componentsdk.ManagedMount{{Source: "config", Target: "/etc/resolver/config", ReadOnly: true}},
		}},
		Files: []componentsdk.ManagedFile{{Path: "config", Content: []byte("same bytes\n")}},
	}, nil
}

func (rendererPin) Render(input componentdns.RenderInput) ([]byte, error) {
	encoded, err := json.Marshal(input)
	return encoded, err
}

func (rendererPin) Digest(input componentdns.RenderInput) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

// Rationale: Controller planning must remain deterministic while consuming
// provider-neutral HTTP-router host observations.
func TestBuildIntentIsDeterministic(t *testing.T) {
	t.Parallel()
	component := core.Component{
		ID: "cmp_coredns", Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKindCoreDNS,
		Enabled: true, GeneratedServices: []string{"svc_coredns"},
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			UpstreamAuto: true, TailnetDelegation: true,
		}},
	}
	hosts := []componentdns.Host{{Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"app.example.com"}}}
	baseline := []componentdns.ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}}
	planner := environmentPlannerPin{image: "example/resolver:1"}
	first, err := BuildIntent(rendererPin{}, planner, component, hosts, baseline)
	if err != nil {
		t.Fatalf("BuildIntent() error = %v", err)
	}
	second, err := BuildIntent(rendererPin{}, planner, component, hosts, baseline)
	if err != nil {
		t.Fatalf("BuildIntent() second error = %v", err)
	}
	if first != second {
		t.Fatal("BuildIntent() was not deterministic")
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("Intent.Validate() error = %v", err)
	}
}

func TestBuildIntentRejectsChangedServiceReplay(t *testing.T) {
	t.Parallel()
	component := core.Component{
		ID: "cmp_coredns", Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKindCoreDNS,
		Enabled: true, GeneratedServices: []string{"svc_coredns"},
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{UpstreamAuto: true}},
	}
	hosts := []componentdns.Host{{Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"app.example.com"}}}
	baseline := []componentdns.ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}}
	first, err := BuildIntent(rendererPin{}, environmentPlannerPin{image: "example/resolver:1"}, component, hosts, baseline)
	if err != nil {
		t.Fatalf("BuildIntent() first error = %v", err)
	}
	second, err := BuildIntent(rendererPin{}, environmentPlannerPin{image: "example/resolver:2"}, component, hosts, baseline)
	if err != nil {
		t.Fatalf("BuildIntent() second error = %v", err)
	}
	if first.ArtifactSHA256 != second.ArtifactSHA256 || first.ArtifactLength != second.ArtifactLength {
		t.Fatal("replay fixture must retain identical managed file bytes")
	}
	if first.PlanSHA256 == second.PlanSHA256 {
		t.Fatal("BuildIntent() ignored changed service behavior")
	}
}
