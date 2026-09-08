package dnsresolver

import (
	"crypto/sha256"
	"encoding/hex"
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

func (planner environmentPlannerPin) Plan(
	_ componentsdk.ImplementationKey,
	serviceID string,
	input componentdns.RenderInput,
) (componentsdk.EnvironmentPlan, error) {
	return componentsdk.EnvironmentPlan{
		Services: []componentsdk.ManagedService{
			{
				ID:       serviceID,
				Name:     "resolver",
				Image:    testPlatformImage(planner.image),
				Command:  []string{"--config", "/etc/resolver/config"},
				Restart:  "unless-stopped",
				Replicas: 1,
				Mounts: []componentsdk.ManagedMount{
					{Source: "config", Target: "/etc/resolver/config", ReadOnly: true},
				},
			},
		},
		Files: []componentsdk.ManagedFile{{Path: "config", Content: []byte("same bytes\n")}},
	}, nil
}

func testPlatformImage(identity string) componentsdk.OCIImage {
	index := sha256.Sum256([]byte(identity + "/index"))
	amd64 := sha256.Sum256([]byte(identity + "/linux/amd64"))
	arm64 := sha256.Sum256([]byte(identity + "/linux/arm64/v8"))
	amd64Config := sha256.Sum256([]byte(identity + "/linux/amd64/config"))
	arm64Config := sha256.Sum256([]byte(identity + "/linux/arm64/config"))
	return componentsdk.OCIImage{
		Repository: identity, IndexDigest: hex.EncodeToString(index[:]),
		Platforms: []componentsdk.OCIPlatform{
			{
				OS:           "linux",
				Architecture: "amd64",
				ChildDigest:  hex.EncodeToString(amd64[:]),
				ConfigDigest: hex.EncodeToString(amd64Config[:]),
			},
			{
				OS:           "linux",
				Architecture: "arm64",
				Variant:      "v8",
				ChildDigest:  hex.EncodeToString(arm64[:]),
				ConfigDigest: hex.EncodeToString(arm64Config[:]),
			},
		},
	}
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
			CorefileTemplate: testCorefileTemplate,
			UpstreamAuto:     true, TailnetDelegation: true,
		}},
	}
	hosts := []componentdns.Host{{Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"app.example.com"}}}
	baseline := []componentdns.ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}}
	canonicalBaseline, err := componentdns.NewResolverBaseline(1, baseline)
	if err != nil {
		t.Fatalf("NewResolverBaseline() error = %v", err)
	}
	resolverInput := componentdns.ResolverInput{
		Baseline: canonicalBaseline,
		HostResolution: componentdns.HostResolutionProjection{
			InputRevision: 1, InputSHA256: [sha256.Size]byte{1}, Hosts: hosts,
		},
	}
	planner := environmentPlannerPin{image: "example/resolver:1"}
	first, err := BuildIntent(rendererPin{}, planner, component, resolverInput, "resolver")
	if err != nil {
		t.Fatalf("BuildIntent() error = %v", err)
	}
	second, err := BuildIntent(rendererPin{}, planner, component, resolverInput, "resolver")
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
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			CorefileTemplate: testCorefileTemplate,
			UpstreamAuto:     true,
		}},
	}
	hosts := []componentdns.Host{{Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"app.example.com"}}}
	baseline := []componentdns.ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}}
	canonicalBaseline, err := componentdns.NewResolverBaseline(1, baseline)
	if err != nil {
		t.Fatalf("NewResolverBaseline() error = %v", err)
	}
	resolverInput := componentdns.ResolverInput{
		Baseline: canonicalBaseline,
		HostResolution: componentdns.HostResolutionProjection{
			InputRevision: 1, InputSHA256: [sha256.Size]byte{1}, Hosts: hosts,
		},
	}
	first, err := BuildIntent(
		rendererPin{},
		environmentPlannerPin{image: "example/resolver:1"},
		component,
		resolverInput,
		"resolver",
	)
	if err != nil {
		t.Fatalf("BuildIntent() first error = %v", err)
	}
	second, err := BuildIntent(
		rendererPin{},
		environmentPlannerPin{image: "example/resolver:2"},
		component,
		resolverInput,
		"resolver",
	)
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

func TestBuildIntentUsesRegisteredResolverImplementation(t *testing.T) {
	t.Parallel()
	component := core.Component{
		ID: "cmp_alternate", Owner: core.ComponentOwnerPlatform, Kind: core.ComponentKind("alternate-resolver"),
		Enabled: true, GeneratedServices: []string{"svc_alternate"},
		Config: core.ComponentConfig{CoreDNS: &core.CoreDNSComponentConfig{
			CorefileTemplate: testCorefileTemplate,
			UpstreamAuto:     true,
		}},
	}
	baseline, err := componentdns.NewResolverBaseline(
		1,
		[]componentdns.ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}},
	)
	if err != nil {
		t.Fatalf("NewResolverBaseline() error = %v", err)
	}
	input := componentdns.ResolverInput{
		Baseline: baseline,
		HostResolution: componentdns.HostResolutionProjection{
			InputRevision: 1, InputSHA256: [sha256.Size]byte{1},
		},
	}
	intent, err := BuildIntent(
		rendererPin{}, environmentPlannerPin{image: "example/alternate:1"}, component, input,
		componentsdk.ImplementationKey("alternate-resolver"),
	)
	if err != nil {
		t.Fatalf("BuildIntent() error = %v", err)
	}
	if intent.ComponentID != component.ID || intent.ServiceID != component.GeneratedServices[0] {
		t.Fatalf("BuildIntent() identity = %#v", intent)
	}
}
