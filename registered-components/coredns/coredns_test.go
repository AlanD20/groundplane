package coredns

import (
	"net/netip"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane-component-sdk/dnsresolver"
)

// Rationale: normalized DNS input must produce byte-identical Corefiles
// regardless of input order so catalog actions remain reproducible.
func TestRendererIsDeterministic(t *testing.T) {
	t.Parallel()
	input := dnsresolver.RenderInput{
		Hosts: []dnsresolver.Host{
			{Address: netip.MustParseAddr("10.200.40.7"), Hostnames: []string{"admin.example.com"}},
			{Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"app.example.com", "api.example.com"}},
		},
		Forwarders: []dnsresolver.Forwarder{
			{Domain: "home.arpa", Resolvers: []dnsresolver.ResolverEndpoint{resolver("192.168.1.1", 0)}},
			{Domain: "lab.home.arpa", Resolvers: []dnsresolver.ResolverEndpoint{resolver("10.0.0.53", 0)}},
		},
		CatchAll: []dnsresolver.ResolverEndpoint{resolver("8.8.8.8", 53), resolver("1.1.1.1", 0)},
	}
	renderer := Renderer{}
	first, err := renderer.Render(input)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	reversed := dnsresolver.CloneRenderInput(input)
	slices.Reverse(reversed.Hosts)
	slices.Reverse(reversed.Forwarders)
	slices.Reverse(reversed.CatchAll)
	second, err := renderer.Render(reversed)
	if err != nil {
		t.Fatalf("Render(reversed) error = %v", err)
	}
	if !slices.Equal(first, second) {
		t.Fatalf("Render(reversed) = %q, want %q", second, first)
	}
	firstDigest, err := renderer.Digest(input)
	if err != nil {
		t.Fatalf("Digest() error = %v", err)
	}
	secondDigest, err := renderer.Digest(reversed)
	if err != nil || firstDigest != secondDigest {
		t.Fatalf("Digest(reversed) = %x, %v, want %x", secondDigest, err, firstDigest)
	}
}

// Rationale: an ambiguous hostname must fail before Corefile bytes exist so
// the last-known-good DNS configuration remains active.
func TestRendererRejectsConflictingHosts(t *testing.T) {
	t.Parallel()
	input := dnsresolver.RenderInput{
		Hosts: []dnsresolver.Host{
			{Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"api.example.com"}},
			{Address: netip.MustParseAddr("10.200.40.7"), Hostnames: []string{"api.example.com"}},
		},
		CatchAll: []dnsresolver.ResolverEndpoint{resolver("1.1.1.1", 0)},
	}
	if rendered, err := (Renderer{}).Render(input); err == nil || rendered != nil {
		t.Fatalf("Render() = %q, %v, want nil bytes and an error", rendered, err)
	}
}

func TestPlanPinsServingImageValidationAndHealthObservation(t *testing.T) {
	t.Parallel()
	plan, err := Plan(PlanInput{
		GeneratedServiceID: "svc_test",
		Render:             dnsresolver.RenderInput{CatchAll: []dnsresolver.ResolverEndpoint{resolver("1.1.1.1", 53)}},
	})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(plan.Services) != 1 || !sameOCIImage(plan.Services[0].Image, Image) ||
		plan.Services[0].ObservationAction != ObserveServingAction ||
		!slices.Equal(ValidateConfigCommand(), []string{"-conf", "/dev/stdin", "-dns.port", "0"}) {
		t.Fatalf("Plan() serving recipe = %#v", plan.Services)
	}
}

func sameOCIImage(left, right component.OCIImage) bool {
	if left.Repository != right.Repository || left.IndexDigest != right.IndexDigest ||
		len(left.Platforms) != len(right.Platforms) {
		return false
	}
	for index, platform := range left.Platforms {
		if platform != right.Platforms[index] {
			return false
		}
	}
	return true
}

func resolver(address string, port uint16) dnsresolver.ResolverEndpoint {
	return dnsresolver.ResolverEndpoint{Address: netip.MustParseAddr(address), Port: port}
}
