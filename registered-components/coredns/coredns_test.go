package coredns

import (
	"bytes"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane-component-sdk/dnsresolver"
)

// Rationale: normalized DNS input must produce byte-identical Corefiles
// regardless of input order so catalog actions remain reproducible.
func TestRendererIsDeterministic(t *testing.T) {
	t.Parallel()
	input := dnsresolver.RenderInput{
		CorefileTemplate: DefaultCorefileTemplate,
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
		CorefileTemplate: DefaultCorefileTemplate,
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

// Rationale: the immutable CoreDNS validation action appends -dns.port 0 to
// the image entrypoint, but CoreDNS cannot override an explicit Corefile port.
// The serving artifact must therefore use the default-port form so validation
// can bind an ephemeral port while the ordinary service still defaults to 53.
func TestRendererKeepsValidationPortOverridable(t *testing.T) {
	t.Parallel()
	plan, err := Plan(PlanInput{
		GeneratedServiceID: "svc_test",
		Render: dnsresolver.RenderInput{
			CorefileTemplate: DefaultCorefileTemplate,
			CatchAll:         []dnsresolver.ResolverEndpoint{resolver("1.1.1.1", 53)},
		},
	})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(plan.Files) != 1 || !bytes.HasPrefix(plan.Files[0].Content, []byte(". {\n")) {
		t.Fatalf("Plan() Corefile = %q, want an overridable default-port server block", plan.Files)
	}
	if !slices.Equal(ValidateConfigCommand(), []string{"-conf", "/dev/stdin", "-dns.port", "0"}) ||
		len(plan.Services) != 1 ||
		!slices.Equal(plan.Services[0].Command, []string{"-conf", CorefileTarget}) {
		t.Fatalf("Plan() validation/serving argv = %q / %#v", ValidateConfigCommand(), plan.Services)
	}
}

// Rationale: the registered action and serving Service must use the same
// immutable CoreDNS image, validation argv, and serving observation authority.
func TestPlanPinsServingImageValidationAndHealthObservation(t *testing.T) {
	t.Parallel()
	plan, err := Plan(PlanInput{
		GeneratedServiceID: "svc_test",
		Render: dnsresolver.RenderInput{
			CorefileTemplate: DefaultCorefileTemplate,
			CatchAll:         []dnsresolver.ResolverEndpoint{resolver("1.1.1.1", 53)},
		},
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

// Rationale: the canonical operator template must preserve the historical
// Corefile shape while expanding only the Controller-owned marker.
func TestRendererExpandsCanonicalTemplateExactly(t *testing.T) {
	t.Parallel()
	rendered, err := (Renderer{}).Render(dnsresolver.RenderInput{
		CorefileTemplate: DefaultCorefileTemplate,
		CatchAll:         []dnsresolver.ResolverEndpoint{resolver("1.1.1.1", 0)},
	})
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	want := []byte(". {\n    bind 127.0.0.1\n    forward . 1.1.1.1\n    reload\n    prometheus 127.0.0.1:9153\n    log\n    errors\n}\n")
	if !bytes.Equal(rendered, want) {
		t.Fatalf("Render() = %q, want %q", rendered, want)
	}
}

// Rationale: malformed or unbounded operator templates must fail before any
// Corefile bytes exist so the last-known-good serving configuration survives.
func TestRendererRejectsInvalidCorefileTemplate(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"empty":            "",
		"missing marker":   ". {\n    errors\n}\n",
		"duplicate marker": ". {\n    {groundplane}\n    {groundplane}\n}\n",
		"missing final LF": ". {\n    {groundplane}\n}",
		"CRLF":             ". {\r\n    {groundplane}\r\n}\r\n",
		"NUL":              ". {\n    {groundplane}\x00\n}\n",
		"invalid UTF-8":    ". {\n    {groundplane}\n" + string([]byte{0xff}) + "}\n",
		"too large":        "{groundplane}" + strings.Repeat("x", maxCorefileTemplateBytes) + "\n",
	}
	for name, template := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rendered, err := (Renderer{}).Render(dnsresolver.RenderInput{
				CorefileTemplate: template,
				CatchAll:         []dnsresolver.ResolverEndpoint{resolver("1.1.1.1", 0)},
			})
			if err == nil || rendered != nil {
				t.Fatalf("Render() = %q, %v, want nil bytes and an error", rendered, err)
			}
		})
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
