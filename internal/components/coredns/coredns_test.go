package coredns

import (
	"errors"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: normalized CoreDNS input must produce byte-identical Corefiles
// regardless of input ordering, so reloads are reproducible.
func TestRenderCorefileIsDeterministic(t *testing.T) {
	t.Parallel()
	input := CoreDNSRenderInput{
		Hosts: []CoreDNSHost{
			{Address: netip.MustParseAddr("10.200.40.7"), Hostnames: []string{"admin.example.com"}},
			{
				Address:   netip.MustParseAddr("10.200.30.4"),
				Hostnames: []string{"app.example.com", "api.example.com"},
			},
		},
		Forwarders: []CoreDNSForwarder{
			{Domain: "home.arpa", Resolvers: []ResolverEndpoint{resolver("192.168.1.1", 0)}},
			{Domain: "lab.home.arpa", Resolvers: []ResolverEndpoint{resolver("10.0.0.53", 0)}},
			{Domain: "ts.net", Resolvers: []ResolverEndpoint{resolver("100.100.100.100", 0)}},
		},
		CatchAll: []ResolverEndpoint{resolver("8.8.8.8", 53), resolver("1.1.1.1", 0)},
	}
	want := ".:53 {\n" +
		"    bind 127.0.0.1\n" +
		"    hosts {\n" +
		"        10.200.30.4 api.example.com app.example.com\n" +
		"        10.200.40.7 admin.example.com\n" +
		"        no_reverse\n" +
		"        fallthrough\n" +
		"    }\n" +
		"    forward lab.home.arpa 10.0.0.53\n" +
		"    forward home.arpa 192.168.1.1\n" +
		"    forward ts.net 100.100.100.100\n" +
		"    forward . 1.1.1.1 8.8.8.8\n" +
		"    reload\n" +
		"    prometheus 127.0.0.1:9153\n" +
		"    log\n" +
		"    errors\n" +
		"}\n"

	got, err := RenderCorefile(input)
	if err != nil {
		t.Fatalf("RenderCorefile() error = %v", err)
	}
	if string(got) != want {
		t.Fatalf("RenderCorefile() = %q, want %q", got, want)
	}

	reversed := input
	reversed.Hosts = slices.Clone(input.Hosts)
	slices.Reverse(reversed.Hosts)
	reversed.Forwarders = slices.Clone(input.Forwarders)
	slices.Reverse(reversed.Forwarders)
	reversed.CatchAll = slices.Clone(input.CatchAll)
	slices.Reverse(reversed.CatchAll)
	for index := range reversed.Hosts {
		reversed.Hosts[index].Hostnames = slices.Clone(reversed.Hosts[index].Hostnames)
		slices.Reverse(reversed.Hosts[index].Hostnames)
	}

	again, err := RenderCorefile(reversed)
	if err != nil {
		t.Fatalf("RenderCorefile(reversed) error = %v", err)
	}
	if !slices.Equal(again, got) {
		t.Fatalf("RenderCorefile(reversed) = %q, want byte-identical %q", again, got)
	}
}

// Rationale: split-horizon records must retain each Environment's applied
// Caddy address and reject an ambiguous hostname before rendering.
func TestRenderCorefilePreservesEnvironmentAddressesAndRejectsConflicts(t *testing.T) {
	t.Parallel()
	input := CoreDNSRenderInput{
		Hosts: []CoreDNSHost{
			{Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"api.example.com"}},
			{Address: netip.MustParseAddr("10.200.40.7"), Hostnames: []string{"admin.example.com"}},
		},
		CatchAll: []ResolverEndpoint{resolver("1.1.1.1", 0)},
	}
	rendered, err := RenderCorefile(input)
	if err != nil {
		t.Fatalf("RenderCorefile() error = %v", err)
	}
	if !strings.Contains(string(rendered), "10.200.30.4 api.example.com") ||
		!strings.Contains(string(rendered), "10.200.40.7 admin.example.com") {
		t.Fatalf("RenderCorefile() lost environment-specific addresses: %q", rendered)
	}

	input.Hosts[1].Hostnames = []string{"api.example.com"}
	rendered, err = RenderCorefile(input)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || rendered != nil {
		t.Fatalf("RenderCorefile(conflict) = %q, %v, want nil validation failure", rendered, err)
	}
}

// Rationale: invalid resolver inputs must fail before bytes exist, preserving
// the last-known-good CoreDNS configuration during a candidate reload.
func TestRenderCorefileRejectsInvalidInputBeforeProducingBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input CoreDNSRenderInput
	}{
		{
			name: "IPv6 static host",
			input: CoreDNSRenderInput{
				Hosts:    []CoreDNSHost{{Address: netip.MustParseAddr("2001:db8::1"), Hostnames: []string{"api.example.com"}}},
				CatchAll: []ResolverEndpoint{resolver("1.1.1.1", 0)},
			},
		},
		{
			name: "wildcard hostname",
			input: CoreDNSRenderInput{
				Hosts:    []CoreDNSHost{{Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"*.example.com"}}},
				CatchAll: []ResolverEndpoint{resolver("1.1.1.1", 0)},
			},
		},
		{
			name: "IP literal hostname",
			input: CoreDNSRenderInput{
				Hosts:    []CoreDNSHost{{Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"192.0.2.10"}}},
				CatchAll: []ResolverEndpoint{resolver("1.1.1.1", 0)},
			},
		},
		{
			name: "catch-all forwarder domain",
			input: CoreDNSRenderInput{
				Forwarders: []CoreDNSForwarder{{Domain: ".", Resolvers: []ResolverEndpoint{resolver("10.0.0.53", 0)}}},
				CatchAll:   []ResolverEndpoint{resolver("1.1.1.1", 0)},
			},
		},
		{
			name: "duplicate forwarder domain",
			input: CoreDNSRenderInput{
				Forwarders: []CoreDNSForwarder{
					{Domain: "home.arpa", Resolvers: []ResolverEndpoint{resolver("10.0.0.53", 0)}},
					{Domain: "home.arpa", Resolvers: []ResolverEndpoint{resolver("10.0.0.54", 0)}},
				},
				CatchAll: []ResolverEndpoint{resolver("1.1.1.1", 0)},
			},
		},
		{
			name: "loopback resolver",
			input: CoreDNSRenderInput{
				CatchAll: []ResolverEndpoint{resolver("127.0.0.1", 0)},
			},
		},
		{
			name: "duplicate normalized resolver",
			input: CoreDNSRenderInput{
				CatchAll: []ResolverEndpoint{resolver("1.1.1.1", 0), resolver("1.1.1.1", 53)},
			},
		},
		{name: "missing catch-all", input: CoreDNSRenderInput{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rendered, err := RenderCorefile(test.input)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || rendered != nil {
				t.Fatalf("RenderCorefile() = %q, %v, want nil validation failure", rendered, err)
			}
		})
	}
}

// Rationale: an empty static-record projection must not emit a misleading
// hosts block while still retaining the fixed resolver contract.
func TestRenderCorefileOmitsEmptyHostsBlock(t *testing.T) {
	t.Parallel()
	rendered, err := RenderCorefile(CoreDNSRenderInput{
		CatchAll: []ResolverEndpoint{resolver("2001:4860:4860::8888", 853)},
	})
	if err != nil {
		t.Fatalf("RenderCorefile() error = %v", err)
	}
	want := ".:53 {\n" +
		"    bind 127.0.0.1\n" +
		"    forward . [2001:4860:4860::8888]:853\n" +
		"    reload\n" +
		"    prometheus 127.0.0.1:9153\n" +
		"    log\n" +
		"    errors\n" +
		"}\n"
	if string(rendered) != want {
		t.Fatalf("RenderCorefile() = %q, want %q", rendered, want)
	}
}

func TestRenderCorefileAllowsHostResolverStub(t *testing.T) {
	t.Parallel()
	if _, err := RenderCorefile(CoreDNSRenderInput{
		CatchAll: []ResolverEndpoint{resolver("127.0.0.53", 53)},
	}); err != nil {
		t.Fatalf("RenderCorefile() rejected host resolver stub: %v", err)
	}
}

func resolver(address string, port uint16) ResolverEndpoint {
	return ResolverEndpoint{Address: netip.MustParseAddr(address), Port: port}
}
