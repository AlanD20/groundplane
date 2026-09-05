package dnsresolver

import (
	"crypto/sha256"
	"net/netip"
	"testing"
)

func TestResolverInputCanonicalizesAndValidatesImmutableSources(t *testing.T) {
	baseline, err := NewResolverBaseline(1, []ResolverEndpoint{
		{Address: netip.MustParseAddr("8.8.8.8")},
		{Address: netip.MustParseAddr("1.1.1.1"), Port: 53},
	})
	if err != nil {
		t.Fatalf("NewResolverBaseline() error = %v", err)
	}
	digest := sha256.Sum256([]byte("revision-bound-hosts"))
	projection, err := NewHostResolutionProjection(7, digest, []Host{
		{Address: netip.MustParseAddr("192.0.2.10"), Hostnames: []string{"api.example.test", "admin.example.test"}},
		{Address: netip.MustParseAddr("10.25.0.3"), Hostnames: []string{"web.example.test"}},
	})
	if err != nil {
		t.Fatalf("NewHostResolutionProjection() error = %v", err)
	}
	if err := (ResolverInput{Baseline: baseline, HostResolution: projection}).Validate(); err != nil {
		t.Fatalf("ResolverInput.Validate() error = %v", err)
	}
	if len(baseline.Resolvers) != 2 || baseline.Resolvers[0].Address.String() != "1.1.1.1" ||
		baseline.Resolvers[1].Address.String() != "8.8.8.8" || len(projection.Hosts) != 2 ||
		projection.Hosts[0].Address.String() != "10.25.0.3" ||
		projection.Hosts[1].Address.String() != "192.0.2.10" {
		t.Fatalf("input was not canonical: %#v", projection)
	}
}

func TestHostResolutionProjectionRejectsAmbiguousHostname(t *testing.T) {
	digest := sha256.Sum256([]byte("ambiguous"))
	_, err := NewHostResolutionProjection(1, digest, []Host{
		{Address: netip.MustParseAddr("192.0.2.10"), Hostnames: []string{"app.example.test"}},
		{Address: netip.MustParseAddr("10.25.0.3"), Hostnames: []string{"app.example.test"}},
	})
	if err == nil {
		t.Fatal("NewHostResolutionProjection() accepted an ambiguous hostname")
	}
}
