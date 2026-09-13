package dnsresolver

import (
	"crypto/sha256"
	"net/netip"
	"testing"
)

// QA: DNS-01, DNS-02; pure immutable-input canonicalization only, not resolver materialization or DNS answers.
// Rationale: fixed-revision resolver inputs must own deterministic copies of
// their baseline endpoints and Route-derived host projection.
func TestResolverInputCanonicalizesAndValidatesImmutableSources(t *testing.T) {
	resolvers := []ResolverEndpoint{
		{Address: netip.MustParseAddr("8.8.8.8")},
		{Address: netip.MustParseAddr("1.1.1.1"), Port: 53},
	}
	baseline, err := NewResolverBaseline(1, resolvers)
	if err != nil {
		t.Fatalf("NewResolverBaseline() error = %v", err)
	}
	digest := sha256.Sum256([]byte("revision-bound-hosts"))
	hosts := []Host{
		{Address: netip.MustParseAddr("192.0.2.10"), Hostnames: []string{"api.example.test", "admin.example.test"}},
		{Address: netip.MustParseAddr("10.25.0.3"), Hostnames: []string{"web.example.test"}},
	}
	projection, err := NewHostResolutionProjection(7, digest, hosts)
	if err != nil {
		t.Fatalf("NewHostResolutionProjection() error = %v", err)
	}
	if err := (ResolverInput{Baseline: baseline, HostResolution: projection}).Validate(); err != nil {
		t.Fatalf("ResolverInput.Validate() error = %v", err)
	}
	resolvers[0].Address = netip.MustParseAddr("9.9.9.9")
	hosts[0].Hostnames[0] = "mutated.example.test"
	if baseline.Generation != 1 || len(baseline.Resolvers) != 2 ||
		baseline.Resolvers[0].Address.String() != "1.1.1.1" || baseline.Resolvers[0].Port != 53 ||
		baseline.Resolvers[1].Address.String() != "8.8.8.8" || len(projection.Hosts) != 2 ||
		projection.InputRevision != 7 || projection.InputSHA256 != digest ||
		projection.Hosts[0].Address.String() != "10.25.0.3" ||
		projection.Hosts[1].Address.String() != "192.0.2.10" ||
		len(projection.Hosts[1].Hostnames) != 2 || projection.Hosts[1].Hostnames[0] != "admin.example.test" ||
		projection.Hosts[1].Hostnames[1] != "api.example.test" {
		t.Fatalf("input was not canonical and detached: baseline=%#v projection=%#v", baseline, projection)
	}
}

// QA: DNS-01, DNS-02; pure host-projection rejection only, not serving-config retention or real DNS behavior.
// Rationale: one hostname cannot resolve to two router addresses in the same
// immutable input because selection by input order would be ambiguous.
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
