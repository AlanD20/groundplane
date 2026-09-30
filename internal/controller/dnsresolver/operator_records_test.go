package dnsresolver

import (
	"context"
	"net/netip"
	"testing"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type emptyDNSRecordsResolver struct{}

func (emptyDNSRecordsResolver) ResolveDNSRecords(
	_ context.Context,
	records []core.DNSRecord,
	_ int64,
) ([]platformcomponents.PlatformDNSHost, []keyvalue.Condition, error) {
	if len(records) != 0 {
		return nil, nil, errs.New(errs.KindInternal, "test fixture cannot resolve DNS references")
	}
	return nil, nil, nil
}

// Rationale: operator records cannot override an existing Route's address;
// matching duplicates must not cause duplicate resolver output.
func TestOperatorRecordsMergeRejectsRouteCollision(t *testing.T) {
	projection, err := componentdns.NewHostResolutionProjection(
		1,
		[32]byte{1},
		[]componentdns.Host{{Address: netip.MustParseAddr("10.20.0.2"), Hostnames: []string{"app.internal"}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, hosts, err := mergeResolverHosts(
		projection,
		[]platformcomponents.PlatformDNSHost{
			{Address: "10.20.0.2", Hostnames: []string{"app.internal", "other.internal"}},
		},
	)
	if err != nil || len(hosts) != 1 || len(hosts[0].Hostnames) != 2 {
		t.Fatalf("merged records = %#v, %v", hosts, err)
	}
	if _, _, err := mergeResolverHosts(projection, []platformcomponents.PlatformDNSHost{{Address: "10.20.0.3", Hostnames: []string{"app.internal"}}}); err == nil {
		t.Fatal("operator record replaced the Route's address")
	}
}
