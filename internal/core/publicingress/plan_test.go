package publicingress

import (
	"bytes"
	"net/netip"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

func TestBuildIsDeterministicAndKeepsSpecificRouteFirst(t *testing.T) {
	input := publicationFixture()
	root := input.Routes[0]
	root.Source.ID = ids.New(ids.KindRoute)
	root.Source.Path = "/"
	root.Endpoint.RouteID = root.Source.ID
	input.Routes = append(input.Routes, root)
	first, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Routes[0], input.Routes[1] = input.Routes[1], input.Routes[0]
	second, err := Build(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Configuration, second.Configuration) || second.Routes[0].Source.Path != "/api/*" {
		t.Fatal("publication depends on input order or routes the broad path before the specific path")
	}
}

func TestBuildRejectsAmbiguousOrUnverifiedPublication(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Input)
	}{
		{"internal path under public prefix", func(input *Input) {
			route := input.Routes[0]
			route.Source.Exposure = "internal"
			route.Source.Path = "/API/private/*"
			input.InternalRoutes = []InternalRoute{{Source: route.Source, EnvironmentID: route.EnvironmentID}}
		}},
		{"case alias", func(input *Input) {
			route := input.Routes[0]
			route.Source.ID = ids.New(ids.KindRoute)
			route.Source.Path = "/API/*"
			route.Endpoint.RouteID = route.Source.ID
			input.Routes = append(input.Routes, route)
		}},
		{"endpoint serves another Service", func(input *Input) {
			input.Routes[0].Endpoint.ServiceID = ids.New(ids.KindService)
		}},
		{"endpoint is public", func(input *Input) {
			input.Routes[0].Endpoint.Address = netip.MustParseAddrPort("8.8.8.8:8080")
		}},
		{"endpoint from future revision", func(input *Input) {
			input.Routes[0].Endpoint.AcknowledgedRevision = input.Revision + 1
		}},
		{"wildcard listener", func(input *Input) {
			input.Addresses = []netip.Addr{netip.IPv4Unspecified()}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := publicationFixture()
			test.mutate(&input)
			if plan, err := Build(input); err == nil || len(plan.Configuration) > 0 {
				t.Fatal("unsafe publication produced a usable Plan")
			}
		})
	}
}

func publicationFixture() Input {
	host := ids.New(ids.KindHost)
	environment := ids.New(ids.KindEnvironment)
	source := core.Route{
		ID: ids.New(ids.KindRoute), Host: "example.com", Path: "/api/*",
		TargetServiceID: ids.New(ids.KindService), TargetPort: 8080, Exposure: "public",
	}
	return Input{
		HostID: host, Revision: 5, Addresses: []netip.Addr{netip.MustParseAddr("192.0.2.20")},
		Routes: []Route{{
			Source: source, EnvironmentID: environment,
			Endpoint: Endpoint{
				Kind: EndpointServiceProxy, HostID: host, EnvironmentID: environment,
				RouteID: source.ID, ServiceID: source.TargetServiceID, ReleaseID: ids.New(ids.KindDeployment),
				Address: netip.MustParseAddrPort("10.1.2.3:8080"), AcknowledgedRevision: 4,
			},
		}},
	}
}
